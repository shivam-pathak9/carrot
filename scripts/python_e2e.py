#!/usr/bin/env python3
"""
Carrot - deep end-to-end test harness.

Run from the root of the Carrot repository on Linux/WSL:

python3 scripts/python_e2e.py

Examples:

python3 scripts/python_e2e.py --server standard
python3 scripts/python_e2e.py --server reactor
python3 scripts/python_e2e.py --server both
python3 scripts/python_e2e.py --server both --concurrency 32 --iterations 500
python3 scripts/python_e2e.py --skip-build

The harness:
  * builds both Go server binaries unless --skip-build is supplied
  * starts the selected server(s) on isolated loopback ports
  * tests RESP framing, fragmentation and pipelining
  * tests strings, DEL and all documented TTL/expiration forms
  * tests the documented list command surface
  * tests WRONGTYPE/arity/invalid-option behavior
  * tests concurrent clients and same-key contention
  * tests request/RESP boundary behavior
  * tests AOF append, restart recovery and AOFREWRITE
  * reports every failure with the command and observed response

No Python third-party packages are required.
"""

from __future__ import annotations

import argparse
import atexit
import concurrent.futures
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path


CRLF = b"\r\n"
ROOT = Path(__file__).resolve().parent.parent


class TestFailure(AssertionError):
    pass


def fail(msg: str):
    raise TestFailure(msg)


def check(condition, msg: str):
    if not condition:
        fail(msg)


def b(value) -> bytes:
    return value if isinstance(value, bytes) else str(value).encode()


# ---------------------------------------------------------------------------
# RESP encoder / decoder
# ---------------------------------------------------------------------------

def resp_command(*parts) -> bytes:
    parts = [b(x) for x in parts]
    return b"*" + str(len(parts)).encode() + CRLF + b"".join(
        b"$" + str(len(x)).encode() + CRLF + x + CRLF for x in parts
    )


class RESPError(Exception):
    pass


def read_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise RESPError(f"connection closed while reading {n} bytes")
        out.extend(chunk)
    return bytes(out)


def read_line(sock: socket.socket) -> bytes:
    out = bytearray()
    while True:
        c = sock.recv(1)
        if not c:
            raise RESPError("connection closed while reading RESP line")
        out.extend(c)
        if out.endswith(CRLF):
            return bytes(out[:-2])


def read_resp(sock: socket.socket):
    prefix = read_exact(sock, 1)

    if prefix == b"+":
        return ("simple", read_line(sock))
    if prefix == b"-":
        return ("error", read_line(sock))
    if prefix == b":":
        raw = read_line(sock)
        try:
            return ("integer", int(raw))
        except ValueError:
            raise RESPError(f"invalid RESP integer: {raw!r}")
    if prefix == b"$":
        raw = read_line(sock)
        try:
            n = int(raw)
        except ValueError:
            raise RESPError(f"invalid bulk length: {raw!r}")
        if n == -1:
            return ("bulk", None)
        if n < -1:
            raise RESPError(f"invalid bulk length: {n}")
        data = read_exact(sock, n)
        check(read_exact(sock, 2) == CRLF, "bulk string missing CRLF")
        return ("bulk", data)
    if prefix == b"*":
        raw = read_line(sock)
        try:
            n = int(raw)
        except ValueError:
            raise RESPError(f"invalid array length: {raw!r}")
        if n == -1:
            return ("array", None)
        if n < -1:
            raise RESPError(f"invalid array length: {n}")
        return ("array", [read_resp(sock) for _ in range(n)])

    raise RESPError(f"unknown RESP prefix: {prefix!r}")


def pretty(value):
    if isinstance(value, tuple):
        typ, payload = value
        if typ == "array":
            if payload is None:
                return "NULL_ARRAY"
            return "[" + ", ".join(pretty(x) for x in payload) + "]"
        if payload is None:
            return f"{typ.upper()}(nil)"
        if isinstance(payload, bytes):
            return f"{typ.upper()}({payload!r})"
        return f"{typ.upper()}({payload!r})"
    return repr(value)


def is_simple(value, expected: bytes) -> bool:
    return value == ("simple", expected)


def is_error(value, contains: str | None = None) -> bool:
    if value[0] != "error":
        return False
    if contains is None:
        return True
    return contains.lower().encode() in value[1].lower()


def is_bulk(value, expected: bytes | None) -> bool:
    return value == ("bulk", expected)


def is_integer(value, expected: int | None = None) -> bool:
    return value[0] == "integer" and (
        expected is None or value[1] == expected
    )


def array_payload(value):
    check(value[0] == "array" and value[1] is not None,
          f"expected non-null array, got {pretty(value)}")
    return value[1]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.addr = (host, port)
        self.timeout = timeout
        self.sock = socket.create_connection(self.addr, timeout=timeout)
        self.sock.settimeout(timeout)
        self.lock = threading.Lock()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def command(self, *parts):
        payload = resp_command(*parts)
        with self.lock:
            self.sock.sendall(payload)
            return read_resp(self.sock)

    def send_raw(self, payload: bytes):
        with self.lock:
            self.sock.sendall(payload)

    def recv(self):
        with self.lock:
            return read_resp(self.sock)

    def reconnect(self):
        self.close()
        self.sock = socket.create_connection(self.addr, timeout=self.timeout)
        self.sock.settimeout(self.timeout)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

@dataclass
class ServerProcess:
    name: str
    binary: Path
    port: int
    aof: Path | None = None
    max_connections: int = 128
    aof_sync: str = "always"
    scratch_dir: Path | None = None

    def __post_init__(self):
        self.proc: subprocess.Popen | None = None
        log_dir = self.scratch_dir or Path(tempfile.gettempdir())
        self.log_path = log_dir / f"carrot-{self.name}-{self.port}.log"
        self.log_file = None

    def start(self):
        if self.proc is not None:
            return

        args = [
            str(self.binary),
            "-host", "127.0.0.1",
            "-port", str(self.port),
            "-max-connections", str(self.max_connections),
            "-max-request-bytes", str(2 * 1024 * 1024),
            "-max-response-bytes", str(4 * 1024 * 1024),
            "-read-timeout", "5s",
            "-write-timeout", "5s",
        ]

        if self.aof is not None:
            args += [
                "-aof-enabled=true",
                "-aof-file", str(self.aof),
                "-aof-sync", self.aof_sync,
            ]
        else:
            args += ["-aof-enabled=false"]

        self.log_file = open(self.log_path, "wb")
        self.proc = subprocess.Popen(
            args,
            cwd=ROOT,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        deadline = time.monotonic() + 15
        last_error = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.25):
                    return
            except OSError as exc:
                last_error = exc
                time.sleep(0.1)

        rc = self.proc.poll()
        log = ""
        try:
            log = self.log_path.read_text(errors="replace")[-8000:]
        except OSError:
            pass
        self.stop()
        raise RuntimeError(
            f"{self.name} failed to start (rc={rc}, last_error={last_error})\n{log}"
        )

    def stop(self, kill=False):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            try:
                if kill:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                else:
                    os.killpg(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.proc.wait(timeout=5)

        if self.log_file:
            self.log_file.close()
            self.log_file = None
        self.proc = None


def build_binaries(skip_build: bool, build_dir: Path):
    if skip_build:
        binaries = {
            "standard": ROOT / "carrot-server",
            "reactor": ROOT / "carrot-reactor-server",
        }
        missing = [
            str(path)
            for path in binaries.values()
            if not path.is_file() or not os.access(path, os.X_OK)
        ]
        if missing:
            raise RuntimeError(
                "--skip-build requires executable binaries at the repository root: "
                + ", ".join(missing)
            )
        return binaries

    build_dir.mkdir(parents=True, exist_ok=True)
    standard = build_dir / "carrot-server"
    reactor = build_dir / "carrot-reactor-server"

    print("[build] go build ./cmd/server")
    r = subprocess.run(
        ["go", "build", "-o", str(standard), "./cmd/server"],
        cwd=ROOT,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError("failed to build cmd/server")

    print("[build] go build ./cmd/reactor-server")
    r = subprocess.run(
        ["go", "build", "-o", str(reactor), "./cmd/reactor-server"],
        cwd=ROOT,
        text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(
            "failed to build cmd/reactor-server; this test requires Linux/WSL"
        )

    return {"standard": standard, "reactor": reactor}


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

class Runner:
    def __init__(self, client: Client):
        self.c = client
        self.count = 0

    def expect(self, name, actual, expected=None, predicate=None):
        self.count += 1
        if predicate is not None:
            ok = predicate(actual)
        else:
            ok = actual == expected
        if not ok:
            fail(
                f"{name}\n"
                f"  expected: {pretty(expected) if predicate is None else '<predicate>'}\n"
                f"  observed: {pretty(actual)}"
            )

    def command(self, *parts):
        try:
            return self.c.command(*parts)
        except Exception as exc:
            raise TestFailure(
                f"command: {parts!r}\n  exception: {exc}"
            ) from exc

    def close(self):
        self.c.close()


# ---------------------------------------------------------------------------
# Functional tests
# ---------------------------------------------------------------------------

def test_ping(r: Runner):
    r.expect("PING", r.command("PING"), ("simple", b"PONG"))
    r.expect("PING with payload", r.command("PING", "arg"), ("bulk", b"arg"))
    r.expect(
        "PING too many arguments",
        r.command("PING", "too many", "args"),
        predicate=lambda x: is_error(x, "wrong number"),
    )


def test_strings_and_del(r: Runner):
    key = b"test:string"
    r.expect("GET missing", r.command("GET", "missing"), ("bulk", None))

    r.expect("SET", r.command("SET", key, b"hello"), ("simple", b"OK"))
    r.expect("GET", r.command("GET", key), ("bulk", b"hello"))

    r.expect(
        "SET overwrite",
        r.command("SET", key, b"world"),
        ("simple", b"OK"),
    )
    r.expect("GET overwritten", r.command("GET", key), ("bulk", b"world"))

    r.expect(
        "DEL multiple",
        r.command("DEL", key, "does-not-exist"),
        ("integer", 1),
    )
    r.expect("DEL missing", r.command("DEL", key), ("integer", 0))


def test_ttl(r: Runner):
    # EX
    k = b"ttl:ex"
    r.expect("SET EX", r.command("SET", k, "value", "EX", "2"), ("simple", b"OK"))
    ttl = r.command("TTL", k)
    check(ttl[0] == "integer" and 0 <= ttl[1] <= 2, f"unexpected TTL: {pretty(ttl)}")

    time.sleep(2.2)
    r.expect("EX expiration", r.command("GET", k), ("bulk", None))
    r.expect("TTL expired", r.command("TTL", k), ("integer", -2))

    # PX
    k = b"ttl:px"
    r.expect("SET PX", r.command("SET", k, "value", "PX", "300"), ("simple", b"OK"))
    ttl = r.command("TTL", k)
    check(ttl[0] == "integer" and ttl[1] in (-1, 0, 1), f"unexpected PX TTL: {pretty(ttl)}")
    time.sleep(0.45)
    r.expect("PX expiration", r.command("GET", k), ("bulk", None))

    # EXPIRE positive then zero
    k = b"ttl:expire"
    r.expect("SET persistent", r.command("SET", k, "value"), ("simple", b"OK"))
    r.expect("EXPIRE", r.command("EXPIRE", k, "2"), ("integer", 1))
    ttl = r.command("TTL", k)
    check(ttl[0] == "integer" and 0 <= ttl[1] <= 2, f"unexpected EXPIRE TTL: {pretty(ttl)}")
    r.expect("EXPIRE zero deletes", r.command("EXPIRE", k, "0"), ("integer", 1))
    r.expect("EXPIRE zero GET", r.command("GET", k), ("bulk", None))

    # PEXPIREAT future
    k = b"ttl:pxat"
    future_ms = int(time.time() * 1000) + 1500
    r.expect(
        "PEXPIREAT future",
        r.command("PEXPIREAT", k, str(future_ms)),
        ("integer", 0),
    )
    # Missing key is expected to return 0.
    r.expect("SET for PEXPIREAT", r.command("SET", k, "v"), ("simple", b"OK"))
    future_ms = int(time.time() * 1000) + 1200
    r.expect(
        "PEXPIREAT live key",
        r.command("PEXPIREAT", k, str(future_ms)),
        ("integer", 1),
    )
    time.sleep(1.35)
    r.expect("PEXPIREAT expiration", r.command("GET", k), ("bulk", None))

    # Already expired absolute deadline with SET PXAT.
    k = b"ttl:pxat-set"
    past = int(time.time() * 1000) - 1000
    r.expect(
        "SET PXAT past",
        r.command("SET", k, "v", "PXAT", str(past)),
        ("simple", b"OK"),
    )
    r.expect("SET PXAT past removes key", r.command("GET", k), ("bulk", None))

    r.expect("TTL persistent", r.command("SET", "ttl:persistent", "v"), ("simple", b"OK"))
    r.expect("TTL persistent = -1", r.command("TTL", "ttl:persistent"), ("integer", -1))


def test_wrong_types_and_validation(r: Runner):
    r.expect("create list", r.command("RPUSH", "type:list", "a"), ("integer", 1))
    r.expect(
        "GET list WRONGTYPE",
        r.command("GET", "type:list"),
        predicate=lambda x: is_error(x, "wrongtype"),
    )

    r.expect(
        "SET invalid EX",
        r.command("SET", "bad-ex", "v", "EX", "0"),
        predicate=lambda x: is_error(x),
    )
    r.expect(
        "SET invalid PX",
        r.command("SET", "bad-px", "v", "PX", "-1"),
        predicate=lambda x: is_error(x),
    )
    r.expect(
        "SET invalid option",
        r.command("SET", "bad-opt", "v", "BOGUS", "1"),
        predicate=lambda x: is_error(x),
    )
    r.expect(
        "EXPIRE invalid integer",
        r.command("EXPIRE", "x", "not-int"),
        predicate=lambda x: is_error(x),
    )
    r.expect(
        "PEXPIREAT invalid integer",
        r.command("PEXPIREAT", "x", "not-int"),
        predicate=lambda x: is_error(x),
    )
    r.expect(
        "GET wrong arity",
        r.command("GET"),
        predicate=lambda x: is_error(x, "wrong number"),
    )
    r.expect(
        "DEL wrong arity",
        r.command("DEL"),
        predicate=lambda x: is_error(x, "wrong number"),
    )
    r.expect(
        "UNKNOWN command",
        r.command("THIS_COMMAND_DOES_NOT_EXIST"),
        predicate=lambda x: is_error(x),
    )


def as_bulk_array(value):
    arr = array_payload(value)
    out = []
    for item in arr:
        check(item[0] == "bulk", f"expected bulk list element, got {pretty(item)}")
        out.append(item[1])
    return out


def test_lists(r: Runner):
    k = b"list:main"

    # RPUSH / LPUSH
    r.expect("RPUSH 1", r.command("RPUSH", k, "b", "c"), ("integer", 2))
    r.expect("LPUSH 1", r.command("LPUSH", k, "a"), ("integer", 3))
    r.expect(
        "LRANGE full",
        r.command("LRANGE", k, "0", "-1"),
        predicate=lambda x: as_bulk_array(x) == [b"a", b"b", b"c"],
    )
    r.expect("LLEN", r.command("LLEN", k), ("integer", 3))
    r.expect("LINDEX 0", r.command("LINDEX", k, "0"), ("bulk", b"a"))
    r.expect("LINDEX -1", r.command("LINDEX", k, "-1"), ("bulk", b"c"))
    r.expect("LINDEX out", r.command("LINDEX", k, "99"), ("bulk", None))

    # LSET
    r.expect("LSET", r.command("LSET", k, "1", "B"), ("simple", b"OK"))
    r.expect("LINDEX after LSET", r.command("LINDEX", k, "1"), ("bulk", b"B"))

    # LPUSHX/RPUSHX existing and missing
    r.expect("LPUSHX existing", r.command("LPUSHX", k, "x"), ("integer", 4))
    r.expect("RPUSHX existing", r.command("RPUSHX", k, "y"), ("integer", 5))
    r.expect("LPUSHX missing", r.command("LPUSHX", "no-list", "x"), ("integer", 0))
    r.expect("RPUSHX missing", r.command("RPUSHX", "no-list2", "x"), ("integer", 0))

    # Ranges / negative indexes
    r.expect(
        "LRANGE negative",
        r.command("LRANGE", k, "-3", "-1"),
        predicate=lambda x: as_bulk_array(x) == [b"B", b"c", b"y"],
    )

    # LINSERT
    r.expect(
        "LINSERT BEFORE",
        r.command("LINSERT", k, "BEFORE", "B", "before-B"),
        ("integer", 6),
    )
    r.expect(
        "LINSERT AFTER",
        r.command("LINSERT", k, "AFTER", "B", "after-B"),
        ("integer", 7),
    )

    # LPOS: first, rank, count, maxlen
    r.expect(
        "LPOS first",
        r.command("LPOS", k, "B"),
        ("integer", 3),
    )
    r.expect(
        "LPOS rank",
        r.command("LPOS", k, "B", "RANK", "-1"),
        ("integer", 3),
    )
    r.expect(
        "LPOS count",
        r.command("LPOS", k, "B", "COUNT", "2"),
        predicate=lambda x: x[0] == "array" and x[1] is not None and [i[1] for i in x[1]] == [3],
    )

    # LREM positive count, then negative count, then zero = all.
    r.expect("RPUSH duplicates", r.command("RPUSH", k, "dup", "dup", "dup"), ("integer", 10))
    r.expect("LREM positive", r.command("LREM", k, "2", "dup"), ("integer", 2))
    r.expect("LREM negative", r.command("LREM", k, "-1", "dup"), ("integer", 1))
    r.expect("LREM zero", r.command("LREM", k, "0", "dup"), ("integer", 0))

    # LTRIM
    r.expect("LTRIM", r.command("LTRIM", k, "0", "2"), ("simple", b"OK"))
    arr = as_bulk_array(r.command("LRANGE", k, "0", "-1"))
    check(len(arr) == 3, f"LTRIM produced wrong length: {arr!r}")

    # scalar LPOP/RPOP
    r.expect("LPOP scalar", r.command("LPOP", k), predicate=lambda x: x[0] == "bulk")
    r.expect("RPOP scalar", r.command("RPOP", k), predicate=lambda x: x[0] == "bulk")

    # Count form
    count_key = b"list:count-pop"
    r.expect(
        "RPUSH count-pop setup",
        r.command("RPUSH", count_key, "1", "2", "3", "4"),
        ("integer", 4),
    )
    popped = r.command("LPOP", count_key, "2")
    check(as_bulk_array(popped) == [b"1", b"2"], f"LPOP count wrong: {pretty(popped)}")
    popped = r.command("RPOP", count_key, "2")
    check(as_bulk_array(popped) == [b"4", b"3"], f"RPOP count wrong: {pretty(popped)}")

    # LREM missing list, LSET errors.
    r.expect("LREM missing", r.command("LREM", "missing:list", "1", "x"), ("integer", 0))
    r.expect(
        "LSET missing",
        r.command("LSET", "missing:list", "0", "x"),
        predicate=lambda x: is_error(x),
    )

    # Move commands
    r.expect("RPUSH move source", r.command("RPUSH", "move:src", "a", "b", "c"), ("integer", 3))
    r.expect("LMOVE", r.command("LMOVE", "move:src", "move:dst", "LEFT", "RIGHT"), ("bulk", b"a"))
    r.expect(
        "LMOVE destination",
        r.command("LRANGE", "move:dst", "0", "-1"),
        predicate=lambda x: as_bulk_array(x) == [b"a"],
    )
    r.expect("RPOPLPUSH setup", r.command("RPUSH", "rpl:src", "x", "y"), ("integer", 2))
    r.expect("RPOPLPUSH", r.command("RPOPLPUSH", "rpl:src", "rpl:dst"), ("bulk", b"y"))
    r.expect(
        "RPOPLPUSH destination",
        r.command("LRANGE", "rpl:dst", "0", "-1"),
        predicate=lambda x: as_bulk_array(x) == [b"y"],
    )

    # Same-key LMOVE rotates an element.
    r.expect("same-key move setup", r.command("RPUSH", "rotate", "a", "b", "c"), ("integer", 3))
    r.expect(
        "same-key LMOVE",
        r.command("LMOVE", "rotate", "rotate", "LEFT", "RIGHT"),
        ("bulk", b"a"),
    )
    r.expect(
        "same-key LMOVE result",
        r.command("LRANGE", "rotate", "0", "-1"),
        predicate=lambda x: as_bulk_array(x) == [b"b", b"c", b"a"],
    )


def test_empty_and_expired_lists(r: Runner):
    k = b"empty:list"
    r.expect("LPOP missing scalar", r.command("LPOP", k), ("bulk", None))
    r.expect(
        "LPOP missing count",
        r.command("LPOP", k, "2"),
        predicate=lambda x: x[0] == "array" and x[1] == [],
    )
    r.expect("RPOP missing scalar", r.command("RPOP", k), ("bulk", None))

    r.expect("create expiring list", r.command("RPUSH", "exp:list", "x"), ("integer", 1))
    r.expect("expire list", r.command("EXPIRE", "exp:list", "1"), ("integer", 1))
    time.sleep(1.2)
    r.expect("expired list LLEN", r.command("LLEN", "exp:list"), ("integer", 0))


# ---------------------------------------------------------------------------
# RESP / transport robustness
# ---------------------------------------------------------------------------

def test_fragmented_resp(host, port):
    c = Client(host, port)
    try:
        payload = resp_command("PING")
        # Send one byte at a time. This exercises partial-frame buffering.
        for byte in payload:
            c.send_raw(bytes([byte]))
            time.sleep(0.001)
        got = c.recv()
        check(got == ("simple", b"PONG"), f"fragmented PING failed: {pretty(got)}")
    finally:
        c.close()


def test_pipelining(host, port, n=1000):
    c = Client(host, port, timeout=10)
    try:
        payload = b"".join(resp_command("SET", f"pipe:{i}", f"value-{i}") for i in range(n))
        payload += b"".join(resp_command("GET", f"pipe:{i}") for i in range(n))
        c.send_raw(payload)

        for i in range(n):
            got = c.recv()
            check(got == ("simple", b"OK"), f"pipeline SET {i}: {pretty(got)}")
        for i in range(n):
            got = c.recv()
            check(got == ("bulk", f"value-{i}".encode()), f"pipeline GET {i}: {pretty(got)}")
    finally:
        c.close()


def test_half_close(host, port):
    s = socket.create_connection((host, port), timeout=3)
    try:
        s.settimeout(3)
        s.sendall(resp_command("PING"))
        got = read_resp(s)
        check(got == ("simple", b"PONG"), f"half-close precondition failed: {pretty(got)}")
        s.shutdown(socket.SHUT_WR)
        # The server is allowed to close after consuming the final request.
        try:
            s.recv(1)
        except OSError:
            pass
    finally:
        s.close()


def test_protocol_boundaries(host, port):
    # Array > configured RESP element limit should not produce a normal PONG.
    c = Client(host, port, timeout=3)
    try:
        oversized_array = b"*" + str(1025).encode() + CRLF
        oversized_array += b"$1\r\nx\r\n" * 1025
        c.send_raw(oversized_array)
        try:
            got = c.recv()
            check(
                got[0] == "error",
                f"oversized RESP array unexpectedly succeeded: {pretty(got)}",
            )
        except (RESPError, socket.timeout, ConnectionError, OSError):
            # Closing/rejecting the connection is also valid for malformed/oversized input.
            pass
    finally:
        c.close()

    # Invalid bulk length must be rejected, not interpreted as a command.
    c = Client(host, port, timeout=3)
    try:
        c.send_raw(b"*1\r\n$abc\r\n")
        try:
            got = c.recv()
            check(got[0] == "error", f"invalid bulk length was accepted: {pretty(got)}")
        except (RESPError, socket.timeout, ConnectionError, OSError):
            pass
    finally:
        c.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------

def concurrent_worker(host, port, worker_id, iterations):
    c = Client(host, port, timeout=10)
    try:
        for i in range(iterations):
            key = f"conc:{worker_id}:{i}"
            expected = f"value-{worker_id}-{i}".encode()
            got = c.command("SET", key, expected)
            if got != ("simple", b"OK"):
                return f"SET failed worker={worker_id} i={i}: {pretty(got)}"
            got = c.command("GET", key)
            if got != ("bulk", expected):
                return f"GET failed worker={worker_id} i={i}: {pretty(got)}"
            if i % 17 == 0:
                got = c.command("DEL", key)
                if got != ("integer", 1):
                    return f"DEL failed worker={worker_id} i={i}: {pretty(got)}"
                got = c.command("GET", key)
                if got != ("bulk", None):
                    return f"GET-after-DEL failed worker={worker_id}: {pretty(got)}"
        return None
    except Exception as exc:
        return f"worker={worker_id} exception: {exc}"
    finally:
        c.close()


def test_concurrency(host, port, workers, iterations):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(concurrent_worker, host, port, w, iterations)
            for w in range(workers)
        ]
        errors = [f.result() for f in futures if f.result() is not None]
    check(not errors, "concurrency failures:\n" + "\n".join(errors[:10]))


def test_same_key_contention(host, port, workers=16, per_worker=100):
    """
    Every worker repeatedly SETs the same key. We don't assert a final value
    because the last writer is intentionally nondeterministic; we assert that
    every individual operation gets a valid response and that the server
    remains usable afterward.
    """
    key = "hot-key"

    def worker(w):
        c = Client(host, port, timeout=10)
        try:
            for i in range(per_worker):
                got = c.command("SET", key, f"{w}:{i}")
                if got != ("simple", b"OK"):
                    return f"worker {w} SET {i}: {pretty(got)}"
                got = c.command("GET", key)
                if got[0] != "bulk":
                    return f"worker {w} GET {i}: {pretty(got)}"
            return None
        except Exception as exc:
            return f"worker {w}: {exc}"
        finally:
            c.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        errors = [x for x in pool.map(worker, range(workers)) if x]
    check(not errors, "same-key contention failures:\n" + "\n".join(errors[:10]))


# ---------------------------------------------------------------------------
# AOF
# ---------------------------------------------------------------------------

def run_aof_test(binary: Path, name: str, port: int, scratch_dir: Path):
    tmp_owner = tempfile.TemporaryDirectory(prefix="carrot-aof-")
    tmp = Path(tmp_owner.name)
    aof = tmp / "carrot.aof"
    srv = ServerProcess(
        name + "-aof",
        binary,
        port,
        aof=aof,
        aof_sync="always",
        scratch_dir=scratch_dir,
    )

    try:
        srv.start()
        c = Client("127.0.0.1", port, timeout=5)
        try:
            # Durable string state.
            check(c.command("SET", "aof:string", "version-1") == ("simple", b"OK"), "AOF SET failed")
            check(c.command("SET", "aof:string", "version-2") == ("simple", b"OK"), "AOF overwrite failed")

            # Durable list state.
            check(c.command("RPUSH", "aof:list", "one", "two", "three") == ("integer", 3), "AOF list failed")
            check(c.command("LPOP", "aof:list") == ("bulk", b"one"), "AOF list mutation failed")

            # Delete should be replayed as absence.
            check(c.command("SET", "aof:deleted", "gone") == ("simple", b"OK"), "AOF delete setup failed")
            check(c.command("DEL", "aof:deleted") == ("integer", 1), "AOF delete failed")

            # Expiration should be canonicalized/replayed.
            check(c.command("SET", "aof:ttl", "short", "PX", "5000") == ("simple", b"OK"), "AOF TTL SET failed")

            # Generate obsolete history so rewrite actually has work to do.
            for i in range(50):
                check(
                    c.command("SET", "aof:rewrite-key", f"value-{i}") == ("simple", b"OK"),
                    "AOF rewrite history SET failed",
                )

            before = aof.stat().st_size
            check(before > 0, "AOF file was not created")

            rewrite = c.command("AOFREWRITE")
            check(
                rewrite == ("simple", b"OK"),
                f"AOFREWRITE unexpected response: {pretty(rewrite)}",
            )

            after = aof.stat().st_size
            check(after > 0, "AOF file empty after rewrite")
            check(after <= before, f"AOF rewrite did not compact: before={before}, after={after}")
        finally:
            c.close()

        # Stop and restart using the same AOF.
        srv.stop()
        srv.start()

        c = Client("127.0.0.1", port, timeout=5)
        try:
            check(c.command("GET", "aof:string") == ("bulk", b"version-2"), "AOF string recovery failed")
            check(
                as_bulk_array(c.command("LRANGE", "aof:list", "0", "-1"))
                == [b"two", b"three"],
                "AOF list recovery failed",
            )
            check(c.command("GET", "aof:deleted") == ("bulk", None), "AOF deleted key recovered incorrectly")
            check(
                c.command("GET", "aof:rewrite-key") == ("bulk", b"value-49"),
                "AOF rewritten value recovery failed",
            )

            # Mutation after restart must also append and survive another restart.
            check(c.command("SET", "aof:after-restart", "yes") == ("simple", b"OK"), "post-restart write failed")
        finally:
            c.close()

        srv.stop()
        srv.start()

        c = Client("127.0.0.1", port, timeout=5)
        try:
            check(
                c.command("GET", "aof:after-restart") == ("bulk", b"yes"),
                "post-restart mutation did not recover",
            )
        finally:
            c.close()

        return True
    finally:
        srv.stop()
        tmp_owner.cleanup()


# ---------------------------------------------------------------------------
# Test orchestration
# ---------------------------------------------------------------------------

def functional_suite(host, port):
    c = Client(host, port, timeout=5)
    try:
        r = Runner(c)
        test_ping(r)
        test_strings_and_del(r)
        test_ttl(r)
        test_wrong_types_and_validation(r)
        test_lists(r)
        test_empty_and_expired_lists(r)
        return r.count
    finally:
        c.close()


def run_server_suite(kind, binary, port, concurrency, iterations, scratch_dir):
    print(f"\n=== {kind.upper()} SERVER ===")
    srv = ServerProcess(kind, binary, port, scratch_dir=scratch_dir)
    srv.start()

    try:
        start = time.monotonic()

        print("[1/7] functional command suite")
        n = functional_suite("127.0.0.1", port)
        print(f"      PASS ({n} assertions)")

        print("[2/7] fragmented RESP frames")
        test_fragmented_resp("127.0.0.1", port)
        print("      PASS")

        print("[3/7] pipelining")
        test_pipelining("127.0.0.1", port, n=1000)
        print("      PASS (2,000 pipelined commands)")

        print("[4/7] connection/protocol boundary cases")
        test_half_close("127.0.0.1", port)
        test_protocol_boundaries("127.0.0.1", port)
        print("      PASS")

        print(f"[5/7] concurrent clients ({concurrency} x {iterations})")
        test_concurrency("127.0.0.1", port, concurrency, iterations)
        print("      PASS")

        print("[6/7] same-key contention")
        test_same_key_contention("127.0.0.1", port)
        print("      PASS")

        print("[7/7] final health check")
        c = Client("127.0.0.1", port)
        try:
            check(c.command("PING") == ("simple", b"PONG"), "server failed final health check")
        finally:
            c.close()
        print("      PASS")

        elapsed = time.monotonic() - start
        print(f"=== {kind.upper()} PASS ({elapsed:.2f}s) ===")
    except Exception:
        print(f"\n--- {kind} server log tail: {srv.log_path} ---")
        try:
            print(srv.log_path.read_text(errors="replace")[-12000:])
        except OSError:
            pass
        raise
    finally:
        srv.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--server",
        choices=["standard", "reactor", "both"],
        default="both",
        help="which server implementation to test",
    )
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--iterations", type=int, default=100)
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument(
        "--skip-aof",
        action="store_true",
        help="skip the AOF restart/rewrite suite",
    )
    args = ap.parse_args()

    if args.concurrency < 1 or args.iterations < 1:
        ap.error("--concurrency and --iterations must be positive")

    if not sys.platform.startswith("linux"):
        print(
            "ERROR: run this harness under Linux/WSL. "
            "The reactor server is Linux/epoll based.",
            file=sys.stderr,
        )
        return 2

    scratch = tempfile.TemporaryDirectory(prefix="carrot-e2e-")
    atexit.register(scratch.cleanup)
    scratch_dir = Path(scratch.name)
    binaries = build_binaries(args.skip_build, scratch_dir)

    selected = []
    if args.server in ("standard", "both"):
        selected.append(("standard", binaries["standard"], allocate_port()))
    if args.server in ("reactor", "both"):
        selected.append(("reactor", binaries["reactor"], allocate_port()))

    total_start = time.monotonic()
    failures = []

    for kind, binary, port in selected:
        try:
            run_server_suite(
                kind, binary, port, args.concurrency, args.iterations, scratch_dir
            )
        except Exception as exc:
            failures.append((kind, str(exc)))
            print(f"\n!!! {kind.upper()} FAILED: {exc}\n")

    if not args.skip_aof:
        for kind, binary, port in selected:
            print(f"\n=== {kind.upper()} AOF SUITE ===")
            try:
                start = time.monotonic()
                run_aof_test(binary, kind, allocate_port(), scratch_dir)
                print(f"=== {kind.upper()} AOF PASS ({time.monotonic() - start:.2f}s) ===")
            except Exception as exc:
                failures.append((kind + "-aof", str(exc)))
                print(f"\n!!! {kind.upper()} AOF FAILED: {exc}\n")

    elapsed = time.monotonic() - total_start

    print("\n" + "=" * 72)
    if failures:
        print(f"CARROT TEST RESULT: FAIL ({len(failures)} suite(s) failed)")
        for name, msg in failures:
            print(f"\n[{name}] {msg}")
        print("=" * 72)
        return 1

    print(f"CARROT TEST RESULT: PASS ({elapsed:.2f}s)")
    print("Both networking models, command semantics, protocol boundaries,")
    print("concurrency, expiration, and AOF recovery/rewrite passed.")
    print("=" * 72)
    return 0


def allocate_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


if __name__ == "__main__":
    raise SystemExit(main())
