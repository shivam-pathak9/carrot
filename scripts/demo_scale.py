#!/usr/bin/env python3
"""Scale demo: 10,000 concurrent clients, crash under load, memory flood.

Everything runs against real Carrot servers on loopback using only the Python
standard library. Run it with `make demo-scale` (Linux/WSL, needs Go).
"""
import asyncio
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLIENTS = int(os.environ.get("DEMO_CLIENTS", "10000"))
WRITERS = int(os.environ.get("DEMO_WRITERS", "200"))
FLOOD_KEYS = int(os.environ.get("DEMO_FLOOD_KEYS", "1000000"))
PORT = int(os.environ.get("DEMO_PORT", "16410"))

Y, G, D, B, R = "\033[1;33m", "\033[1;32m", "\033[2m", "\033[1m", "\033[0m"


def say(text=""):
    print(text, flush=True)


def act(title):
    say(f"\n{Y}# {title}{R}")


def note(text):
    say(f"{D}  {text}{R}")


def result(text):
    say(f"{G}  ✔{R} {text}")


class Server:
    def __init__(self, binary, *args):
        self.cmd = [binary, "-host", "127.0.0.1", "-port", str(PORT), *args]
        self.proc = None

    async def start(self):
        self.proc = subprocess.Popen(self.cmd, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                _, w = await asyncio.open_connection("127.0.0.1", PORT)
                w.close()
                return
            except OSError:
                await asyncio.sleep(0.1)
        raise RuntimeError("server did not start")

    def kill9(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait()
        self.proc = None

    def stop(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            self.proc = None


def encode(*parts):
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        p = p if isinstance(p, bytes) else str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)


async def reply(r):
    line = await r.readline()
    if not line:
        raise ConnectionError("closed")
    kind, rest = line[:1], line[1:-2]
    if kind == b"$":
        n = int(rest)
        if n < 0:
            return None
        data = await r.readexactly(n + 2)
        return data[:-2]
    if kind == b":":
        return int(rest)
    if kind == b"-":
        return Exception(rest.decode())
    return rest


async def connect():
    return await asyncio.open_connection("127.0.0.1", PORT)


# --- 1. many concurrent clients ------------------------------------------

async def one_client(i, opened, go):
    r, w = await connect()
    opened.append(1)
    await go.wait()
    ok = 0
    for round_ in range(3):
        value = f"v{i}-{round_}"
        w.write(encode("SET", f"user:{i}", value))
        await reply(r)
        w.write(encode("GET", f"user:{i}"))
        got = await reply(r)
        ok += got == value.encode()
    w.close()
    return ok == 3


async def many_clients(label, binary):
    server = Server(binary, "-aof-enabled=false", "-max-connections", str(CLIENTS + 100),
                    "-read-timeout", "120s", "-write-timeout", "120s")
    await server.start()
    try:
        opened, go = [], asyncio.Event()
        start = time.perf_counter()
        tasks = [asyncio.create_task(one_client(i, opened, go)) for i in range(CLIENTS)]
        while len(opened) < CLIENTS:
            if any(t.done() and t.exception() for t in tasks):
                break
            await asyncio.sleep(0.05)
        held = time.perf_counter() - start
        go.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        total = time.perf_counter() - start
        good = sum(o is True for o in outcomes)
        ops = CLIENTS * 3 * 2
        result(f"{label}: {len(opened):,} connections open at once ({held:.1f}s), "
               f"{good:,}/{CLIENTS:,} clients got correct data")
        note(f"{ops:,} commands in {total - held:.1f}s ≈ {ops / (total - held):,.0f}/s "
             "(bounded by the single Python client, not the server)")
    finally:
        server.stop()


# --- 2. kill -9 under load ------------------------------------------------

async def writer(w_id, acked, stop):
    try:
        r, w = await connect()
        n = 0
        while not stop.is_set():
            key = f"w{w_id}:{n}"
            w.write(encode("SET", key, key))
            res = await reply(r)
            if res == b"OK":
                acked.append(key)
            n += 1
    except (ConnectionError, OSError, asyncio.IncompleteReadError):
        pass


async def crash_under_load(binary, workdir):
    aof = os.path.join(workdir, "load.aof")
    args = ["-aof-file", aof, "-aof-sync", "always"]
    server = Server(binary, *args)
    await server.start()
    acked, stop = [], asyncio.Event()
    tasks = [asyncio.create_task(writer(i, acked, stop)) for i in range(WRITERS)]
    note(f"{WRITERS} clients writing with appendfsync=always...")
    await asyncio.sleep(3)
    before = len(acked)
    note(f"{before:,} writes acknowledged so far — sending SIGKILL now")
    server.kill9()
    await asyncio.gather(*tasks)
    total = len(acked)
    note(f"{total:,} writes were acknowledged before the process died")
    start = time.perf_counter()
    await server.start()
    note(f"restarted and replayed the AOF in {time.perf_counter() - start:.2f}s")
    r, w = await connect()
    lost = 0
    for i in range(0, total, 500):
        chunk = acked[i:i + 500]
        w.write(b"".join(encode("GET", k) for k in chunk))
        for k in chunk:
            lost += (await reply(r)) != k.encode()
    w.close()
    server.stop()
    if lost:
        say(f"  ✘ {lost:,} acknowledged writes were LOST")
        sys.exit(1)
    result(f"{total:,} acknowledged, {total - lost:,} recovered, {lost} lost")


# --- 4. memory flood ------------------------------------------------------

async def flood(binary, policy):
    server = Server(binary, "-aof-enabled=false", "-maxmemory", "50mb",
                    "-maxmemory-policy", policy)
    await server.start()
    r, w = await connect()
    value = b"x" * 100
    hot = [f"hot:{i}" for i in range(100)]
    for k in hot:
        w.write(encode("SET", k, value))
        await reply(r)
    accepted = refused = 0
    start = time.perf_counter()
    batch = 1000
    for base in range(0, FLOOD_KEYS, batch):
        w.write(b"".join(encode("SET", f"key:{i}", value) for i in range(base, base + batch)))
        w.write(b"".join(encode("GET", k) for k in hot))
        for _ in range(batch):
            res = await reply(r)
            if isinstance(res, Exception):
                refused += 1
            else:
                accepted += 1
        for _ in hot:
            await reply(r)
    elapsed = time.perf_counter() - start

    async def present(keys):
        n = 0
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            w.write(b"".join(encode("GET", k) for k in chunk))
            for _ in chunk:
                n += (await reply(r)) is not None
        return n

    old = await present([f"key:{i}" for i in range(10000)])
    new = await present([f"key:{i}" for i in range(FLOOD_KEYS - 10000, FLOOD_KEYS)])
    kept_hot = await present(hot)
    w.close()
    server.stop()
    return accepted, refused, elapsed, old, new, kept_hot


async def memory_flood(binary):
    note(f"writing {FLOOD_KEYS:,} keys (~100 B each) into a 50 MB limit, "
         "re-reading 100 hot keys every 1,000 writes")
    acc, ref, t, old, new, hot = await flood(binary, "allkeys-lru")
    result(f"allkeys-lru: {acc:,} writes accepted, {ref} errors, {acc / t:,.0f} writes/s")
    note(f"oldest 10,000 keys still present: {old:,}   newest 10,000: {new:,}   "
         f"hot keys: {hot}/100")
    acc, ref, t, old, new, hot = await flood(binary, "noeviction")
    result(f"noeviction: first {acc:,} writes accepted, then {ref:,} refused with -OOM")
    note(f"nothing evicted: oldest 10,000 keys present: {old:,}")


async def main():
    work = tempfile.mkdtemp(prefix="carrot-scale-")
    try:
        for name in ("server", "reactor-server"):
            subprocess.run(["go", "build", "-o", os.path.join(work, name),
                            f"./cmd/{name}"], cwd=ROOT, check=True)
        std, reactor = (os.path.join(work, n) for n in ("server", "reactor-server"))

        say(f"{B}🥕 Carrot at scale{R}")
        act(f"1. {CLIENTS:,} concurrent clients")
        await many_clients("goroutine server", std)
        await many_clients("epoll reactor   ", reactor)

        act("2. kill -9 under load, then recover from the AOF")
        await crash_under_load(reactor, work)

        act("3. Memory flood with a 50 MB limit")
        await memory_flood(std)
        say(f"\n{B}Done.{R}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
