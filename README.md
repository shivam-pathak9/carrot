<p align="center">
  <img src="assets/carrot_logo.png" alt="Carrot Retro Logo" width="280" />
</p>

<h1 align="center">🥕 Carrot</h1>

<p align="center">
  <b>Carrot is a Redis-inspired single-node server written in Go.</b><br/>
  It is an in-memory development project progressing toward production readiness; it is not currently safe for production data.
</p>

---

## What this project includes

Carrot currently includes:

- A RESP parser and encoder for Redis-style wire protocol messages
- An in-memory storage layer with string and list values plus TTL support
- Core commands: `PING`, `GET`, `SET`, `DEL`, `TTL`, and `EXPIRE`
- Redis-style list commands including pushes, pops, ranges, indexed updates, trimming, removal, insertion, positions, and atomic moves
- A per-connection goroutine server built with Go net listeners
- A Linux epoll-based reactor server for non-blocking I/O
- Scheduled background expiration cleanup for stale keys
- A test suite covering storage, config, command behavior, and RESP parsing

---

## Project structure

- `cmd/server` — goroutine-based TCP server
- `cmd/reactor-server` — epoll-based event loop server
- `internal/command` — command parsing and execution logic
- `internal/config` — configuration defaults
- `internal/protocol/resp` — RESP framing, encoder/decoder
- `internal/storage` — Map-based in-memory key store with TTL support
- `internal/reactor` — reactor implementation and architecture docs

---

## Supported behavior

The server currently supports this Redis-inspired command subset:

- `PING`
- `SET key value [EX seconds|PX milliseconds|PXAT unix-milliseconds]`
- `GET key`
- `DEL key [key ...]`
- `TTL key`
- `EXPIRE key seconds`
- `PEXPIREAT key unix-milliseconds`

List commands currently supported:

- `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`
- `LPOP`, `RPOP` (including count form)
- `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`
- `LREM`, `LINSERT`, `LPOS`
- `LMOVE`, `RPOPLPUSH`

Blocking list commands (such as `BLPOP` and `BRPOP`) are not implemented.

The storage layer also supports passive expiration on access and a bounded active expiration sweep for background cleanup.

---

## How to run

Start the goroutine server:

```bash
go run ./cmd/server
```

The default bind address is `0.0.0.0`, which exposes the unauthenticated,
unencrypted server on every network interface. Use loopback for local
development; expose it only on a trusted, firewalled network:

```bash
go run ./cmd/server -host 127.0.0.1 -port 16379
```

Start the reactor server:

```bash
go run ./cmd/reactor-server -host 127.0.0.1 -port 16380
```

Operational settings can be configured on either binary:

```bash
go run ./cmd/server \
  -host 127.0.0.1 -port 16379 \
  -max-connections 128 \
  -max-request-bytes 2097152 \
  -max-response-bytes 4194304 \
  -read-timeout 30s -write-timeout 10s
```

AOF persistence is currently supported on Linux and is enabled by default
(`appendonly.aof`, `everysec` sync). On other operating systems, builds remain
available but startup with AOF enabled returns an explicit unsupported-platform
error; disable AOF only for intentionally ephemeral runs.
The sync policy can be set to `always`, `everysec`, or `no`; see
[docs/AOF_PERSISTENCE.md](docs/AOF_PERSISTENCE.md) for the durability contract,
recovery behavior, and limitations. For a different path or stronger
write-by-write syncing:

```bash
go run ./cmd/server -aof-file ./data/carrot.aof -aof-sync always
```

On a running server, compact the AOF from the current live data with:

```bash
redis-cli -h 127.0.0.1 -p 6379 AOFREWRITE
```

This initial rewrite is synchronous: writes pause until the temporary AOF is
synced and atomically installed; reads continue. The server appends waiting
writes to the replacement afterward.

### AOF rewrite smoke test

On 2026-10-03, `redis-cli` was used to verify `AOFREWRITE` on both the standard
server and the event-loop server on Linux. Each test used `always` sync,
repeatedly overwrote one key to create obsolete history, kept a string and
list, deleted another key, then rewrote the AOF:

| Server | AOF size before rewrite | After rewrite | Restart check |
|---|---:|---:|---|
| Standard | 4,360 bytes | 205 bytes | Passed |
| Event-loop reactor | 4,360 bytes | 205 bytes | Passed |

For both servers, the current string/list values and latest overwritten value
were present after rewrite; the deleted key remained absent. A write issued
after rewrite also survived a forced process stop and restart using the same
AOF. This is a manual smoke test, not a performance benchmark or a power-loss
test; it does not establish durability against OS or hardware failure. See
[docs/AOF_PERSISTENCE.md](docs/AOF_PERSISTENCE.md) for rewrite behavior and
remaining persistence limitations.

The parent directory must already exist. For an explicitly ephemeral run:

```bash
go run ./cmd/server -aof-enabled=false
```

Both binaries handle `SIGINT` and `SIGTERM` for shutdown. The standard server
stops accepting connections and waits for active clients, closing them if its
10-second shutdown window expires. The reactor closes its clients and epoll
resources during shutdown. The epoll implementation currently supports IPv4
addresses only.

Run the test suite:

```bash
go test ./...
go test -race ./...
go test -cover ./...
go build ./...
go vet ./...
```

Server and reactor TCP integration tests run as part of `go test ./...`.

---

## Current status

Carrot is an MVP and is **not production-ready**. AOF persistence, restart
recovery, and manual rewrite/compaction are implemented, but broader
crash/failure validation and backup procedures are still outstanding.
Durability depends on the configured sync policy. It also has no authentication
or transport encryption, and the default bind address is `0.0.0.0`. Use
loopback for local testing and do not expose it to untrusted networks.

The project currently demonstrates:

- A documented subset of RESP and Redis-style string/list commands
- In-memory typed storage and TTL behavior
- Two networking models: goroutine-per-connection and Linux epoll
- Unit-test coverage for storage, commands, RESP, and configuration

Important gaps before production use still include:

- Crash/fault-injection validation and backup procedures
- Authentication, transport security, and a safer default bind policy
- Total database memory quotas, metrics, health checks, and deployment guidance
- Hashes, sets, sorted sets, and blocking list commands

## Local benchmark

The following is a single local comparison run made on 2026-10-02 with
`redis-benchmark 7.0.15`. Both servers ran sequentially on the same WSL2 Linux
host (`12` logical CPUs, Linux kernel `6.18.33.2-microsoft-standard-WSL2`),
bound to loopback. Each benchmark case sent 10,000 requests using 10 concurrent
clients, a 16-byte payload, and pipeline depth 1:

```bash
redis-benchmark -h 127.0.0.1 -p PORT \
  -n 10000 -c 10 -d 16 -P 1 \
  -t ping_inline,ping_mbulk,set,get,lpush,rpush,lpop,rpop
```

| Command | Goroutine server req/s | p50 / p95 / p99 (ms) | Reactor server req/s | p50 / p95 / p99 (ms) |
|---|---:|---:|---:|---:|
| PING_INLINE | 28,985.51 | 0.191 / 0.759 / 1.527 | 61,349.69 | 0.119 / 0.295 / 0.631 |
| PING_MBULK | 37,313.43 | 0.167 / 0.511 / 1.055 | 64,102.56 | 0.127 / 0.271 / 0.487 |
| SET | 37,174.72 | 0.167 / 0.527 / 1.015 | 60,606.06 | 0.135 / 0.287 / 0.623 |
| GET | 35,087.72 | 0.175 / 0.543 / 1.111 | 58,139.53 | 0.143 / 0.303 / 0.623 |
| LPUSH | 36,630.04 | 0.167 / 0.519 / 1.079 | 54,945.05 | 0.151 / 0.327 / 0.567 |
| RPUSH | 34,843.21 | 0.175 / 0.575 / 1.135 | 53,475.93 | 0.143 / 0.359 / 0.551 |
| LPOP | 34,843.21 | 0.175 / 0.559 / 1.071 | 47,846.89 | 0.167 / 0.415 / 0.615 |
| RPOP | 33,112.59 | 0.191 / 0.567 / 0.975 | 50,251.26 | 0.167 / 0.383 / 0.551 |

These figures are a one-off loopback smoke benchmark, not a capacity estimate,
SLA, or production comparison. Results depend on the host, runtime, and load;
they were not repeated to calculate confidence intervals or run to saturation.
The native benchmark covers PING, SET/GET, and list push/pop only; it does not
measure every implemented list command or unsupported Redis commands. It also
does not model persistence, TLS, or network latency. `redis-benchmark` printed
`WARNING: Could not fetch server CONFIG` because Carrot does not implement
`CONFIG`; the selected benchmark cases still completed.

---

## Server Architecture Comparison: When to Use Which?

Carrot provides two distinct networking models. Below is a high-level architectural insight and selection guide to help you choose the right server for your deployment:

### 1. Goroutine-per-Client Server (`cmd/server`)

* **How it works:** Uses Go's standard `net.Listener`. Each incoming TCP connection spawns a dedicated goroutine (`go s.handleClient(conn)`). Blocking I/O reads and writes are managed transparently by the Go runtime netpoller.
* **Pros:**
  - **Cross-Platform Compatibility:** Runs on Linux, macOS, Windows, and BSD without OS-specific system call dependencies.
  - **Simplicity & Debuggability:** Straightforward code paths with clean stack traces and easy profiling using standard Go tools (`pprof`).
  - **Safety Under Long Commands:** Individual client latency spikes do not block the event loop of other clients.
* **Cons:**
  - **Higher Memory Overhead:** Each client connection allocates a Go goroutine stack (2KB–8KB) plus I/O buffers.
  - **Goroutine Context Switching:** High connection counts (10,000+ connections) incur Go scheduler context switching overhead.
* **When to use:** Local cross-platform development (macOS/Windows), debugging, or non-Linux deployment environments.

---

### 2. Linux Epoll Reactor Server (`cmd/reactor-server`)

* **How it works:** Implements an event-driven, single-threaded Reactor pattern using Linux `epoll_wait` system calls directly via `golang.org/x/sys/unix`. Sockets are configured as non-blocking (`SOCK_NONBLOCK`), and a single event loop thread handles connection accepting (`accept4`), buffer draining, command dispatching, and response flushing.
* **Pros:**
  - **Maximum Throughput & Low Latency:** Delivers **~60,000–64,000 QPS** (~1.7x higher throughput than the Goroutine server) with sub-150 microsecond median p50 latency.
  - **Minimal Memory Overhead:** Sockets are held as raw file descriptors registered in kernel memory without per-client goroutines.
  - **Zero CPU Waste on Idle Connections:** Scales efficiently to thousands of idle clients without goroutine wakeups.
* **Cons:**
  - **Linux Specific:** Requires Linux `epoll_create1`, `epoll_wait`, and `accept4` system calls (`//go:build linux`).
  - **Single-Threaded Head-of-Line Risk:** Long-running CPU-bound commands on the event loop thread delay execution for all other connected clients.
* **When to use:** Linux production deployments, high-throughput micro-benchmarking, ultra-low latency SLAs, and high connection density on Linux hosts.

---

### Summary Comparison Matrix

| Feature / Criteria | Goroutine Server (`cmd/server`) | Epoll Reactor Server (`cmd/reactor-server`) |
|---|---|---|
| **Networking Architecture** | Goroutine-per-client (`net.Conn`) | Event Loop (`epoll_wait` system calls) |
| **Supported OS** | Cross-platform (Linux, macOS, Windows) | Linux only (`//go:build linux`) |
| **Peak Throughput** | **~35,000 – 37,000 QPS** | **~60,000 – 64,000 QPS** (🚀 **1.7x Faster**) |
| **Median Latency (p50)** | **~0.16 – 0.19 ms** | **~0.11 – 0.14 ms** |
| **Client Memory Footprint** | ~2 KB – 8 KB stack per connection | Low (raw Socket FD in kernel) |
| **IP Protocol Support** | IPv4 & IPv6 | IPv4 & IPv6 (Dual-Stack `::`) |
| **Recommended Use Case** | Cross-platform dev, testing & debugging | Linux production & high-throughput SLAs |

---

## Design notes

Carrot was built to explore two concurrency models:

1. A simple per-client goroutine server for clarity and ease of debugging
2. A Linux epoll reactor for lower-overhead, event-driven networking

Carrot is not a drop-in Redis replacement. The project goal, out-of-scope features, readiness phases, and verified test baseline are described in [docs/PROJECT_SCOPE.md](docs/PROJECT_SCOPE.md), [docs/TEST_SUMMARY.md](docs/TEST_SUMMARY.md), and [docs/TEST_EXECUTION_GUIDE.md](docs/TEST_EXECUTION_GUIDE.md).

---

## License

This project is distributed under the MIT license. See [LICENSE](LICENSE).
