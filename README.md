<p align="center">
  <img src="assets/carrot_logo.png" alt="Carrot Retro Logo" width="280" />
</p>

<h1 align="center">🥕 Carrot</h1>

<p align="center">
  <b>Carrot is a Redis-inspired single-node server written in Go.</b><br/>
  It is an in-memory development project progressing toward production readiness; it is not currently safe for production data.
</p>

<p align="center">
  Architecture references: <a href="HLD.md">HLD</a> · <a href="LLD.md">LLD</a>
</p>

---

## What this project includes

Carrot currently includes:

- A RESP parser and encoder for Redis-style wire protocol messages
- An in-memory storage layer with string and list values plus TTL support
- String and expiration commands: `PING`, `GET`, `SET`, `DEL`, `TTL`, `EXPIRE`, and `PEXPIREAT`
- AOF append, streaming recovery, configurable sync policies, and manual rewrite/compaction on Linux
- Redis-style list commands including pushes, pops, ranges, indexed updates, trimming, removal, insertion, positions, and atomic moves
- A per-connection goroutine server built with Go net listeners
- A Linux epoll-based reactor server for non-blocking I/O
- Scheduled background expiration cleanup for stale keys
- Unit, TCP integration, and AOF recovery tests across the core packages

---

## Project structure

- `cmd/server` — goroutine-based TCP server
- `cmd/reactor-server` — epoll-based event loop server
- `cmd/scale-server` — active-expiration scale demonstration; not a network load server
- `internal/command` — command parsing and execution logic
- `internal/config` — configuration defaults
- `internal/protocol/resp` — RESP framing, encoder/decoder
- `internal/storage` — Map-based in-memory key store with TTL support
- `internal/aof` — Linux-only append-only persistence, replay, and compaction
- `internal/server` — goroutine-server lifecycle and client handling
- `internal/client` — client connection and protocol helpers
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
synced and atomically installed. Standard-server reads can proceed while the
rewrite runs; the reactor's event loop cannot serve requests until its rewrite
call returns. Waiting writes append to the replacement afterward.

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
resources during shutdown. Its listener accepts IPv4 and IPv6 address forms;
dual-stack behavior for an IPv6 wildcard depends on host configuration.

Run the project checks:

```bash
make check
# Or run the Go commands individually:
go test ./...
go test -race ./...
go test -cover ./...
go build ./...
go vet ./...
```

Server and reactor TCP integration tests run as part of `go test ./...`.
GitHub Actions runs formatting, tests, race tests, coverage, vet, and build
checks on Linux for pushes and pull requests.

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
- AOF append/recovery and manual rewrite/compaction
- Two networking models: goroutine-per-connection and Linux epoll
- Unit and TCP integration tests for storage, commands, persistence, protocol,
  configuration, and both server implementations

Important gaps before production use still include:

- Crash/fault-injection validation and backup procedures
- Authentication, transport security, and a safer default bind policy
- Total database memory quotas, metrics, health checks, and deployment guidance
- Hashes, sets, sorted sets, and blocking list commands

## Local benchmark

The following repeatable local comparison was run on 2026-10-03 with
`redis-benchmark 7.0.15`, Go 1.26.4, on WSL2 Linux/amd64 (`12` logical CPUs,
kernel `6.18.33.2-microsoft-standard-WSL2`). Both servers bound to loopback;
AOF was disabled to compare the network and in-memory command paths. Each of
three rounds sent 10,000 requests per command with 10 concurrent clients, a
16-byte payload, and pipeline depth 1. The table reports median throughput and
median p50 latency across those three runs:

| Command | Goroutine req/s | Goroutine p50 (ms) | Reactor req/s | Reactor p50 (ms) |
|---|---:|---:|---:|---:|
| PING_INLINE | 30,960 | 0.199 | 47,619 | 0.151 |
| PING_MBULK | 30,303 | 0.207 | 47,170 | 0.159 |
| SET | 27,174 | 0.231 | 42,553 | 0.191 |
| GET | 29,070 | 0.207 | 42,373 | 0.191 |
| LPUSH | 29,070 | 0.215 | 41,841 | 0.199 |
| RPUSH | 28,249 | 0.223 | 39,683 | 0.199 |
| LPOP | 29,240 | 0.215 | 42,918 | 0.191 |
| RPOP | 29,586 | 0.207 | 43,290 | 0.183 |

Reproduce the comparison with `make benchmark`; the script runs three rounds
and prints environment and parameter details. See
[docs/TEST_EXECUTION_GUIDE.md](docs/TEST_EXECUTION_GUIDE.md) for overrides.
Executor/storage microbenchmarks are available with
`go test -run '^$' -bench . ./internal/command`.
These results are a local microbenchmark, not a capacity estimate, SLA, or
production comparison. They do not measure AOF durability, TLS, network
latency, or every implemented/unsupported command. `redis-benchmark` reports
that it cannot fetch `CONFIG`, which Carrot does not implement; the selected
cases complete despite that warning.

---

## Server architecture comparison

Carrot provides two networking models. This is an implementation comparison,
not a production deployment recommendation.

### Goroutine-per-client server (`cmd/server`)

Uses Go's standard `net.Listener`, with a goroutine handling each client.
This is the simpler and easier-to-debug implementation. The networking code
uses Go's portable APIs, but AOF currently requires Linux and is enabled by
default; on non-Linux systems this server must be run with
`-aof-enabled=false`, making it ephemeral.

### Linux epoll reactor (`cmd/reactor-server`)

Uses non-blocking sockets and Linux `epoll` to dispatch client readiness
through an event loop. It avoids a goroutine per client, but commands execute
on the event-loop path, so long operations can delay other clients. IPv4 and
IPv6 listener addresses are supported; dual-stack behavior for an IPv6
wildcard bind depends on OS configuration.

| Property | Goroutine server | Epoll reactor |
|---|---|---|
| Networking model | Goroutine per client using `net.Conn` | Linux epoll event loop |
| Server networking API | Go `net` package | Linux syscalls |
| AOF platform support | Linux only | Linux only |
| Intended role in this project | Simpler networking implementation | Event-loop implementation for comparison |

Benchmark results above are local measurements with stated conditions, not
capacity guarantees, SLAs, or production-readiness evidence.

---

## Design notes

Carrot was built to explore two concurrency models:

1. A simple per-client goroutine server for clarity and ease of debugging
2. A Linux epoll reactor for lower-overhead, event-driven networking

The repository-level architecture source of truth is
[HLD.md](HLD.md) (system behavior and component interactions) and
[LLD.md](LLD.md) (code structures, algorithms, and synchronization). Read
those documents with the source; package-specific guides are supplementary.

Carrot is not a drop-in Redis replacement. The project goal, out-of-scope
features, readiness phases, and verified test baseline are described in
[docs/PROJECT_SCOPE.md](docs/PROJECT_SCOPE.md),
[docs/TEST_SUMMARY.md](docs/TEST_SUMMARY.md), and
[docs/TEST_EXECUTION_GUIDE.md](docs/TEST_EXECUTION_GUIDE.md).
Contribution and test expectations are documented in
[CONTRIBUTING.md](CONTRIBUTING.md).

---

## License

This project is distributed under the MIT license. See [LICENSE](LICENSE).
