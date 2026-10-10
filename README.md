<p align="center">
  <img src="assets/carrot_logo.png" alt="Carrot logo" width="190" />
</p>

<h1 align="center">🥕 Carrot</h1>

<p align="center">
  <b>A Redis-inspired in-memory data server, built in Go.</b><br/>
  Explore RESP, append-only persistence, and two contrasting TCP concurrency models.
</p>

<p align="center">
  <a href="https://github.com/shivam-pathak9/carrot/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/shivam-pathak9/carrot/actions/workflows/ci.yml/badge.svg"></a>
  <img alt="Go 1.26.4" src="https://img.shields.io/badge/Go-1.26.4-00ADD8?logo=go&logoColor=white">
  <a href="LICENSE"><img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue.svg"></a>
</p>

## Why Carrot?

Carrot is a systems project focused on correctness and trade-offs—not a claim
of Redis compatibility or production readiness. It currently demonstrates:

| Area | What is implemented |
|---|---|
| Protocol & commands | RESP framing; string and list commands; type and TTL semantics |
| Storage | Sharded in-memory store with passive and bounded active expiration |
| Persistence | Linux AOF append, replay, group commit, sync policies, and background rewrite |
| Networking | Portable goroutine-per-client server and Linux epoll reactor |
| Reliability | Resource limits, graceful shutdown, race-tested packages, and TCP E2E coverage |

The two servers share the command and storage layers, making their networking
trade-offs directly comparable. The reactor is Linux-only and runs command
execution on its event loop; it is not automatically faster for every workload.

## Quick start

Requires Go 1.26.4. On Linux, start the standard server on loopback:

```bash
go run ./cmd/server -host 127.0.0.1 -port 16379
```

In another terminal, use `redis-cli` or any RESP client:

```bash
redis-cli -h 127.0.0.1 -p 16379 PING
redis-cli -h 127.0.0.1 -p 16379 SET greeting "Hello, Carrot!"
redis-cli -h 127.0.0.1 -p 16379 GET greeting
```

The server enables AOF persistence by default and writes `appendonly.aof` in
its working directory. See [AOF persistence](docs/AOF_PERSISTENCE.md) before
changing sync policy or relying on stored data. The default server bind is
`0.0.0.0`; the example deliberately binds to loopback because Carrot has no
authentication or transport encryption.

## Architecture at a glance

```text
RESP client
    |
    +-- cmd/server         -> goroutine per connection --+
    |                                                     |
    +-- cmd/reactor-server -> Linux epoll event loop -----+--> command executor
                                                           |       |
                                                           |       +--> sharded in-memory store
                                                           |       +--> AOF journal (Linux)
                                                           |
                                                           +--> RESP replies
```

| Path | Purpose |
|---|---|
| `cmd/server` | Portable Go TCP server |
| `cmd/reactor-server` | Linux epoll server |
| `internal/protocol/resp` | RESP decoder and encoder |
| `internal/command` | Command parsing, validation, execution, and AOF coordination |
| `internal/storage` | Sharded strings/lists and TTL expiration |
| `internal/aof` | Linux append-only journal, recovery, and compaction |

More detail: [high-level design](HLD.md) · [low-level design](LLD.md) ·
[project scope and readiness](docs/PROJECT_SCOPE.md).

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
redis-cli -h 127.0.0.1 -p 16379 AOFREWRITE
```

`AOFREWRITE` starts a background snapshot rewrite and returns `OK`; check
progress with `AOFREWRITE STATUS`. Writes continue during snapshot serialization
and are captured in a delta log. They pause briefly to install the delta and
replacement. Snapshotting temporarily uses memory proportional to the live
dataset; see [AOF persistence](docs/AOF_PERSISTENCE.md) for details.

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
# Optional network-level coverage (Linux/WSL):
make test-e2e
```

`make check` runs formatting, tests, race checks, coverage, vet, and builds.
`make test-e2e` exercises both network servers, protocol boundaries,
concurrency, and AOF restart/rewrite behavior. GitHub Actions runs the Go
checks, the Linux E2E harness, and a bounded RESP decoder fuzz run.
When `redis-server` is installed, `go test ./internal/command` also runs a
live differential test for the common string/list command subset; otherwise
that test is skipped.

---

## Current status

Carrot is an MVP and is **not production-ready**. AOF persistence, restart
recovery, group commit, and background rewrite/compaction are implemented, but broader
crash/failure validation and backup procedures are still outstanding.
Durability depends on the configured sync policy. It also has no authentication
or transport encryption, and the default bind address is `0.0.0.0`. Use
loopback for local testing and do not expose it to untrusted networks.

The project currently demonstrates:

- A documented subset of RESP and Redis-style string/list commands
- In-memory typed storage and TTL behavior
- AOF append/recovery, group commit, and background rewrite/compaction
- Two networking models: goroutine-per-connection and Linux epoll
- Unit and TCP integration tests for storage, commands, persistence, protocol,
  configuration, and both server implementations

Important gaps before production use still include:

- OS/power-loss and disk-failure injection beyond the server-process SIGKILL
  recovery harness, plus backup procedures
- Authentication, transport security, and a safer default bind policy
- Total database memory quotas, metrics, health checks, and deployment guidance
- Hashes, sets, sorted sets, and blocking list commands

## Local benchmarks

The following loopback comparison was run on 2026-10-05 with
`redis-benchmark 7.0.15`, Redis Server 7.0.15, and Go 1.26.4, on WSL2
Linux/amd64 (12 logical CPUs, kernel
`6.18.33.2-microsoft-standard-WSL2`). AOF/persistence was disabled for all
three servers. Each of three runs sent 10,000 requests per command with 10
clients and a 16-byte payload. Numbers below are medians of the three
reported throughputs, in requests per second; these are observations on this
host, not a general ranking or capacity guarantee.

### Pipeline depth 1

| Command | Goroutine req/s | Reactor req/s | Redis req/s |
|---|---:|---:|---:|
| PING_INLINE | 29,412 | 40,816 | 49,751 |
| PING_MBULK | 30,030 | 45,045 | 52,910 |
| SET | 29,240 | 41,152 | 52,910 |
| GET | 28,902 | 42,553 | 53,763 |
| LPUSH | 28,490 | 42,918 | 50,251 |
| RPUSH | 27,548 | 40,323 | 53,763 |
| LPOP | 27,778 | 39,370 | 54,945 |
| RPOP | 27,933 | 37,313 | 57,471 |

### Pipeline depth 16

| Command | Goroutine req/s | Reactor req/s | Redis req/s |
|---|---:|---:|---:|
| PING_INLINE | 126,582 | 232,558 | 909,091 |
| PING_MBULK | 129,870 | 192,308 | 769,231 |
| SET | 125,000 | 178,571 | 588,235 |
| GET | 119,048 | 178,571 | 714,286 |
| LPUSH | 119,048 | 175,439 | 588,235 |
| RPUSH | 107,527 | 151,515 | 714,286 |
| LPOP | 109,890 | 158,730 | 588,235 |
| RPOP | 105,263 | 169,492 | 666,667 |

In this run, the reactor delivered about 1.34–1.51x the goroutine server's
throughput at pipeline depth 1, and 1.41–1.84x at depth 16. Redis was about
1.17–1.54x faster than the reactor at depth 1 and 3.3–4.7x faster at depth 16.
Pipelining raised throughput but also increased observed request latency;
for example, the reactor's depth-16 p95 latencies ranged from about 1.31 to
1.74 ms across these commands. The earlier 2026-10-04 reactor-only depth-16
measurements in this environment were 19–47% lower across the same command
cases; this before/after comparison is suggestive, not a controlled attribution
of the difference solely to the parser change.

The depth-16 gap is an observation, not an explained Redis-vs-Go benchmark
result. A separate diagnostic run used one 50,000-request round at depths 1
and 16 on the same WSL2 host, with CPU profiling enabled. The reactor profile's
largest flat sample was in Linux syscall handling (`Syscall6`, about 42%); it
also showed time in Go runtime allocation/GC and RESP decoding. This points to
the network/read/parse/runtime path as worthwhile places to investigate, but
does not isolate a single cause or profile Redis. The reactor also processes
commands serially on its event loop; depth-16 throughput is not evidence of
parallel command execution. The 10,000-request, three-round measurements above
are a small local sample, and neither those numbers nor the profile establish
which Redis implementation detail accounts for the remaining difference.

To collect diagnostic profiles (not valid throughput numbers), run:

```sh
BENCH_RUNS=1 BENCH_REQUESTS=50000 BENCH_CPU_PROFILE_DIR=/tmp/carrot-pprof make benchmark
go tool pprof -top /tmp/carrot-pprof/reactor.cpu.pprof
go tool pprof -top /tmp/carrot-pprof/standard.cpu.pprof
```

Each profile covers that Carrot server's complete benchmark run, including
both pipeline depths; it does not include Redis. Profiling adds overhead, so
do not compare the profiled throughputs with the unprofiled table above.

### AOF SET throughput by sync policy

A focused write benchmark was run on 2026-10-07 with the standard
goroutine-per-client server on the same WSL2 Linux/amd64 host described above
(Go 1.26.4, Redis benchmark tools 7.0.15). Each run issued 20,000 `SET`
requests with 10 clients and a 16-byte payload; each cell is the median of
three runs. The `always` and `everysec` runs used distinct AOF files. These
are network benchmark observations, not a controlled storage-device test.

| AOF policy | Pipeline | Median req/s (run range) | Median p95 ms | Median p99 ms |
|---|---:|---:|---:|---:|
| Disabled | 1 | 23,810 (21,075–24,752) | 0.791 | 1.527 |
| Disabled | 16 | 99,502 (99,502–99,502) | 1.647 | 3.455 |
| `everysec` | 1 | 20,080 (19,608–20,812) | 1.079 | 1.951 |
| `everysec` | 16 | 49,020 (41,322–51,680) | 1.359 | 2.903 |
| `always` | 1 | 1,415 (1,199–1,499) | 12.071 | 15.983 |
| `always` | 16 | 1,430 (1,003–1,774) | 18.063 | 22.991 |

In this sample, `always` delivered roughly 14x fewer SETs/s than `everysec`
at depth 1 and 34x fewer at depth 16. Its depth-16 throughput was not
materially higher than depth 1, and its p95 latency increased. This indicates
that the synchronous durability cost remained dominant on this WSL2 setup
despite the bounded group-commit implementation. It does **not** measure the
improvement over the earlier per-write-fsync implementation: no matched run
of that old implementation was collected, and fsync counts per batch were not
instrumented. `everysec` also has a weaker crash/power-loss durability
contract than `always`.

To reproduce the SET workload, start the standard server separately for each
policy (use a unique port and AOF path, and use `-aof-enabled=false` for the
disabled row), then run both pipeline depths three times:

```sh
redis-benchmark --csv -h 127.0.0.1 -p 16379 \
  -n 20000 -c 10 -d 16 -P 1 -t set
redis-benchmark --csv -h 127.0.0.1 -p 16379 \
  -n 20000 -c 10 -d 16 -P 16 -t set
```

The full 2026-10-07 Redis/Carrot multi-command rerun was not added to the
comparison table: it produced negative RPS readings for some rows and large
run-to-run swings on this host. The earlier multi-command table above remains
a separate 2026-10-05 observation; do not combine it with the AOF SET results.

For a direct storage-level parallel measurement, this host reported
`BenchmarkStoreParallelSetGet` at 53.11, 60.45, and 58.33 ns/op (median
58.33 ns/op; 0 allocations/op). One benchmark operation is a `Set` followed
by a `GetString`, running directly against `Store` with `b.RunParallel`.
It bypasses the journaled write sequencer, compares only the current 256-shard
implementation, and is not evidence that command mutations execute in
parallel or that 256 shards is optimal.

The AOF-disabled `BenchmarkExecutorParallelSetGet` measured 135.0, 130.9,
and 129.0 ns/op (median 130.9 ns/op; 48 B/op and 2 allocations/op). Each
iteration executes `SET` then `GET` through `Executor` on a worker-selected
key. With no journal, executor mutations rely on shard locks and bypass the
journaled write sequencer. This is an in-process microbenchmark, not a network
benchmark or an AOF-enabled contention measurement.

Reproduce the network comparison and Redis reference baseline with
`make benchmark`; by default it runs pipeline depths 1 and 16, three times
each, and prints environment and parameter details. The script requires
`redis-server`, `redis-cli`, and `redis-benchmark`, uses three distinct
loopback ports, and refuses to use ports where a Redis-compatible service
already responds. Set `BENCH_PIPELINE=1` to run only depth 1, or set it to
another positive depth to run depth 1 and that depth. See
[docs/TEST_EXECUTION_GUIDE.md](docs/TEST_EXECUTION_GUIDE.md) for further
overrides. Reproduce the store benchmark with
`go test -run '^$' -bench '^BenchmarkStoreParallelSetGet$' -benchtime=3s -count=3 ./internal/storage`.

These local microbenchmarks are not capacity estimates, SLAs, or production
comparisons. They do not measure AOF durability, TLS, external network
latency, or every implemented/unsupported command. `redis-benchmark` warns
that it cannot fetch `CONFIG`, which Carrot does not implement; the selected
cases completed despite that warning.

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
2. A Linux epoll reactor for readiness-driven networking and comparison

The repository-level architecture source of truth is
[HLD.md](HLD.md) (system behavior and component interactions) and
[LLD.md](LLD.md) (implementation map and synchronization boundaries). Read
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
