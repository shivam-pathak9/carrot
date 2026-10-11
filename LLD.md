# Carrot low-level design

This is a compact map of the implementation and its key synchronization
boundaries. The [HLD](./HLD.md) explains system-level behavior; package source
is authoritative when prose and code differ.

## Components

| Package | Responsibility |
|---|---|
| `internal/protocol/resp` | Bounded RESP decoding and encoding |
| `internal/command` | Parse commands, dispatch handlers, and sequence journaled mutations |
| `internal/storage` | Typed string/list values, expiration, memory estimate, LRU sampling, sharded access, and rewrite snapshots |
| `internal/aof` | AOF locking, canonical append/replay, group commit, sync policies, and rewrite |
| `internal/server` | Standard `net.Conn` listener with a goroutine per client |
| `internal/reactor` | Linux epoll loop, non-blocking client I/O, and synchronous command dispatch |

Both servers use the same parser, executor, storage, and AOF implementation.
Their networking models differ; the reactor does not move command execution
off its event-loop goroutine.

## Command execution and write ordering

Without a journal, mutations execute through the regular command dispatch path
and storage shard locks provide synchronization. With the AOF journal,
`Executor` sends mutation requests to a bounded queue serviced by one write
dispatcher:

1. The dispatcher collects up to 64 queued mutations. Under `always`, it waits
   at most 250 microseconds for requests to join the batch; other policies
   batch requests already waiting without an intentional delay.
2. `Log.AppendBatch` canonicalizes relative expiry commands, writes valid
   records in order, flushes the batch, and calls `file.Sync` once for the
   batch under `always`.
3. Only after the append and required sync succeed does the executor apply
   canonicalized mutations in journal order and return each result.

The mutation barrier covers append and in-memory application. It preserves a
single order between the AOF and live state and synchronizes snapshot capture
and final rewrite installation. Group commit amortizes fsync cost; it does not
make journaled mutations execute in parallel. Writes to unrelated keys still
pass through this sequencer while AOF is enabled.

On append or required sync failure, the complete batch is rejected before
any mutation is applied. The AOF rolls back the failed append; if rollback
fails, it marks the log unhealthy and rejects later writes. Command-level
errors are RESP errors and may still be logged when the request is
canonicalizable, so an invalid mutation can add history without changing data.

## AOF policies and rewrite

The AOF is a sequence of RESP command arrays. Startup locks the file and
replays complete mutation records before serving clients. Relative expirations
are stored as absolute deadlines. An incomplete final record is truncated;
malformed complete records fail startup.

| Policy | Append behavior | Durability boundary |
|---|---|---|
| `always` | Append, flush, and sync each bounded group before applying it | One fsync per batch; concurrent writes can share that sync |
| `everysec` | Append before apply; a periodic goroutine syncs the file | Recent acknowledged writes can be lost after OS/power failure |
| `no` | Append before apply without periodic sync | Orderly close syncs; machine failure can lose more recent data |

`AOFREWRITE` is asynchronous for network clients. The executor uses the
mutation barrier to copy a point-in-time store snapshot, then the AOF worker
serializes it to a temporary file while writes continue. Each continuing
mutation is appended to the active AOF and a sidecar delta. Installation
takes the mutation barrier briefly, appends the delta, syncs the replacement,
renames it over the active path, and syncs the directory. The old file remains
the recovery source if snapshot writing fails before rename.

Snapshot copying uses memory proportional to live data, and the final pause
depends on delta size. Both the snapshot and delta temporarily consume disk
space. Rewrite is manual; it does not provide backup, replication, checksums,
or hardware-failure protection. AOF currently requires Linux for file locking
and directory syncing.

## Storage and networking

`storage.Store` routes keys by FNV-1a to 256 fixed shards, each protected by an
`RWMutex`. This permits concurrent direct storage operations on different
shards, but no benchmark establishes that 256 shards is optimal. AOF-enabled
mutation ordering is centralized above the store, so sharding does not make
those mutations concurrent.

All shard writes go through `shard.put` and `shard.remove` in
`internal/storage`. They maintain the store-wide memory estimate, a per-shard
index of all keys (for LRU sampling), and an index of keys that may expire (for
active expiry). Writing to `shard.data` directly would desynchronize these.

`command.Executor.admit` (`internal/command/memory.go`) enforces `-maxmemory`
before commands that add data, under the mutation lock when a journal is
installed. `Executor.ActiveExpire` re-checks sampled expired keys and journals
a `DEL` for each before deleting. See the HLD for the eviction flow.

The standard server handles each connection in a goroutine. The reactor uses
non-blocking sockets and epoll, with one event-loop path for reads, parsing,
command execution, and output scheduling. This avoids a goroutine per client
but serializes command execution on that loop. CPU profiles and network
benchmarks are diagnostic evidence, not capacity guarantees; see the
[benchmarks](#benchmarks) and [testing](#testing).

## Reactor internals

The reactor lives in `internal/reactor` and is four small files:

| File | Job |
|---|---|
| `poller.go` | Thin wrapper over `epoll_create1`, `epoll_ctl` and `epoll_wait` |
| `connection.go` | Non-blocking fd, input buffer, bounded output buffer, RESP framing |
| `event_loop.go` | Accept loop, readiness dispatch, command execution, active expiry |
| `server.go` | Listener socket setup (IPv4/IPv6), start and stop |

A connection is registered for `EPOLLIN | EPOLLERR | EPOLLRDHUP`. When a write
hits `EAGAIN`, the loop adds `EPOLLOUT` and flushes on the next event, then
drops it again once the output buffer is empty. Reads accumulate bytes, decode
every complete RESP value, and keep any incomplete tail for the next read, so
pipelined requests are handled in order.

| Errno / result | Where | What the reactor does |
|---|---|---|
| `EAGAIN` | `accept4` | Backlog drained, go back to `epoll_wait` |
| `EAGAIN` | `read` | Buffer drained, parse what we have |
| `EAGAIN` | `write` | Turn on `EPOLLOUT`, resume later |
| `EINTR` | any syscall | Retry |
| 0 bytes read | `read` | Peer closed, unregister and close the fd |

## Testing

The repo has 180 top-level test functions (subtests not counted separately).
Last verified on 2026-10-11 with Go 1.26.4 on Linux/amd64:

| Package | Coverage |
|---|---:|
| `internal/aof` | 75.3% |
| `internal/client` | 57.7% |
| `internal/command` | 75.6% |
| `internal/config` | 94.3% |
| `internal/cpuprofile` | 81.8% |
| `internal/protocol/resp` | 78.7% |
| `internal/reactor` | 70.0% |
| `internal/server` | 73.8% |
| `internal/storage` | 86.0% |

Coverage is per package, not a project-wide number, and `cmd/*` has no tests.

```sh
make check      # gofmt, go test, go test -race, coverage, go vet, build
make test-e2e   # Python harness against both servers (Linux/WSL)
make demo       # scripted walkthrough against a real server
make demo-scale # 10,000 clients, kill -9 under load, 50 MB memory flood
make benchmark  # redis-benchmark: goroutine server vs reactor vs Redis
go test -run '^$' -bench . ./internal/command ./internal/storage
go test -v ./internal/server -run TestCrashHarnessAcknowledgedWritesSurviveSIGKILL
```

What the tests cover:

- **Unit and integration:** RESP framing, commands, wrong-type errors,
  expiry, the TTL index, memory accounting, LRU victim selection, `-OOM`
  rejection, TCP flows for both servers (pipelining, limits, timeouts, shutdown).
- **AOF:** replay, absolute expiry deadlines, a final record truncated at every
  byte offset, file locking, group commit, rewrite and rewrite failure cleanup,
  `DEL` journaling for expired and evicted keys, and restart through both servers.
- **Crash harness:** a subprocess writes under `always` and `everysec`, gets
  SIGKILLed, restarts on the same AOF, and checks that every acknowledged key
  survived (100 or 101 per run, three runs per policy). It kills the process
  only, so it says nothing about power loss.
- **Differential:** if `redis-server` is installed, the common string/list
  subset is compared against real Redis.
- **CI:** GitHub Actions runs the checks, the E2E harness and a short RESP
  decoder fuzz run.

What they do not cover: OS or power-loss injection, failed `fsync` injection,
large-AOF rewrite performance, sustained load, long fuzzing, and sampled-LRU
quality (checked only by hand, see the benchmarks).

The benchmark script builds both servers, starts a private Redis, disables
persistence on all three and runs depths 1 and 16 three times. Overrides:
`BENCH_PIPELINE`, `BENCH_RUNS`, `BENCH_REQUESTS`, `BENCH_CLIENTS`,
`BENCH_DATA_SIZE`, `STANDARD_PORT`, `REACTOR_PORT`, `REDIS_PORT`, and
`BENCH_CPU_PROFILE_DIR` for CPU profiles (profiled numbers are not benchmark
results).

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
test; it does not establish durability against OS or hardware failure. See the AOF section above for rewrite behavior and limits.

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


## Benchmarks

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

### Memory limit overhead and behavior

Measured with `redis-benchmark -t set,get -n 200000 -c 10 -P 16 -d 100
-r 1000000`, AOF disabled, `cmd/server`, WSL2. Medians of 7 interleaved runs
(the servers ran side by side). "Last commit" is the build before the
active-expiry and `maxmemory` changes.

| Config | SET median rps | GET median rps | p99 SET / GET |
| --- | ---: | ---: | --- |
| Last commit | 156,250 | 167,785 | 1.70 / 1.61 ms |
| New, no limit | 147,167 | 163,934 | 2.30 / 1.94 ms |
| New, `-maxmemory 5mb -maxmemory-policy allkeys-lru` (every `SET` evicts) | 142,755 | 168,776 | 1.89 / 1.71 ms |

`SET` is about 6% lower than the last commit; `GET` is within noise. Run-to-run
variation on WSL2 was as large as these gaps (one config's `SET` ranged from
126k to 163k rps), so read this as "no large regression", not a precise cost.
AOF-enabled writes, the reactor server, and eviction under `always` sync were
not measured.

Behavior check against a running server with a 1 MB limit and 100-byte values
(a small script over RESP, because `redis-benchmark` aborts on the first
error reply):

| Policy | Result |
| --- | --- |
| `noeviction` | 3,507 of 20,000 writes accepted (about 298 estimated bytes per key); the rest returned `-OOM`. All accepted keys stayed readable, `GET` kept working at the limit, a rejected `SET` was not stored, and `DEL` freed space for the next `SET`. |
| `allkeys-lru` | 60,000 writes, 0 errors, about 3,400 keys resident (bounded). The newest 100 keys were all present. With 50 hot keys read every 100 writes or more often, all 50 survived; read every 500 writes, 40 to 42 of 50 survived, which is expected from sampling 5 keys per eviction. |

Reproduce the network comparison and Redis reference baseline with
`make benchmark`; by default it runs pipeline depths 1 and 16, three times
each, and prints environment and parameter details. The script requires
`redis-server`, `redis-cli`, and `redis-benchmark`, uses three distinct
loopback ports, and refuses to use ports where a Redis-compatible service
already responds. Set `BENCH_PIPELINE=1` to run only depth 1, or set it to
another positive depth to run depth 1 and that depth. See
the Testing section for further
overrides. Reproduce the store benchmark with
`go test -run '^$' -bench '^BenchmarkStoreParallelSetGet$' -benchtime=3s -count=3 ./internal/storage`.

These local microbenchmarks are not capacity estimates, SLAs, or production
comparisons. They do not measure AOF durability, TLS, external network
latency, or every implemented/unsupported command. `redis-benchmark` warns
that it cannot fetch `CONFIG`, which Carrot does not implement; the selected
cases completed despite that warning.

---
## Contributing

Keep changes small and focused, run `make check` before sending anything, and
add tests for observable behavior (bad arguments, wrong types, expiry,
recovery). Use table-driven tests when they help. Comment invariants and
trade-offs, not what the code already says. Keep persistence and network
errors explicit. Do not describe microbenchmarks as capacity or SLAs, and say
which OS and durability setting a result came from.
