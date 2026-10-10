# Test execution guide

Run commands from the repository root.

## Standard checks

```sh
make check
# Or run commands individually:
go test ./...
go test -race ./...
go test -cover ./...
go build ./...
go vet ./...
```

`make check` runs formatting, unit/integration tests, race tests, coverage,
`go vet`, and builds all packages. GitHub Actions runs the same checks on
Linux for pushes and pull requests. CI also runs the RESP decoder fuzz target
for five seconds after the regular unit tests.

## Network end-to-end harness

On Linux/WSL, run the Python standard-library harness against both server
implementations:

```sh
make test-e2e
# Or select one server / skip the AOF restart-and-rewrite suite:
python3 scripts/python_e2e.py --server standard
python3 scripts/python_e2e.py --server reactor --skip-aof
```

The harness builds its binaries in a temporary directory, selects loopback
ports dynamically, and removes its logs and AOF files when it exits. It checks
RESP fragmentation and pipelining, documented string/list behavior, malformed
requests, concurrent clients, and AOF rewrite/restart recovery. This
supplements rather than replaces the focused Go tests; the server-process
tests do not simulate OS or power loss.

## Reproducible server benchmark

Install Redis (`redis-server`, `redis-cli`, and `redis-benchmark`), then run:

```sh
make benchmark | tee benchmark-results.txt
```

The script builds both Linux server modes and starts an isolated temporary
Redis instance. Persistence is disabled for all three servers so measurements
focus on network and in-memory command handling. By default it runs three
rounds of the same PING/string/list command cases at pipeline depths 1 and 16.
It reports the date, host/kernel, Go version, Redis benchmark and server
versions, and benchmark parameters. Set `BENCH_PIPELINE=1` to run only depth 1,
or set it to another positive integer to run depth 1 and that depth. Override
`BENCH_RUNS`, `BENCH_REQUESTS`, `BENCH_CLIENTS`, `BENCH_DATA_SIZE`,
`STANDARD_PORT`, `REACTOR_PORT`, and `REDIS_PORT` as needed. It refuses ports
where a Redis-compatible server already responds, and stops only the server
processes it starts.

These measurements are local comparisons, not capacity guarantees or SLAs.
For durability-enabled write throughput, repeat separately with AOF enabled
and report the sync policy; do not compare those results as if they had the
same durability cost. To collect diagnostic CPU profiles for the Carrot
servers, set `BENCH_CPU_PROFILE_DIR=/tmp/carrot-pprof`; the profiles cover the
complete Carrot benchmark runs and exclude Redis. Profiling adds overhead and
invalidates throughput comparison. See the README for the measured profile
summary and limitations.

Command-layer Go benchmarks can be run without the network server:

```sh
go test -run '^$' -bench . ./internal/command
go test -run '^$' -bench . ./internal/storage
```

The storage parallel benchmark runs directly against the sharded store,
outside the journaled write sequencer; it does not compare shard counts or
prove that 256 is optimal. `BenchmarkExecutorParallelSetGet` also measures
command execution without a journal, where the write sequencer is bypassed.
It does not measure AOF-enabled contention. These benchmarks measure
in-process paths, not client-to-server throughput.

The RESP decoder fuzz target can be run for a bounded duration with:

```sh
go test ./internal/protocol/resp -run '^$' -fuzz '^FuzzDecoderDoesNotPanic$' -fuzztime=30s
```

The seed corpus also runs as part of ordinary `go test`. The fuzz target checks
that arbitrary bounded byte sequences do not panic; it is not a proof of full
protocol correctness or security.

## AOF process-crash test

Run the network-level crash harness on Linux with:

```sh
go test -v ./internal/server -run '^TestCrashHarnessAcknowledgedWritesSurviveSIGKILL$'
```

For each of `always` and `everysec`, the test starts a child server, sends
unique `SET` requests while recording successful RESP acknowledgements,
force-kills the server process, restarts it against the same AOF, and checks
each acknowledged key. It also reports attempted-but-unacknowledged requests
and whether each was present after recovery. In three repeated local runs,
each policy recovered every acknowledged write (100 or 101 acknowledgements
per run). One additional attempted request was unacknowledged in each run; it
was usually absent after recovery but was recovered once. These observations
are not guarantees for other machines or filesystems.

The test kills only the server process. The operating system and its page
cache remain alive, so it does not validate the `everysec` loss window after
an OS crash or power loss. `everysec` syncs periodically (targeting roughly
one second); acknowledged writes since the last successful sync may be lost
in a machine-level failure, and scheduler/filesystem stalls mean one second
is not a strict upper bound. `always` also cannot guarantee survival of
hardware or filesystem failures beyond what the OS reports as a successful
sync.

The AOF unit suite also truncates a valid final record at every byte offset
and verifies that recovery preserves all preceding records while discarding
the incomplete tail. This covers torn-record parsing boundaries, not injected
write, `fsync`, directory-sync, or machine-power failures. A live Redis
differential test for the common command subset runs with
`go test ./internal/command` when `redis-server` is installed; otherwise it
skips.

Run one package or test when iterating:

```sh
go test ./internal/storage
go test ./internal/command
go test -run '^TestListCommandFlow$' ./internal/command
```

For a detailed coverage profile:

```sh
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

The generated `coverage.out` is ignored by Git.

## Manual Redis CLI smoke test

`redis-cli` is an optional external tool and must be installed separately.
Start the standard server in a terminal:

```sh
go run ./cmd/server -host 127.0.0.1 -port 16379
```

In another terminal:

```sh
redis-cli -h 127.0.0.1 -p 16379
```

Use the equivalent `go run ./cmd/reactor-server -host 127.0.0.1 -port 16379`
to exercise the Linux epoll server. Stop the server with Ctrl-C after testing.

Automated TCP integration tests for both server modes are included in
`go test ./...`. The manual Redis CLI steps are useful for checking
client-facing behavior. Do not bind to a public interface for unauthenticated
testing.
