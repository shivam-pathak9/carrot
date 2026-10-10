# Verified test baseline

Validated on 2026-10-07 with Go 1.26.4 on Linux/amd64. The repository has 161
top-level test functions in `*_test.go` files; nested subtests are not counted
separately.

## Package coverage

| Package | Statement coverage | Result |
|---|---:|---|
| `internal/aof` | 75.3% | Pass |
| `internal/client` | 57.7% | Pass |
| `internal/command` | 73.7% | Pass |
| `internal/config` | 92.2% | Pass |
| `internal/cpuprofile` | 81.8% | Pass |
| `internal/protocol/resp` | 78.7% | Pass |
| `internal/reactor` | 69.7% | Pass |
| `internal/server` | 73.0% | Pass |
| `internal/storage` | 84.0% | Pass |

Packages without tests (`cmd/*`) are not represented in these percentages.
Coverage is package-local, not a weighted project-wide percentage.

## Verified checks

The following `make check` targets completed successfully:

```sh
make fmt-check
go test ./...
go test -race ./...
go test -cover ./...
go vet ./...
go build ./...
```

The CI workflow runs the same checks on Linux. To run the command-layer Go
microbenchmarks, use
`go test -run '^$' -bench . ./internal/command`. For the reproducible network
microbenchmark, run `make benchmark`; results and limitations are documented
in the [README](../README.md).

`internal/storage` includes a parallel Set/Get microbenchmark that calls the
store directly. `internal/command` includes a parallel executor Set/Get
benchmark with no journal configured; it exercises executor dispatch and
storage shard locks without AOF ordering overhead. Executor mutations use the
journaled write sequencer only when a journal is configured. Both
benchmarks measure only the current 256-shard configuration; they do not
compare shard counts or an unsharded baseline. `internal/protocol/resp` includes
`FuzzDecoderDoesNotPanic`; ordinary tests run its seed corpus, while sustained
fuzzing is an explicit command in [TEST_EXECUTION_GUIDE.md](./TEST_EXECUTION_GUIDE.md).

## Coverage and test limitations

- Coverage is not correctness: passing tests do not prove protocol
  compatibility, durability, security, or production readiness.
- The suite includes TCP integration tests for both server implementations,
  covering command flows, pipelining, connection limits, request/response
  caps, timeouts, and shutdown. It does not exhaustively test every socket
  failure or shutdown race.
- Redis CLI verification is a manual smoke test; automated tests use Go TCP
  clients.
- AOF tests cover RESP replay, expiration preservation, incomplete-tail
  truncation, file locking, bounded group commit, compaction, rewrite failure
  cleanup, and restart through both server modes. A network-level subprocess
  harness records acknowledgements while writing under `always` and
  `everysec`, force-kills the server process, restarts it, and verifies
  acknowledged keys. On the
  three repeated local runs, all 100 or 101 acknowledged writes survived each
  policy. Each run also had one in-flight unacknowledged attempt; it was
  usually absent but was recovered once. This does not simulate OS/power loss,
  crashes at every write/sync boundary, or device failures.
- Large-scale AOF rewrite performance, sustained rewrite contention,
  resource exhaustion, sustained fuzzing beyond the short CI smoke run, and
  sustained-load behavior are not covered.

For list commands and network behavior, see the [README](../README.md).
