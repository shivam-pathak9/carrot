# Contributing

Carrot is an educational Redis-inspired server and is not production-ready.
Changes should preserve the documented RESP and command behavior and should
not imply compatibility or durability guarantees beyond those documented.

## Before opening a change

Run the repository checks from the root:

```sh
make check
```

For changes to command or storage performance, run the focused Go benchmarks:

```sh
go test -run '^$' -bench . ./internal/command
go test -run '^$' -bench . ./internal/storage
```

The storage package's `BenchmarkStoreParallelSetGet` measures direct
concurrent store calls. `BenchmarkExecutorParallelSetGet` measures concurrent
commands with AOF disabled; this bypasses the journal-ordering mutex but uses
the same sharded store. Both use the existing 256-shard configuration and do
not compare shard counts.

The network-level comparison is available with `make benchmark`. It requires
`redis-cli` and `redis-benchmark`, binds only to loopback, and disables AOF;
report the host and benchmark settings with any results.

## Implementation and tests

- Keep changes focused and follow the package boundaries in the README.
- Add tests for observable behavior, including invalid arguments, wrong-type
  operations, expiration, and persistence/recovery effects where applicable.
- Use table-driven tests when they make related edge cases clearer.
- Add comments for non-obvious invariants and design tradeoffs; avoid comments
  that merely restate the code.
- Preserve explicit errors for failed persistence and network operations.
- AOF currently requires Linux. Tests touching server modes or AOF should state
  their platform assumptions.

## Reporting results

Do not describe local microbenchmarks as SLAs or production capacity.
Document limitations, operating-system assumptions, and durability settings
when they affect behavior.
