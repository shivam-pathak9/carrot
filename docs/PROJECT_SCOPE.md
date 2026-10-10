# Carrot project scope and readiness

## Product goal

Carrot is an educational, single-node, Redis-inspired server written in Go.
The goal is to explore systems design through small, tested milestones. It is
not a production service, production-readiness project, or Redis replacement.

The project should prioritize data correctness, predictable resource use,
recoverability, operability, and compatibility guarantees before feature
breadth or performance claims.

## Intended scope

The planned product is a single process and a single logical database with:

- A documented and tested subset of RESP and Redis-style commands.
- String, list, hash, set, and sorted-set values, with type validation.
- Expiration behavior that remains consistent across types.
- A documented durability contract and persistence/recovery before data is
  considered safe against process or machine failure.
- Observable health and resource use, configurable operation, and orderly
  startup and shutdown.

Additional data structures and commands are delivered incrementally. Each
increment must include storage semantics, command validation, protocol-level
tests, wrong-type behavior, expiration interactions, and documentation.

## Out of scope unless the goal changes

- Redis drop-in compatibility or support for every Redis command and protocol
  extension.
- Replication, automatic failover, clustering, and multi-node coordination.
- Redis scripting, pub/sub, transactions, modules, and ACL compatibility.
- Durability guarantees stronger than the persistence mode explicitly
  implemented and documented.

## Current behavior and verified baseline

As of 2026-10-03, Carrot has:

- A single-process store with RESP AOF persistence, enabled by default.
  Durability varies by sync policy; manual background compaction is available
  with `AOFREWRITE`, but backup automation is not.
- String commands: `PING`, `GET`, `SET` with `EX`/`PX`/`PXAT`, `DEL`, `TTL`,
  `EXPIRE`, and `PEXPIREAT`.
- Non-blocking list commands documented in the [README](../README.md).
- Two network implementations: a goroutine-per-connection server and a
  Linux-only epoll reactor.
- A RESP decoder with line, bulk-string, and array size limits.
- Validated host, port, connection, request/response size, and timeout settings.
- A maximum connection count, aggregate per-request byte limit, bounded
  encoded responses, and read/write timeouts.
- AOF enabled by default with `always`, `everysec`, and `no` sync policies.
  The AOF replays before accepting connections and records absolute expiration
  deadlines; see [AOF_PERSISTENCE.md](./AOF_PERSISTENCE.md) for its contract
  and limitations.
- SIGINT/SIGTERM handling, listener shutdown, and TCP integration tests for
  both server implementations.
- Host and port flags on both server binaries; the default host is
  `0.0.0.0`.

The repository currently has 161 top-level unit and integration test
functions, including server and reactor TCP behavior. That count is not a
measure of coverage. The current package coverage is recorded in
[TEST_SUMMARY.md](./TEST_SUMMARY.md).

For the implementation-derived architecture reference, see the repository
root [HLD](../HLD.md) and [LLD](../LLD.md). They describe current behavior;
the Go source remains authoritative.

## Production-readiness boundary

Do not expose Carrot to untrusted networks or rely on AOF as the only copy of
important production data. The current implementation has no authentication
or encryption; backup automation and broad crash/failure validation are still
missing. AOF rewrite is manual, and the background snapshot temporarily needs
memory proportional to live data. `always` uses bounded group commit; mutation
application remains ordered. The default bind remains
`0.0.0.0` by choice, with an explicit startup warning; local deployments
should bind to loopback.
There is no total database-memory quota or metrics. Passing tests does not
close these operational gaps.

## Readiness milestones

1. **Phase 0 — Scope and evidence:** completed; scope, supported commands,
   baseline results, and limitations are documented.
2. **Phase 1 — Safe operation:** implemented configuration validation,
   connection/request/response limits, deadlines, signal-driven shutdown, and
   TCP integration tests. The all-interface default was retained by choice
   and is accompanied by a warning; loopback is recommended for local use.
3. **Phase 2 — Durability:** AOF append/replay and manual background rewrite
   are implemented and tested, including recovery and rewrite failure paths.
   A network-level server-process SIGKILL harness checks acknowledged writes
   under `always` and `everysec`; it does not simulate OS/power loss.
   Recovery is also tested against a final record truncated at every byte
   offset. Injected write/fsync/directory-sync failures, OS/power-loss
   validation, and backup procedures remain before this phase is complete.
4. **Phase 3 — Correctness under stress:** race checks, deterministic rewrite
   serialization tests, repeatable network microbenchmarks, and a bounded RESP
   decoder fuzz run in CI are in place. Longer fuzzing, sustained-load testing,
   and broader fault injection remain.
5. **Phase 4 — Operations:** add structured logs, health/readiness reporting,
   metrics, and deployment/recovery guidance.
6. **Feature increments:** complete hashes, sets, and sorted sets one at a
   time, including persistence and integration coverage before calling them
   production-ready.

The phase order is a readiness plan, not a claim that later work has already
been completed.
