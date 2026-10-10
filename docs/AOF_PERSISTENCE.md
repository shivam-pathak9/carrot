# Append-only persistence

This package guide describes AOF-specific behavior. The repository-wide
architecture is documented in [HLD](../HLD.md) and [LLD](../LLD.md); the
implementation in `internal/aof` is authoritative.

Carrot's initial persistence implementation uses a RESP command log. AOF is
enabled by default and writes to `appendonly.aof` in the working directory.
The implementation currently supports Linux only. Other platforms can still
build the repository, but opening AOF fails explicitly because the required
exclusive file lock and directory sync are not implemented there.
Both server binaries accept:

```text
-aof-enabled=true|false
-aof-file PATH
-aof-sync always|everysec|no
```

## Durability contract

- `always`: append and sync a bounded batch of mutations before applying any
  of them or returning their responses. Concurrent writes may share one
  `fsync`; the executor waits up to 250 microseconds to form a batch of at most
  64 commands. A single write retains append-before-apply durability, with a
  small batching delay.
- `everysec` (default): append before applying a mutation and sync periodically.
  A process crash normally leaves appended data available to replay, but an OS
  crash or power loss can lose recently acknowledged writes that were not yet
  synced. The approximate one-second interval is a target, not an absolute
  bound under stalls or sync failures.
- `no`: append before applying a mutation but do not sync periodically.
  Orderly shutdown still syncs; a machine failure can lose a larger amount of
  acknowledged data.

An append or required synchronous write failure rejects the command before its
in-memory mutation. A periodic sync failure is logged and subsequent writes
are rejected. Reads continue to be served. The AOF file is exclusively locked
so two Carrot processes cannot write the same file concurrently.

## Format and recovery

Each record is a RESP command array. Relative `SET EX`/`SET PX` and `EXPIRE`
operations are recorded with absolute `PXAT`/`PEXPIREAT` deadlines, so replay
does not renew a key's lifetime. Recovery replays records before the server
accepts client connections. An incomplete final RESP command is truncated to
the last complete record; malformed complete data fails startup rather than
silently skipping history. Recovery streams records from disk instead of
loading the complete AOF into memory; temporary parsing memory is bounded by
the RESP decoder buffer and the configured maximum record size.

The initial log covers string and list mutation command families. Read-only
commands are not added. A syntactically canonicalizable mutation is appended
before execution, so a command that turns out to be a no-op or returns a
command-level error (for example, a list operation against a string key) can
still be recorded. Replay preserves the data state, but repeated rejected
requests grow the AOF until rewrite. Filtering those records safely requires
a command validation/commit design that keeps append-before-apply semantics;
executing first could change memory if the subsequent append fails.

## Current limitations

Use `AOFREWRITE` to start a background compaction and `AOFREWRITE STATUS` to
inspect it. Start returns `+OK`; status returns a two-element array containing
`queued`, `running`, `completed`, or `failed`, followed by an error message
(empty unless the job failed). Only one rewrite may run at a time.

The executor briefly orders a point-in-time snapshot against mutations, then
the rewrite worker serializes that copy while writes continue. Concurrent
mutations are appended to the active AOF and a temporary delta file. Before
installation, the executor takes a mutation barrier, appends the delta to the
replacement, syncs it, and atomically renames it over the old AOF. This reduces
the pause from snapshot I/O to snapshot copying plus delta installation; a
large accumulated delta can still make the final pause noticeable. Snapshot
copying temporarily needs memory proportional to live database contents, and
the old AOF, snapshot, and delta temporarily consume additional disk space.
Carrot does not fork a child for rewrite: it trades copy-on-write process
semantics for an explicit in-process snapshot and delta log, keeping rewrite
errors and journal ownership in the existing process at the cost of that
temporary memory copy.

If snapshot or delta writing fails before rename, the original AOF remains in
place and status reports failure. If syncing the directory after rename fails,
the replacement is active but later writes are rejected until restart. The
reactor event loop can continue processing while the snapshot is serialized,
but requests that need a mutation barrier can wait during snapshot capture or
final installation. With `always`, concurrent writes share one `fsync` when
they arrive within the bounded group-commit window.

Compaction reduces accumulated history but does not run automatically. There
is no backup automation, replication, checksum, or disk-failure protection.
Use `-aof-enabled=false` only when data loss on restart is acceptable.
Persistence is not a substitute for backups or a complete production-readiness
review.

## Implementation map and write/recovery flow

The implementation is split across a few focused files:

- `internal/aof/aof.go` owns the AOF lifecycle and format: `Open` locks and
  recovers the file; `AppendBatch` canonicalizes and writes ordered RESP
  commands with one sync per batch under `always`; `StartRewrite`,
  `CompleteRewrite`, and `AbortRewrite` implement background compaction;
  `rollback` removes a failed append and marks the log unhealthy if recovery
  of the append itself fails; `syncLoop` implements `everysec`;
  `Close` stops the sync loop, syncs, unlocks, and closes; `replay` streams
  records to reconstruct the store without buffering the entire AOF in memory.
- `internal/aof/lock_linux.go` uses Linux `flock` to prevent concurrent Carrot
  processes from using the same file. The `lock_other.go` counterpart rejects
  non-Linux use instead of silently proceeding without a lock.
- `internal/aof/sync_directory_linux.go` syncs the parent directory when a new
  AOF is created, so the filename entry is made durable as well as file data.
  `sync_directory_other.go` reports unsupported use on non-Linux systems.
- `internal/aof/aof_test.go` tests record replay, expiration deadlines,
  truncation at every byte boundary of a final record, corrupt records,
  exclusive locking, sync policy, and failed appends.
- `internal/command/executor.go` is the shared write boundary used by both
  servers. Its bounded write sequencer batches concurrent journal appends,
  then applies them in journal order. Rewrite snapshot capture and final
  installation use the mutation barrier; intervening writes are retained in
  the rewrite delta and replayed after the snapshot.
- `internal/storage/storage.go` provides `SnapshotEntries`, which copies the
  live database for a point-in-time background rewrite. This temporarily uses
  memory proportional to the live data set.
- `internal/server/server.go` and `internal/reactor/server.go` open/replay
  persistence before accepting commands and close it during shutdown.

For a client mutation, the flow is:

1. The server parses the RESP request into a command and calls the shared
   executor.
2. The executor queues journaled mutations into an ordered batch. The batch
   converts relative expirations to absolute deadlines and appends RESP records
   before any mutation is applied. With `always`, one `fsync` covers the batch.
3. The executor applies each returned canonical command in journal order. If
   append or required sync fails, no command in the batch is applied.
5. At startup, `Open` obtains the exclusive lock and `replay` applies complete
   records to a fresh store before the listener/event loop starts.

The same executor path is used during replay with no journal attached, which
prevents replayed commands from being appended again.

During `AOFREWRITE`, the executor's mutation barrier creates a point-in-time
snapshot copy. A background worker writes the copy while concurrent mutations
continue to append to the active AOF and delta file. At completion, journaled
mutations pause while the delta is appended and the synced replacement is
renamed into place. The `Log` switches its descriptor before releasing the old
file lock, preserving writes across compaction.
