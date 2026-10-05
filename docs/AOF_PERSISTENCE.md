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

- `always`: append and sync each mutating command before applying it in memory
  or returning its response. This is the strongest local durability mode, but
  adds an `fsync` to each write.
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

Use `AOFREWRITE` to compact the log to the current live string and list values.
The command is synchronous in this initial implementation: mutation commands
wait while Carrot streams the snapshot, syncs a same-directory temporary file,
and atomically renames it over the old AOF. Standard-server read handlers can continue; the reactor event
loop cannot process reads until the rewrite returns. In reactor mode, an
`always`-policy mutation also blocks the event loop while its per-command
`file.Sync` runs. The replacement is locked before rename so another Carrot
process cannot open it during the handoff. Mutations waiting for the rewrite
are appended to the new file after the rewrite completes. Rewriting
temporarily requires disk space for both the old log and the compacted
replacement. If snapshot writing fails before rename, the original AOF remains
in place; if syncing the directory
after rename fails, the replacement is active but later writes are rejected
until the server restarts and reopens the AOF.

Compaction reduces accumulated history but does not run automatically. There
is no backup automation, replication, checksum, or disk-failure protection.
Use `-aof-enabled=false` only when data loss on restart is acceptable.
Persistence is not a substitute for backups or a complete production-readiness
review.

## Implementation map and write/recovery flow

The implementation is split across a few focused files:

- `internal/aof/aof.go` owns the AOF lifecycle and format: `Open` locks and
  recovers the file; `Append` canonicalizes and writes one RESP command;
  `Rewrite` writes and syncs a compact snapshot before atomically replacing the
  old file; `rollback` removes a failed append and marks the log unhealthy if
  recovery of the append itself fails; `syncLoop` implements `everysec`;
  `Close` stops the sync loop, syncs, unlocks, and closes; `replay` streams
  records to reconstruct the store without buffering the entire AOF in memory.
- `internal/aof/lock_linux.go` uses Linux `flock` to prevent concurrent Carrot
  processes from using the same file. The `lock_other.go` counterpart rejects
  non-Linux use instead of silently proceeding without a lock.
- `internal/aof/sync_directory_linux.go` syncs the parent directory when a new
  AOF is created, so the filename entry is made durable as well as file data.
  `sync_directory_other.go` reports unsupported use on non-Linux systems.
- `internal/aof/aof_test.go` tests record replay, expiration deadlines,
  truncated tails, corrupt records, exclusive locking, sync policy, and failed
  appends.
- `internal/command/executor.go` is the shared write boundary used by both
  servers. It holds a mutex across append and in-memory execution, ensuring
  journal order and state-change order agree. `AOFREWRITE` holds the same
  mutex during compaction, so writes cannot slip between the snapshot and file
  replacement; queued writes continue in the replacement log afterward.
- `internal/storage/storage.go` provides `ForEachSnapshot`, which copies one
  shard's live values at a time and releases storage locks before encoding.
  This keeps rewrite from duplicating the entire dataset in an in-memory
  snapshot.
- `internal/server/server.go` and `internal/reactor/server.go` open/replay
  persistence before accepting commands and close it during shutdown.

For a client mutation, the flow is:

1. The server parses the RESP request into a command and calls the shared
   executor.
2. The executor recognizes mutations, takes its write mutex, and calls the AOF
   journal before changing storage.
3. AOF converts relative expirations to absolute deadlines and appends the
   RESP-encoded command. With `always`, it also syncs the file at this point.
4. The executor applies the returned canonical command to memory and releases
   the mutex. If append or required sync fails, storage is not mutated.
5. At startup, `Open` obtains the exclusive lock and `replay` applies complete
   records to a fresh store before the listener/event loop starts.

The same executor path is used during replay with no journal attached, which
prevents replayed commands from being appended again.

During `AOFREWRITE`, the executor's mutation lock creates a barrier: current
mutations finish first, then the store is streamed into a locked temporary
file. After that file has been flushed and synced, rename atomically makes it
the AOF at the configured path. The `Log` switches its append descriptor to
the replacement before releasing the old file lock. Mutations that arrived
while the barrier was held then append to the replacement, preventing lost
writes across compaction.
