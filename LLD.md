# Carrot implementation notes

This is a compact map of the implementation and its key synchronization
boundaries. The [HLD](./HLD.md) explains system-level behavior; package source
is authoritative when prose and code differ.

## Components

| Package | Responsibility |
|---|---|
| `internal/protocol/resp` | Bounded RESP decoding and encoding |
| `internal/command` | Parse commands, dispatch handlers, and sequence journaled mutations |
| `internal/storage` | Typed string/list values, expiration, sharded access, and rewrite snapshots |
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

The standard server handles each connection in a goroutine. The reactor uses
non-blocking sockets and epoll, with one event-loop path for reads, parsing,
command execution, and output scheduling. This avoids a goroutine per client
but serializes command execution on that loop. CPU profiles and network
benchmarks are diagnostic evidence, not capacity guarantees; see the
[benchmark notes](./README.md#local-benchmarks) and
[test guide](./docs/TEST_EXECUTION_GUIDE.md).

## Evidence and limits

`go test ./...`, `go test -race ./...`, and package coverage are the routine
correctness checks. The repository also has a bounded network benchmark for
both server modes and Redis, with AOF disabled to compare network/in-memory
paths. Those results do not measure durable write throughput, long-running
load, or power-loss behavior. See [TEST_SUMMARY.md](./docs/TEST_SUMMARY.md)
and [AOF_PERSISTENCE.md](./docs/AOF_PERSISTENCE.md) for test scope and
remaining limitations.
