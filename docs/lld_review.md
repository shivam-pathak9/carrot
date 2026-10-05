# 🥕 Carrot — Complete LLD Review

> Full Low-Level Design review of every package, file, and documentation artefact.
> Findings are rated **Critical**, **Major**, or **Minor** and grouped by component.
>
> This is a point-in-time review, not a live issue tracker. Some findings may
> have been addressed since it was written; check the current source and
> [project scope](./PROJECT_SCOPE.md) before treating a recommendation as open.

## Follow-up verification (2026-10-03)

The review prompted a source check and these findings are now addressed:

- `Store.Set` explicitly initializes `StringKind`; `TTL` and `ExpireAt` use a
  single captured time value.
- Failed AOF append rollback resets the buffered writer after truncating and
  seeking. AOF replay and mutation ordering share `command.IsMutation`;
  canonicalization falls through only for known mutations with no relative
  expiration semantics, covered by a test over every current mutation family.
- `PING` has a handler in `ping.go`; storage errors use the generic
  `storageError` mapper; test-only storage helpers are in `export_test.go`.
- Added expiry edge-case tests, client package tests, executor/list
  microbenchmarks, and `CONTRIBUTING.md`.
- README feature/structure details and verified coverage/test counts were
  refreshed. See [TEST_SUMMARY.md](./TEST_SUMMARY.md) for current results.
- The `net.Error.Temporary` deprecation remains a low-priority follow-up:
  replacing it needs a portable retry classification that preserves the
  current accept-loop recovery behavior.

Two repository-state findings in the original review were not applicable:
`server.exe` is ignored by the existing `.gitignore` and is not tracked by
Git; the `internal/logger` and `test` directories contain no tracked files, so
they are not empty packages in the repository. The benchmark comparison and
performance claims in the original review are historical; use the current
conditions and results in the [README](../README.md) instead.

---

## Table of Contents

1. [Repository Structure & Layering](#1-repository-structure--layering)
2. [Protocol Layer (`internal/protocol/resp`)](#2-protocol-layer)
3. [Storage Layer (`internal/storage`)](#3-storage-layer)
4. [Command Layer (`internal/command`)](#4-command-layer)
5. [Client Wrapper (`internal/client`)](#5-client-wrapper)
6. [Goroutine Server (`internal/server`)](#6-goroutine-server)
7. [Reactor Server (`internal/reactor`)](#7-reactor-server)
8. [AOF Persistence (`internal/aof`)](#8-aof-persistence)
9. [Configuration (`internal/config`)](#9-configuration)
10. [Entry Points (`cmd/`)](#10-entry-points)
11. [Documentation & README](#11-documentation--readme)
12. [Cross-Cutting Concerns](#12-cross-cutting-concerns)
13. [Summary Improvement Matrix](#13-summary-improvement-matrix)

---

## 1. Repository Structure & Layering

### Current Layout

```
carrot/
├── cmd/
│   ├── server/           # Goroutine server main
│   ├── reactor-server/   # Epoll server main
│   └── scale-server/     # Scale demo (misleading name)
├── internal/
│   ├── aof/              # Append-only-file persistence
│   ├── client/           # RESP client wrapper
│   ├── command/          # Command parsing + execution
│   ├── config/           # Configuration
│   ├── logger/           # EMPTY package
│   ├── protocol/resp/    # RESP encoder/decoder
│   ├── reactor/          # Epoll reactor
│   ├── server/           # Goroutine server
│   └── storage/          # In-memory store
├── docs/                 # Design documentation
├── assets/               # Logo + diagrams
├── scripts/              # EMPTY directory
└── test/                 # EMPTY directory
```

### Findings

| # | Severity | Finding | Recommendation |
|---|----------|---------|----------------|
| 1.1 | **Minor** | `internal/logger/` is an empty package with no files. | Remove it, or add a structured logger implementation. An empty package is confusing for readers. |
| 1.2 | **Minor** | `scripts/` and `test/` directories are empty. | Remove them from the repo (add to `.gitignore`), or add a README explaining planned use. Empty directories signal abandoned work. |
| 1.3 | **Minor** | `cmd/scale-server/main.go` header says `// scale_test.go` and the run instruction says `go run ./cmd/scale-demo`. Neither matches the directory name `scale-server`. | Rename to `cmd/scale-demo/` and fix the file comment. This is a demo tool, not a server. |
| 1.4 | **Minor** | `server.exe` (a Windows binary) is checked into the repo root. | Add `*.exe` to `.gitignore` and remove it from version control. Binaries should never be committed. |
| 1.5 | **Major** | The dependency graph has clean downward flow: `cmd → server/reactor → command → storage/resp`. However, **`command` imports `storage` directly** (concrete type, not interface). This tight coupling makes it impossible to test command handlers against a mock store or swap storage implementations. | Introduce a `storage.Engine` interface that `command.Executor` depends on. The concrete `Store` implements it. This is the single biggest architectural improvement for testability. |

---

## 2. Protocol Layer

**Files:** [`types.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/types.go), [`decoder.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/decoder.go), [`encode.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/encode.go), [`constructors.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/constructors.go), [`utils.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/utils.go), [`encoder_utils.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/encoder_utils.go), [`bulk_string.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/bulk_string.go), [`arrays.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/arrays.go), [`simple_string.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/simple_string.go), [`error.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/error.go), [`integer.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/integer.go)

### What works well ✅

- Type constants map directly to RESP prefix bytes — elegant and self-documenting.
- Message-size budget tracking across an entire RESP frame (the `consume()` pattern) is a solid defense against malicious input.
- Inline command support (for `redis-benchmark`) is properly handled.
- Nesting depth limit (`maxRESPDepth = 64`) prevents stack overflow attacks.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 2.1 | **Major** | The `Value` struct uses a **flat union** with `String`, `Integer`, `Array` and `IsNull` all living as public fields. A reader cannot tell at a glance which fields are valid for which `Type`. For a "human readable" project this is the biggest readability gap in the RESP layer. | [`types.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/types.go#L33-L47) | Add a doc-table to the `Value` struct doc-comment: "For `SimpleString`/`BulkString`/`Error` → use `String`. For `Integer` → use `Integer`. For `Array` → use `Array`. `IsNull` is valid only when `Type == BulkString`." |
| 2.2 | **Minor** | `bulk_string.go` has a **double block-comment** at the top of `decodeBulkString()` — one says "Read length" and then a longer version restates the same thing. | [`bulk_string.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/bulk_string.go#L9-L38) | Remove the redundant short comment on line 15. Keep only the detailed block comment. |
| 2.3 | **Minor** | `utils.go` line 66 has a typo: `// CLRF == /r/n` — it should be `CRLF` and `\r\n`. | [`utils.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/utils.go#L66) | Fix: `// CRLF == \r\n` |
| 2.4 | **Minor** | Constructor functions in `constructors.go` lack doc-comments. For a "human readable" project these are the most frequently-used functions. | [`constructors.go`](file:///home/shivam/workspace/carrot1/carrot/internal/protocol/resp/constructors.go) | Add one-line godoc to each: `NewSimpleString`, `NewError`, `NewInteger`, `NewBulkString`, `NewNullBulkString`, `NewArray`. |
| 2.5 | **Minor** | The file split between `encoder_utils.go` and `encode.go` is arbitrary — `encode.go` has the dispatch, `encoder_utils.go` has the actual encoding. A reader naturally looks for "encode" to find encoding logic. | resp package | Consider merging `encoder_utils.go` into `encode.go`, or rename `encoder_utils.go` → `encode_types.go` to signal it contains per-type encoding. |

---

## 3. Storage Layer

**Files:** [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go), [`list.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/list.go)

### What works well ✅

- The sharding design comment ([`storage.go` L42–58](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L42-L58)) is excellent — it explains the *why* (lock contention), the *how* (FNV-1a → 256 shards), and the *benefit* (parallel ops).
- `listValue` as a circular buffer with O(1) push/pop is a correct and non-trivial data structure — well done.
- `ListMove` deadlock prevention via sorted shard index ordering is correct and clearly commented.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 3.1 | **Critical** | `TTL()` calls `time.Now()` **twice** — once at line 158 and again implicitly via `time.Until()` at line 169. Between these two calls, time moves forward, which can cause a race: `IsExpired(now)` returns false but `time.Until()` returns negative. The second `delete` at line 171-173 duplicates the work of `IsExpired`. | [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L148-L176) | Use a single `now := time.Now()` and compute remaining as `obj.ExpiresAt.Sub(now).Seconds()`. Remove the redundant negative check. |
| 3.2 | **Major** | `ExpireAt` also calls `time.Now()` **twice** (line 227 and line 233). The second call should reuse the `now` from line 226. | [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L216-L241) | Change line 233: `if !now.Before(expiresAt)` (reuse the existing `now` variable). |
| 3.3 | **Major** | `listValue` **does not export any methods** and has no doc-comments on `push`, `pop`, `insert`, `removeAt`, `rangeCopy`, `replace`, `index`, `at`, `set`, `grow`. For a project targeting human readability, this is a significant gap — the circular buffer logic is non-trivial. | [`list.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/list.go#L15-L138) | Add a block comment at the top of `listValue` explaining the ring-buffer invariant: `buffer[head..head+size-1 mod cap]` are the live elements. Add one-line comments on each method. |
| 3.4 | **Minor** | `getRawObj` and `setRawObj` are test helpers but live in production code with lowercase names. | [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L319-L334) | Move them to a `storage_test.go` `export_test.go` file, or use the `_test.go` convention with an internal test package. |
| 3.5 | **Minor** | `liveObjectLocked` is the central passive-expiry check but is only documented with its function name. | [`list.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/list.go#L140-L147) | Add a doc-comment: `// liveObjectLocked returns obj if key exists and is not expired, performing passive deletion if necessary. Caller must hold sh.mu.` |
| 3.6 | **Minor** | `ActiveExpireCycle` says `thresholdRatio = 0.25` and the comment says "5 / 20" but this is only correct for `sampleSize = 20`. If `sampleSize` ever changes the ratio comment becomes misleading. | [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L250) | Change comment to: `// Stop when ≤25% of the sample was expired` |
| 3.7 | **Major** | `Set()` does not set `Kind: StringKind` on the created `Obj`. This means a key created by `Set()` has `Kind == 0 == StringKind` which works *by accident* because `StringKind` is iota 0. This is fragile — if `ValueKind` constants are reordered, `Set()` silently creates wrong-typed objects. | [`storage.go`](file:///home/shivam/workspace/carrot1/carrot/internal/storage/storage.go#L109-L112) | Explicitly set `Kind: StringKind` in the `Obj` literal inside `Set()`. |

---

## 4. Command Layer

**Files:** [`command.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/command.go), [`executor.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/executor.go), [`parser.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/parser.go), [`set.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/set.go), [`get.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/get.go), [`del.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/del.go), [`ping.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/ping.go), [`ttl.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/ttl.go), [`expire.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/expire.go), [`list.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/list.go)

### What works well ✅

- Each command in its own file — very clean, easy to navigate.
- The `Command` struct with the example block comment is excellent for readability.
- `handleExpireAt` is correctly missing its doc-comment prefix but the logic is sound.
- The `isMutation()` function cleanly separates read vs write commands for the journal boundary.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 4.1 | **Major** | `ping.go` is a **completely empty file** — it contains only a comment saying "handlers/definitions for the PING command" but the actual PING handler is inlined inside `executor.go` `execute()`. This is misleading. | [`ping.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/ping.go) | Either move the PING case from `execute()` into a `handlePing()` function in `ping.go` (consistent with GET, SET, DEL, etc.), or delete `ping.go` entirely. |
| 4.2 | **Major** | `parser.go` has **redundant comments**: both the struct-level `NewParser` and the inside of `Parse` have narrative comments that restate the code. For example, `NewParser` says "NewParser returns a new command parser. Parser has no state for now..." — this duplicates the godoc above. | [`parser.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/parser.go#L14-L18) | Remove the inner comment from `NewParser` (keep the godoc). Inside `Parse`, keep the code-level comments but remove the opening block paragraph that restates the godoc. |
| 4.3 | **Minor** | `handleExpireAt` is missing its godoc comment (unlike every other handler function). | [`expire.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/expire.go#L43) | Add: `// handleExpireAt executes the PEXPIREAT command. Syntax: PEXPIREAT key unix-milliseconds` |
| 4.4 | **Minor** | `get.go` calls `listError(err)` for a WRONGTYPE error on a GET command. The function name `listError` is misleading — it handles all storage errors, not just list errors. | [`get.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/get.go#L24) | Rename `listError` → `storageError` or `toRESPError`. |
| 4.5 | **Major** | The executor's `writeMu` serializes **all** mutations globally. This means two `SET` commands on completely different keys (different shards) still block each other. The storage layer has 256 fine-grained shard locks, but the executor's single mutex nullifies that parallelism. | [`executor.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/executor.go#L48-L49) | This is a conscious trade-off for journal ordering correctness (the comment explains it). Document it more prominently: "**Design trade-off**: all mutations are serialized under `writeMu` to guarantee journal + in-memory ordering. This means shard-level parallelism only benefits read operations." |
| 4.6 | **Minor** | `handleListCommand` is a 170-line switch statement. Each case is 5–15 lines. | [`list.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/list.go#L36-L175) | Consider extracting `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`, `LREM`, `LINSERT`, `LMOVE`, `RPOPLPUSH` into named helper functions (like `handleListPush` and `handleListPop` already are). This would make `handleListCommand` a pure dispatch table. |

---

## 5. Client Wrapper

**File:** [`client.go`](file:///home/shivam/workspace/carrot1/carrot/internal/client/client.go)

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 5.1 | **Minor** | There are **two separate doc comments** describing the same struct fields. Lines 12-14 and lines 48-55 both say "conn, reader, writer" with field descriptions. | [`client.go`](file:///home/shivam/workspace/carrot1/carrot/internal/client/client.go#L12-L14) | Remove the first orphaned comment block (lines 12-14). Keep the struct-level godoc. |
| 5.2 | **Minor** | `Write()` has a long inner comment (lines 68-77) explaining why it flushes. This is good context, but the 10-line comment dwarfs the 3-line function body. | [`client.go`](file:///home/shivam/workspace/carrot1/carrot/internal/client/client.go#L62-L83) | Condense to 2-3 lines: `// Flush ensures the response reaches the client immediately. Higher-level code may batch writes and flush once.` |
| 5.3 | **Minor** | Missing godoc on `Read`, `Close`, `Flush`, `SetReadDeadline`, `SetWriteDeadline`, `RemoteAddr`, `Encoder`, `Decoder`. The inner block comments duplicate what a godoc would say. | [`client.go`](file:///home/shivam/workspace/carrot1/carrot/internal/client/client.go) | Convert each inner comment to a proper godoc above the function. |

---

## 6. Goroutine Server

**File:** [`server.go`](file:///home/shivam/workspace/carrot1/carrot/internal/server/server.go)

### What works well ✅

- `Shutdown()` with context-based grace period is well-implemented.
- `limitedWriter` prevents oversized responses from being partially sent.
- `openPersistence` / `closePersistence` are clean and idempotent.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 6.1 | **Major** | `Serve()` calls `openPersistence()` again even though `Start()` already called it. The idempotency guard (`s.aof != nil`) prevents double-open, but the flow is confusing — a reader wonders why persistence is opened twice. | [`server.go`](file:///home/shivam/workspace/carrot1/carrot/internal/server/server.go#L124) | Add a comment: `// Idempotent — no-op if Start() already opened persistence. Needed when Serve() is called directly without Start().` |
| 6.2 | **Minor** | The `temporary` variable name on line 159 is misleading — the method `.Temporary()` is deprecated in Go. | [`server.go`](file:///home/shivam/workspace/carrot1/carrot/internal/server/server.go#L159) | Use a clearer pattern or remove the temporary-error retry since Go deprecated `net.Error.Temporary()`. |
| 6.3 | **Minor** | The `Server` struct has 14 fields but no field-level comments. | [`server.go`](file:///home/shivam/workspace/carrot1/carrot/internal/server/server.go#L26-L45) | Add brief comments to non-obvious fields like `closingand`, `shutdownRequested`, `stopChan`, `stopOnce`, `serveDone`. |

---

## 7. Reactor Server

**Files:** [`server.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/server.go), [`event_loop.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/event_loop.go), [`poller.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/poller.go), [`connection.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/connection.go)

### What works well ✅

- The ARCH.md is **outstanding** — ASCII art, mermaid diagrams, step-by-step walkthroughs, and error handling matrix. This is the strongest documentation in the repo.
- Deadlock-free shard locking, signal handling with `EINTR`, and the EAGAIN/EWOULDBLOCK distinction are all correctly handled.
- `processCommands()` correctly accounts for `bufReader.Buffered()` — a subtle and critical correctness point.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 7.1 | **Major** | `OnRead()` calls `processCommands()` **twice**: once inside the read loop (line 122) and once after the loop exits (line 148). If data was fully processed inside the loop, the second call is wasteful. If the read loop broke on EAGAIN before calling `processCommands`, the post-loop call is necessary. The current code does both, which means each command may be attempted for parsing twice. | [`connection.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/connection.go#L103-L149) | Remove the inner `processCommands()` call (line 122-124) and rely only on the post-loop call. This simplifies the flow: drain socket first, then process all accumulated data once. |
| 7.2 | **Minor** | `poller.Wait()` allocates a fresh `[]unix.EpollEvent` on **every call**. In a hot loop running at 10 Hz+, this creates GC pressure. | [`poller.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/poller.go#L158) | Pre-allocate the events slice in the `Poller` struct and reuse it across calls. |
| 7.3 | **Minor** | `handleAccept` line 208 has `_ = sa` — the sockaddr from `Accept4` is read and discarded. | [`event_loop.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/event_loop.go#L208) | Remove `_ = sa`. If the sockaddr is not needed, don't assign it. |
| 7.4 | **Minor** | ARCH.md component matrix (line 468–471) has **Windows file paths** (`c:/Users/acer/Desktop/carrot-window/...`) in the file links. | [`ARCH.md`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/ARCH.md#L466-L471) | Update to relative paths. |
| 7.5 | **Minor** | README.md (reactor) references a PNG `<Start Decision Options Flow-2026-07-26-082211.png>` with angle brackets — this won't render and the image is not in the directory. | [`README.md`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/README.md#L55-L56) | Either add the referenced images or remove the broken image tags. |
| 7.6 | **Minor** | `OnRead` allocates a fresh `buf := make([]byte, 4096)` on every call. | [`connection.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/connection.go#L112) | Make it a field of `Connection` to avoid per-call allocation. |
| 7.7 | **Minor** | `processCommands()` creates a new `bufio.Reader`, `bytes.NewReader`, and `resp.Decoder` on every iteration of the parse loop. | [`connection.go`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/connection.go#L157-L161) | This is necessary because the decoder must start from the current buffer position each time. Add a comment explaining why these cannot be reused. |

---

## 8. AOF Persistence

**File:** [`aof.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/aof.go), [`lock_linux.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/lock_linux.go), [`lock_other.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/lock_other.go), [`sync_directory_linux.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/sync_directory_linux.go), [`sync_directory_other.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/sync_directory_other.go)

### What works well ✅

- `canonicalCommand` converting relative TTLs to absolute `PXAT` deadlines is a critical correctness detail done right.
- The `rollback()` mechanism with progressive failure tracking (`l.failed`) is well thought out.
- The `countingReader` for tracking exact file offsets during replay is elegant.
- Platform guard files (`lock_other.go`, `sync_directory_other.go`) fail closed with clear error messages — excellent safety.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 8.1 | **Major** | `canonicalCommand` line 360-363 has an empty `default:` case that silently passes through unknown mutation commands **without canonicalization**. This means if a new mutation command (e.g., `HSET`) is added to `isMutation()` but not to `canonicalCommand`, it gets journaled without any normalization — which may be fine, but the silent pass-through is a maintenance trap. | [`aof.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/aof.go#L362-L363) | Add a comment: `// Other mutations (LPUSH, RPUSH, etc.) require no normalization — their arguments are already absolute.` |
| 8.2 | **Major** | `isPersistedMutation` and `isMutation` in `executor.go` are **duplicate lists** that must stay synchronized. If one is updated and the other isn't, commands will either be silently not journaled or rejected during replay. | [`aof.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/aof.go#L295-L304) vs [`executor.go`](file:///home/shivam/workspace/carrot1/carrot/internal/command/executor.go#L109-L118) | Extract a shared constant or function: `command.IsMutation()` that both the executor and AOF replay reference. This eliminates the synchronization risk. |
| 8.3 | **Minor** | `replay()` creates a new `Executor` and `Parser` just for replay, but the store already has an executor created in `Server`. | [`aof.go`](file:///home/shivam/workspace/carrot1/carrot/internal/aof/aof.go#L252-L253) | This is intentional (replay executor has no journal), but add a comment: `// Replay uses a journal-free executor so replayed commands are not re-appended.` |

---

## 9. Configuration

**File:** [`config.go`](file:///home/shivam/workspace/carrot1/carrot/internal/config/config.go)

### What works well ✅

- `Validate()` is thorough — checks host, port, limits, timeouts, and AOF policy.
- `WithDefaults()` correctly preserves explicitly-set values.
- `IsWildcardHost()` and `validHostname()` are cleanly separated utilities.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 9.1 | **Minor** | `WithDefaults()` uses zero-value checks (`== 0`) for integers and durations, which means you can't intentionally set `MaxConnections = 0` (it would be replaced by the default). This is fine for the current use case but isn't documented. | [`config.go`](file:///home/shivam/workspace/carrot1/carrot/internal/config/config.go#L44-L68) | Add a comment: `// Zero-valued fields inherit defaults; explicitly set non-zero values are preserved.` |
| 9.2 | **Minor** | `validHostname` doesn't handle underscores (which are technically invalid in DNS but widely used in practice, e.g., `my_host`). Not a bug, but worth documenting. | [`config.go`](file:///home/shivam/workspace/carrot1/carrot/internal/config/config.go#L116-L131) | Add a comment: `// RFC 952/1123 compliant; underscores are rejected.` |

---

## 10. Entry Points

**Files:** [`cmd/server/main.go`](file:///home/shivam/workspace/carrot1/carrot/cmd/server/main.go), [`cmd/reactor-server/main.go`](file:///home/shivam/workspace/carrot1/carrot/cmd/reactor-server/main.go), [`cmd/scale-server/main.go`](file:///home/shivam/workspace/carrot1/carrot/cmd/scale-server/main.go)

### What works well ✅

- Both server entrypoints are nearly identical, following the same clean pattern: parse flags → validate → start server → wait for signal → shutdown.
- The wildcard host warning is a nice UX touch.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 10.1 | **Major** | Flag parsing is duplicated **identically** between `cmd/server/main.go` and `cmd/reactor-server/main.go` (11 flag definitions each). | Both `main.go` files | Extract a `config.ParseFlags() Config` function in the `config` package. Both entrypoints call it. |
| 10.2 | **Minor** | The reactor entrypoint has `//go:build linux` but the goroutine server does not. On non-Linux systems, `go run ./cmd/server` starts the goroutine server with AOF enabled, which will fail because AOF requires Linux. The error message is clear, but a user might expect `cmd/server` to work cross-platform. | [`cmd/server/main.go`](file:///home/shivam/workspace/carrot1/carrot/cmd/server/main.go) | Add a note in the `--help` output or README: "AOF is Linux-only. Use `-aof-enabled=false` on other platforms." |

---

## 11. Documentation & README

**Files:** [`README.md`](file:///home/shivam/workspace/carrot1/carrot/README.md), [`docs/PROJECT_SCOPE.md`](file:///home/shivam/workspace/carrot1/carrot/docs/PROJECT_SCOPE.md), [`docs/AOF_PERSISTENCE.md`](file:///home/shivam/workspace/carrot1/carrot/docs/AOF_PERSISTENCE.md), [`internal/reactor/ARCH.md`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/ARCH.md), [`internal/reactor/README.md`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/README.md)

### What works well ✅

- The root README is comprehensive — feature list, project structure, how to run, benchmark data, architecture comparison, and honest gap disclosure.
- AOF_PERSISTENCE.md is a model doc — it covers durability contract, format, recovery, limitations, and implementation map.
- ARCH.md is exceptionally detailed with ASCII art, mermaid diagrams, and error handling matrix.

### Findings

| # | Severity | Finding | File | Recommendation |
|---|----------|---------|------|----------------|
| 11.1 | **Major** | The README's "Project structure" section is **incomplete** — it omits `internal/aof`, `internal/client`, `internal/logger`, `internal/server`, `cmd/reactor-server`, and `cmd/scale-server`. | [`README.md`](file:///home/shivam/workspace/carrot1/carrot/README.md#L29-L38) | Add all packages with one-line descriptions. |
| 11.2 | **Minor** | The reactor README references image files that don't exist in the directory: `Start Decision Options Flow-2026-07-26-082211.png` and `Start Decision Options Flow-2026-07-26-081942.png`. | [`internal/reactor/README.md`](file:///home/shivam/workspace/carrot1/carrot/internal/reactor/README.md#L55-L100) | Add the images or remove the broken references. |
| 11.3 | **Minor** | The reactor README says IPv4 & IPv6 support but the root README says "IPv4 addresses only". These contradict. | [`README.md`](file:///home/shivam/workspace/carrot1/carrot/README.md#L122-L123) vs [`README.md`](file:///home/shivam/workspace/carrot1/carrot/README.md#L239) | Clarify: the code now supports dual-stack (IPv4+IPv6). Update the root README to match. |
| 11.4 | **Minor** | No `CONTRIBUTING.md` or code style guide. For a "human readable" project, this would help new readers understand naming conventions and comment style. | — | Add a brief `CONTRIBUTING.md` with comment conventions, test expectations, and PR process. |

---

## 12. Cross-Cutting Concerns

| # | Severity | Finding | Recommendation |
|---|----------|---------|----------------|
| 12.1 | **Major** | **No structured logging.** The project uses `log.Printf` throughout. Log lines have no levels (INFO, WARN, ERROR), no structured fields, and are inconsistently formatted. Example: `"[Active Expire] Cleaned %d..."` vs `"Reactor Client Connected: %s..."`. | Adopt `log/slog` (stdlib since Go 1.21) or at minimum define level prefixes consistently. The empty `internal/logger/` package suggests this was planned. |
| 12.2 | **Major** | **Inconsistent comment style.** Some functions have godoc-style comments above them, others have block comments *inside* the function body that restate the godoc, and some have both. The `client.go` file is the worst offender with every method having an internal block comment but no godoc. | Establish a rule: Godoc comment above the function (what it does + parameters). Internal comments only for non-obvious *how* details. Never duplicate. |
| 12.3 | **Minor** | **No Go interfaces** for the major abstractions (Store, Journal, Encoder/Decoder). This makes the codebase harder to mock and test in isolation. | Define interfaces at the consumer side: `command.Store` (what the executor needs), `command.Journal` (already exists — good!). |
| 12.4 | **Minor** | **No `Makefile` or `justfile`.** The README lists 5 separate `go` commands to run the full check suite. | Add a `Makefile` with targets: `build`, `test`, `test-race`, `lint`, `vet`, `cover`. |

---

## 13. Summary Improvement Matrix

### Priority Tier 1 — Critical (Correctness)

| ID | Component | Issue | Impact |
|----|-----------|-------|--------|
| 3.1 | Storage | `TTL()` double `time.Now()` race | Returns wrong TTL or fails to delete expired keys |
| 3.7 | Storage | `Set()` missing `Kind: StringKind` | Works by accident; breaks if enum reordered |

### Priority Tier 2 — Major (Robustness & Readability)

| ID | Component | Issue |
|----|-----------|-------|
| 1.5 | Architecture | `command` depends on concrete `*storage.Store` (no interface) |
| 3.2 | Storage | `ExpireAt()` double `time.Now()` |
| 3.3 | Storage | `listValue` has zero documentation on its ring-buffer internals |
| 4.1 | Command | `ping.go` is empty — handler is in executor |
| 4.5 | Command | `writeMu` serializes all mutations — needs prominent documentation |
| 6.1 | Server | `Serve()` redundant `openPersistence()` call is confusing |
| 7.1 | Reactor | `OnRead()` calls `processCommands()` twice |
| 8.2 | AOF | `isPersistedMutation` and `isMutation` are duplicate lists |
| 10.1 | Entry Point | Flag parsing duplicated across both `main.go` files |
| 11.1 | Docs | README project structure is incomplete |
| 12.1 | Cross-cutting | No structured logging |
| 12.2 | Cross-cutting | Inconsistent comment style |

### Priority Tier 3 — Minor (Polish)

| ID | Component | Issue |
|----|-----------|-------|
| 1.1-1.4 | Structure | Empty dirs, checked-in binary, misnamed scale-server |
| 2.2-2.5 | RESP | Duplicate comments, typo, missing godocs, file naming |
| 3.4-3.6 | Storage | Test helpers in prod code, missing docs |
| 4.2-4.4, 4.6 | Command | Redundant parser comments, missing godoc, misleading function name |
| 5.1-5.3 | Client | Duplicate comments, missing godocs |
| 6.2-6.3 | Server | Deprecated API, missing struct comments |
| 7.2-7.7 | Reactor | Allocations, dead code, broken image refs |
| 8.1, 8.3 | AOF | Silent pass-through in canonicalCommand, replay executor comment |
| 9.1-9.2 | Config | Zero-value semantics, hostname validation |
| 11.2-11.4 | Docs | Broken images, contradictory IPv4/IPv6 claims, no CONTRIBUTING.md |
| 12.3-12.4 | Cross-cutting | No interfaces, no Makefile |

---

> [!TIP]
> **Quick wins for maximum readability improvement:**
> 1. Fix the two `time.Now()` bugs (3.1, 3.2) — correctness first
> 2. Add `Kind: StringKind` to `Set()` (3.7) — 1-line fix, prevents future breakage
> 3. Document `listValue` ring-buffer invariants (3.3) — biggest clarity gap
> 4. Consolidate `isMutation` / `isPersistedMutation` (8.2) — prevents future desync
> 5. Add godocs to all exported functions in `client.go` and `constructors.go` — fast, high-impact
