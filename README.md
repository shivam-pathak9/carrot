<p align="center">
  <img src="assets/carrot_logo.png" alt="Carrot Retro Logo" width="280" />
</p>

<h1 align="center">🥕 Carrot</h1>

<p align="center">
  <b>Carrot is a lightweight Redis-inspired in-memory server written in Go.</b><br/>
  It provides a RESP-based command protocol, an in-memory key-value store, and two server implementations for different concurrency models.
</p>

---

## What this project includes

Carrot currently includes:

- A RESP parser and encoder for Redis-style wire protocol messages
- An in-memory storage layer with string values and TTL support
- Core commands: `PING`, `GET`, `SET`, `DEL`, `TTL`, and `EXPIRE`
- A per-connection goroutine server built with Go net listeners
- A Linux epoll-based reactor server for non-blocking I/O
- Scheduled background expiration cleanup for stale keys
- A test suite covering storage, config, command behavior, and RESP parsing

---

## Project structure

- `cmd/server` — goroutine-based TCP server
- `cmd/reactor-server` — epoll-based event loop server
- `internal/command` — command parsing and execution logic
- `internal/config` — configuration defaults
- `internal/protocol/resp` — RESP framing, encoder/decoder
- `internal/storage` — Map-based in-memory key store with TTL support
- `internal/reactor` — reactor implementation and architecture docs

---

## Supported behavior

The server currently supports the Redis-like operations needed for a basic in-memory database:

- `PING`
- `SET key value [EX seconds|PX milliseconds]`
- `GET key`
- `DEL key [key ...]`
- `TTL key`
- `EXPIRE key seconds`

The storage layer also supports passive expiration on access and a bounded active expiration sweep for background cleanup.

---

## How to run

Start the goroutine server:

```bash
go run ./cmd/server
```

Start the reactor server:

```bash
go run ./cmd/reactor-server
```

Run the test suite:

```bash
go test ./...
```

---

## Current status

This is a solid MVP / early production-quality foundation for an in-memory Redis-like service. It is not yet a full Redis replacement.

The project is currently strongest in:

- RESP compatibility for basic command flows
- in-memory key-value storage and TTL logic
- server architecture exploration and event-loop design
- basic correctness coverage through Go tests

The project is still missing or limited in:

- richer Redis data structures such as lists, hashes, sets, and sorted sets
- persistence (AOF/RDB)
- more production hardening like connection quotas and metrics
- formal benchmark data and tuning results

---

## Design notes

Carrot was built to explore two concurrency models:

1. A simple per-client goroutine server for clarity and ease of debugging
2. A Linux epoll reactor for lower-overhead, event-driven networking

The repo is intentionally built around a small, understandable codebase rather than a large Redis-compatible feature set.

---

## License

This project is distributed under the MIT license. See [LICENSE](LICENSE).
