<p align="center">
  <img src="assets/carrot_logo.png" alt="Carrot Retro Logo" width="280" />
</p>

<h1 align="center">🥕 Carrot</h1>

<p align="center">
  <b>A lightweight, high-performance, Redis-inspired server written in Go.</b><br/>
  Featuring standard <b>RESP protocol</b> parsing, zero external dependencies, and dual concurrency models (<b>Per-Client Goroutine</b> & <b>Linux Epoll Reactor</b>).
</p>

---

## Features

- **Dual Server Networking Architectures**:
  1. **Per-Client Goroutine Server** (`cmd/server`): Multi-threaded TCP connection handling powered by standard Go channels and goroutines.
  2. **Reactor Event Loop Server** (`cmd/reactor-server`): Single-threaded, asynchronous I/O multiplexing driven by Linux `epoll` system calls (`golang.org/x/sys/unix`).
- **Native RESP Protocol Implementation**: Hand-written, zero-dependency parser & encoder supporting RESP v2 types:
  - Simple Strings (`+`)
  - Errors (`-`)
  - Integers (`:`)
  - Bulk Strings (`$`)
  - Arrays (`*`)
- **Modular & Extensible Command Engine**: Decoupled RESP Decoder $\rightarrow$ Command Parser $\rightarrow$ Executor pipeline.
- **Redis CLI Compatibility**: Fully compatible with standard tools like `redis-cli`, `netcat`, or custom Redis client SDKs.

---
