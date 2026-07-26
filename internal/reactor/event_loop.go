//go:build linux

package reactor

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/shivampathak/carrot/internal/command"
	"golang.org/x/sys/unix"
)

// EventLoop is the central single-threaded engine driving the Reactor architecture.
// It continuously waits on epoll events via `poller.Wait()` and dispatches ready events to socket handlers.
//
// Struct Fields & Why they exist in EventLoop:
//   - poller    : Pointer to Poller (epoll wrapper). Monitors all active socket file descriptors for I/O events.
//   - listenerFD: Numeric file descriptor for server TCP listener socket (e.g. fd=3).
//                 When epoll signals EPOLLIN on listenerFD, it indicates a new client connection wants to connect.
//   - parser    : Pointer to command.Parser. Used when instantiating new Connection objects.
//   - executor  : Pointer to command.Executor. Used when instantiating new Connection objects.
//   - conns     : Registry map (`map[fd]*Connection`) mapping numeric socket file descriptors (e.g. 5, 6)
//                 to their corresponding Connection instance. Enables O(1) lookup when epoll triggers an event on an FD.
//   - stopChan  : Channel used to notify event loop goroutine to perform graceful shutdown.
//   - mu        : Mutex protecting access to `running` state boolean flag during start/stop calls.
//   - running   : Boolean status flag indicating whether event loop main thread is active.
type EventLoop struct {
	poller     *Poller           // Epoll poller handle
	listenerFD int               // Server listening socket File Descriptor (e.g. 3)
	parser     *command.Parser   // Shared command parser handle
	executor   *command.Executor // Shared command executor handle

	conns map[int]*Connection // O(1) lookup table mapping socket FD -> Connection struct

	stopChan chan struct{} // Signal channel for loop termination
	mu       sync.Mutex    // State mutex for thread safety
	running  bool          // Active execution flag
}

// NewEventLoop constructs a new EventLoop instance.
func NewEventLoop(poller *Poller, listenerFD int, parser *command.Parser, executor *command.Executor) *EventLoop {
	return &EventLoop{
		poller:     poller,
		listenerFD: listenerFD,
		parser:     parser,
		executor:   executor,
		conns:      make(map[int]*Connection),
		stopChan:   make(chan struct{}),
	}
}

// Run enters the main reactor event loop.
//
// Execution Flow:
//   1. Calls `poller.Wait()` which blocks in `epoll_wait` until kernel reports ready socket file descriptors.
//   2. Iterates over ready event slice (`events`).
//   3. Event Routing:
//      - If `ev.FD == listenerFD`: Calls `handleAccept()` to non-blockingly accept new incoming TCP clients.
//      - Else (`ev.FD` is a client socket): Calls `handleClientEvent(fd, events)` to handle read/write/close events.
func (el *EventLoop) Run() error {
	el.mu.Lock()
	el.running = true
	el.mu.Unlock()

	defer func() {
		el.mu.Lock()
		el.running = false
		el.mu.Unlock()
	}()

	log.Printf("Reactor Event Loop running...")

	for {
		// Non-blocking check for shutdown signal
		select {
		case <-el.stopChan:
			log.Println("Stopping reactor event loop...")
			el.cleanup()
			return nil
		default:
		}

		// Block until epoll_wait system call returns ready socket events
		events, err := el.poller.Wait()
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue // Signal interrupt (e.g. SIGINT); retry loop
			}
			return fmt.Errorf("event loop poller wait error: %w", err)
		}

		// Dispatch ready file descriptors
		for _, ev := range events {
			// Check if event occurred on server listener socket FD
			if ev.FD == el.listenerFD {
				el.handleAccept() // Incoming client connection ready to accept
			} else {
				el.handleClientEvent(ev.FD, ev.Events) // Existing client socket event (read/write/close)
			}
		}
	}
}

// handleAccept accepts incoming client connections from the listener socket non-blockingly.
//
// System Call: unix.Accept4(listenerFD, SOCK_NONBLOCK | SOCK_CLOEXEC)
//
// Why `Accept4` instead of standard `Accept`:
//   - `SOCK_NONBLOCK`: Ensures newly created client socket FD is immediately non-blocking.
//                      Avoids a second system call to set O_NONBLOCK via fcntl!
//   - `SOCK_CLOEXEC` : Prevents child processes from inheriting client socket FDs on exec.
//   - Non-blocking loop: Drains all pending client connections in backlog until `EAGAIN`/`EWOULDBLOCK`.
func (el *EventLoop) handleAccept() {
	for {
		// Perform non-blocking accept4 system call on listenerFD
		nfd, sa, err := unix.Accept4(el.listenerFD, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		if err != nil {
			// EAGAIN / EWOULDBLOCK means listening socket backlog is fully drained.
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				break // All pending incoming connections accepted
			}
			if errors.Is(err, unix.EINTR) {
				continue // Signal interrupted accept; retry
			}
			log.Printf("Accept error: %v", err)
			break
		}

		// Step 1: Create Connection object wrapping newly accepted client file descriptor (nfd)
		conn := NewConnection(nfd, el.poller, el.parser, el.executor)
		el.conns[nfd] = conn // Register in map registry: conns[nfd] = conn

		// Step 2: Register nfd with Linux epoll poller for read readiness (DefaultEventMask)
		if err := el.poller.Register(nfd); err != nil {
			log.Printf("Failed to register new client fd %d with epoll: %v", nfd, err)
			conn.Close()
			delete(el.conns, nfd)
			continue
		}

		addrStr := "unknown"
		if remote := conn.RemoteAddr(); remote != nil {
			addrStr = remote.String()
		}
		log.Printf("Reactor Client Connected: %s (fd=%d registered in epoll)", addrStr, nfd)
		_ = sa
	}
}

// handleClientEvent routes ready event flags for an existing client file descriptor (fd).
//
// Parameters:
//   - fd    : Client socket file descriptor.
//   - events: Bitmask of ready events returned by epoll_wait.
func (el *EventLoop) handleClientEvent(fd int, events uint32) {
	conn, ok := el.conns[fd]
	if !ok {
		return // Connection was already closed/removed
	}

	// 1. Check for socket errors or client disconnection signals
	//    - unix.EPOLLERR : Error condition happened on socket.
	//    - unix.EPOLLHUP : Hangup happened on socket (client TCP teardown).
	//    - unix.EPOLLRDHUP: Remote peer shut down write half of TCP connection (client EOF).
	if events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		el.closeClient(fd)
		return
	}

	// 2. Check Read Readiness (unix.EPOLLIN)
	//    Data is available in OS socket receive buffer. Read and process commands.
	if events&unix.EPOLLIN != 0 {
		if err := conn.OnRead(); err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("Client fd %d read error: %v", fd, err)
			} else {
				log.Printf("Client fd %d disconnected", fd)
			}
			el.closeClient(fd)
			return
		}
	}

	// 3. Check Write Readiness (unix.EPOLLOUT)
	//    OS socket send buffer has space to write queued response bytes.
	if events&unix.EPOLLOUT != 0 {
		if err := conn.OnWrite(); err != nil {
			log.Printf("Client fd %d write error: %v", fd, err)
			el.closeClient(fd)
			return
		}
	}
}

// closeClient unregisters client socket file descriptor (fd), closes its socket, and removes it from conns map.
func (el *EventLoop) closeClient(fd int) {
	if conn, ok := el.conns[fd]; ok {
		conn.Close()          // Unregisters from epoll and calls unix.Close(fd)
		delete(el.conns, fd)  // Removes from map registry
	}
}

// cleanup closes all active client connections and the epoll poller instance on server shutdown.
func (el *EventLoop) cleanup() {
	for fd, conn := range el.conns {
		conn.Close()
		delete(el.conns, fd)
	}
	el.poller.Close()
}

// Stop signals the event loop to stop and close all connections cleanly.
func (el *EventLoop) Stop() {
	el.mu.Lock()
	defer el.mu.Unlock()
	if el.running {
		close(el.stopChan)
	}
}
