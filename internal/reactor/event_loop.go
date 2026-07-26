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

// EventLoop drives the single-threaded reactor, waiting for I/O readiness on
// registered file descriptors and dispatching events to connections.
type EventLoop struct {
	poller     *Poller
	listenerFD int
	parser     *command.Parser
	executor   *command.Executor

	conns map[int]*Connection

	stopChan chan struct{}
	mu       sync.Mutex
	running  bool
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

// Run enters the main event loop, waiting on epoll events and processing connections.
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
		select {
		case <-el.stopChan:
			log.Println("Stopping reactor event loop...")
			el.cleanup()
			return nil
		default:
		}

		events, err := el.poller.Wait()
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("event loop poller wait error: %w", err)
		}

		for _, ev := range events {
			if ev.FD == el.listenerFD {
				el.handleAccept()
			} else {
				el.handleClientEvent(ev.FD, ev.Events)
			}
		}
	}
}

// handleAccept accepts a new non-blocking client connection from the listener socket.
func (el *EventLoop) handleAccept() {
	for {
		nfd, sa, err := unix.Accept4(el.listenerFD, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
				// No more pending incoming connections
				break
			}
			if errors.Is(err, unix.EINTR) {
				continue
			}
			log.Printf("Accept error: %v", err)
			break
		}

		conn := NewConnection(nfd, el.poller, el.parser, el.executor)
		el.conns[nfd] = conn

		if err := el.poller.Register(nfd); err != nil {
			log.Printf("Failed to register new client fd %d: %v", nfd, err)
			conn.Close()
			delete(el.conns, nfd)
			continue
		}

		addrStr := "unknown"
		if remote := conn.RemoteAddr(); remote != nil {
			addrStr = remote.String()
		}
		log.Printf("Reactor Client Connected: %s (fd=%d)", addrStr, nfd)
		_ = sa
	}
}

// handleClientEvent dispatches readable, writable, or close events for a client connection.
func (el *EventLoop) handleClientEvent(fd int, events uint32) {
	conn, ok := el.conns[fd]
	if !ok {
		return
	}

	// 1. Check for errors or disconnection signals
	if events&(unix.EPOLLERR|unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		el.closeClient(fd)
		return
	}

	// 2. Read readiness
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

	// 3. Write readiness
	if events&unix.EPOLLOUT != 0 {
		if err := conn.OnWrite(); err != nil {
			log.Printf("Client fd %d write error: %v", fd, err)
			el.closeClient(fd)
			return
		}
	}
}

// closeClient safely closes a client connection and removes it from the event loop registry.
func (el *EventLoop) closeClient(fd int) {
	if conn, ok := el.conns[fd]; ok {
		conn.Close()
		delete(el.conns, fd)
	}
}

// cleanup closes all remaining connections and the poller instance.
func (el *EventLoop) cleanup() {
	for fd, conn := range el.conns {
		conn.Close()
		delete(el.conns, fd)
	}
	el.poller.Close()
}

// Stop signals the event loop to stop and close all connections.
func (el *EventLoop) Stop() {
	el.mu.Lock()
	defer el.mu.Unlock()
	if el.running {
		close(el.stopChan)
	}
}
