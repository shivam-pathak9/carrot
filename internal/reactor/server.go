//go:build linux

// Package reactor implements the Linux epoll-based non-blocking server.
package reactor

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/storage"
	"golang.org/x/sys/unix"
)

// Server implements a TCP server driven by the epoll Reactor EventLoop.
//
// Struct Fields & Why they exist:
//   - config    : Server configuration holding network IP host (e.g. "0.0.0.0") and TCP port (e.g. "6379").
//   - listenerFD: File descriptor integer for listening TCP socket created via `unix.Socket`.
//   - poller    : Pointer to Poller wrapping Linux epoll instance.
//   - eventLoop : Pointer to EventLoop engine managing socket readiness events and event dispatching.
//   - parser    : Pointer to command.Parser converting RESP values into structured Commands.
//   - executor  : Pointer to command.Executor implementing server behavior for commands (PING, GET, SET, TTL, etc.).
type Server struct {
	config     config.Config
	listenerFD int // OS File Descriptor for server listening socket (e.g. fd=3)

	poller    *Poller    // Linux epoll wrapper instance
	eventLoop *EventLoop // Main event multiplexing loop engine
	stopChan  chan struct{}
	stopOnce  sync.Once
	mu        sync.RWMutex
	addr      net.Addr

	store    *storage.Store    // Shared storage engine handle
	parser   *command.Parser   // Shared command parser
	executor *command.Executor // Shared command executor
}

// NewServer creates a reactor server with fresh in-memory storage and command handlers.
func NewServer(cfg config.Config) *Server {
	cfg = cfg.WithDefaults()
	store := storage.NewStore()
	return &Server{
		config:     cfg,
		listenerFD: -1,
		store:      store,
		parser:     command.NewParser(),
		executor:   command.NewExecutor(store),
		stopChan:   make(chan struct{}),
	}
}

// Addr returns the listener address after Start has initialized the socket.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// Start initializes the non-blocking listening socket via system calls, registers it with epoll, and runs the EventLoop.
//
// Step-by-Step System Call Setup:
//  1. unix.Socket      : Creates IPv4, non-blocking TCP socket file descriptor.
//  2. unix.SetsockoptInt: Configures SO_REUSEADDR socket option.
//  3. unix.Bind        : Binds listener socket FD to target IP address and TCP port.
//  4. unix.Listen      : Marks socket FD as passive listener with connection backlog queue of 128.
//  5. NewPoller        : Creates epoll instance via unix.EpollCreate1.
//  6. poller.Register  : Registers listener socket FD in epoll interest list for EPOLLIN (incoming connections).
//  7. eventLoop.Run    : Starts infinite epoll_wait event loop with Active Expiration cycle.
func (s *Server) Start() error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid reactor configuration: %w", err)
	}
	// Parse port string configuration to integer
	portInt, err := strconv.Atoi(s.config.Port)
	if err != nil {
		return fmt.Errorf("invalid port %q: %w", s.config.Port, err)
	}

	// Step 1: Create non-blocking listening TCP socket via low-level system call
	// Flags breakdown:
	//   - unix.AF_INET       : IPv4 protocol family.
	//   - unix.SOCK_STREAM   : Full-duplex byte stream TCP protocol.
	//   - unix.SOCK_NONBLOCK : Sets non-blocking I/O mode on created socket descriptor.
	//   - unix.SOCK_CLOEXEC  : Sets Close-on-Exec flag so child processes do not inherit listening socket.
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}
	s.listenerFD = fd // Store listener socket file descriptor (e.g. fd=3)

	// Step 2: Set SO_REUSEADDR socket option
	// Why SO_REUSEADDR:
	//   Allows immediate rebinding to host:port even if socket is currently in TCP TIME_WAIT state after server restart.
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	// Step 3: Bind listener to the configured IPv4 address.
	tcpAddr, err := net.ResolveTCPAddr("tcp4", net.JoinHostPort(s.config.Host, strconv.Itoa(portInt)))
	if err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("invalid IPv4 listen address: %w", err)
	}
	ip := tcpAddr.IP
	var addr [4]byte
	if ip4 := ip.To4(); ip4 != nil {
		copy(addr[:], ip4)
	} else {
		_ = unix.Close(fd)
		return fmt.Errorf("reactor server requires an IPv4 host")
	}

	sa := &unix.SockaddrInet4{
		Port: portInt,
		Addr: addr,
	}

	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to bind socket to %s:%s: %w", s.config.Host, s.config.Port, err)
	}

	// Step 4: Listen on socket FD with connection backlog limit of 128
	// Marks the socket FD as a passive listening socket for accepting incoming client TCP connections.
	if err := unix.Listen(fd, 128); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to listen on socket: %w", err)
	}
	sockaddr, err := unix.Getsockname(fd)
	if err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("get listener address: %w", err)
	}
	if addr4, ok := sockaddr.(*unix.SockaddrInet4); ok {
		s.mu.Lock()
		s.addr = &net.TCPAddr{IP: net.IP(addr4.Addr[:]), Port: addr4.Port}
		s.mu.Unlock()
	}

	// Step 5: Initialize epoll Poller instance (epoll_create1 system call)
	poller, err := NewPoller(DefaultMaxEvents)
	if err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to create reactor poller: %w", err)
	}
	s.poller = poller

	// Step 6: Register listener socket FD with epoll interest tree for EPOLLIN (incoming connections ready to accept)
	if err := s.poller.Register(fd); err != nil {
		_ = poller.Close()
		_ = unix.Close(fd)
		return fmt.Errorf("failed to register listener fd with epoll: %w", err)
	}

	address := s.addr.String()
	log.Printf("Carrot Reactor listening on %s (epoll fd=%d, listener fd=%d)", address, s.poller.epfd, s.listenerFD)

	// Step 7: Instantiate EventLoop with store reference and run main epoll_wait event multiplexing loop
	s.eventLoop = NewEventLoopWithConfig(s.poller, s.listenerFD, s.parser, s.executor, s.store, s.config, s.stopChan)
	return s.eventLoop.Run()
}

// Stop cleanly stops the reactor server and closes listener socket resources.
func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.stopChan) })
}
