//go:build linux

package reactor

import (
	"fmt"
	"log"
	"net"
	"strconv"

	"github.com/shivampathak/carrot/internal/command"
	"github.com/shivampathak/carrot/internal/config"
	"golang.org/x/sys/unix"
)

// Server implements a non-blocking TCP server driven by the epoll Reactor EventLoop.
type Server struct {
	config     config.Config
	listenerFD int

	poller    *Poller
	eventLoop *EventLoop

	parser   *command.Parser
	executor *command.Executor
}

// NewServer constructs a new Reactor Server instance with default parser and executor.
func NewServer(cfg config.Config) *Server {
	return &Server{
		config:   cfg,
		parser:   command.NewParser(),
		executor: command.NewExecutor(),
	}
}

// Start creates the non-blocking listening socket, registers it with epoll, and runs the reactor event loop.
func (s *Server) Start() error {
	portInt, err := strconv.Atoi(s.config.Port)
	if err != nil {
		return fmt.Errorf("invalid port %q: %w", s.config.Port, err)
	}

	// 1. Create non-blocking socket via system call
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}
	s.listenerFD = fd

	// 2. Set SO_REUSEADDR option
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to set SO_REUSEADDR: %w", err)
	}

	// 3. Bind socket to configured IP and port
	ip := net.ParseIP(s.config.Host)
	var addr [4]byte
	if ip4 := ip.To4(); ip4 != nil {
		copy(addr[:], ip4)
	}

	sa := &unix.SockaddrInet4{
		Port: portInt,
		Addr: addr,
	}

	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to bind socket to %s:%s: %w", s.config.Host, s.config.Port, err)
	}

	// 4. Listen on socket
	if err := unix.Listen(fd, 128); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to listen on socket: %w", err)
	}

	// 5. Initialize epoll Poller
	poller, err := NewPoller(DefaultMaxEvents)
	if err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("failed to create reactor poller: %w", err)
	}
	s.poller = poller

	// 6. Register listener FD with poller for incoming connection readiness
	if err := s.poller.Register(fd); err != nil {
		_ = poller.Close()
		_ = unix.Close(fd)
		return fmt.Errorf("failed to register listener fd with epoll: %w", err)
	}

	address := fmt.Sprintf("%s:%s", s.config.Host, s.config.Port)
	log.Printf("Carrot Reactor listening on %s (epoll fd=%d, listener fd=%d)", address, s.poller.epfd, s.listenerFD)

	// 7. Instantiate and run EventLoop
	s.eventLoop = NewEventLoop(s.poller, s.listenerFD, s.parser, s.executor)
	return s.eventLoop.Run()
}

// Stop cleanly stops the server and closes listening resources.
func (s *Server) Stop() {
	if s.eventLoop != nil {
		s.eventLoop.Stop()
	}
	if s.listenerFD > 0 {
		_ = unix.Close(s.listenerFD)
	}
}
