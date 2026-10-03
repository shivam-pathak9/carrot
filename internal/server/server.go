// Package server implements the goroutine-per-client TCP server.
package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/shivam-pathak9/carrot/internal/aof"
	"github.com/shivam-pathak9/carrot/internal/client"
	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// Server owns the listener lifecycle and shares command execution and storage
// across its client handlers.
type Server struct {
	config   config.Config
	listener net.Listener

	store    *storage.Store
	parser   *command.Parser
	executor *command.Executor
	aof      *aof.Log
	aofMu    sync.Mutex

	mu                sync.Mutex
	active            map[net.Conn]struct{}
	closing           bool
	started           bool
	shutdownRequested bool
	stopChan          chan struct{}
	stopOnce          sync.Once
	serveDone         chan struct{}
	clientsWg         sync.WaitGroup
}

// NewServer creates a server with fresh in-memory storage and initialized handlers.
func NewServer(cfg config.Config) *Server {
	cfg = cfg.WithDefaults()
	store := storage.NewStore()
	return &Server{
		config:    cfg,
		store:     store,
		parser:    command.NewParser(),
		executor:  command.NewExecutor(store),
		active:    make(map[net.Conn]struct{}),
		stopChan:  make(chan struct{}),
		serveDone: make(chan struct{}),
	}
}

// Start validates configuration, opens a TCP listener, and serves until
// Shutdown is called or the listener fails.
func (s *Server) Start() error {
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid server configuration: %w", err)
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return nil
	}
	if err := s.openPersistence(); err != nil {
		return err
	}
	address := net.JoinHostPort(s.config.Host, s.config.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Join(fmt.Errorf("listen on %s: %w", address, err), s.closePersistence())
	}
	s.mu.Lock()
	closing = s.closing
	s.mu.Unlock()
	if closing {
		_ = listener.Close()
		return s.closePersistence()
	}
	serveErr := s.Serve(listener)
	s.mu.Lock()
	shutdownRequested := s.shutdownRequested
	if !shutdownRequested {
		for conn := range s.active {
			_ = conn.Close()
		}
	}
	s.mu.Unlock()
	if shutdownRequested {
		return serveErr
	}
	s.clientsWg.Wait()
	return errors.Join(serveErr, s.closePersistence())
}

// Addr returns the bound listener address, or nil before the server starts.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Serve runs the server on an existing listener. It is useful for embedding
// and for tests that need an ephemeral local port.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("listener must not be nil")
	}
	if err := s.config.Validate(); err != nil {
		return fmt.Errorf("invalid server configuration: %w", err)
	}
	if err := s.openPersistence(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		_ = listener.Close()
		return errors.New("server has already been started")
	}
	if s.closing {
		s.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	s.started = true
	s.listener = listener
	s.mu.Unlock()
	defer func() {
		_ = listener.Close()
		s.stopOnce.Do(func() { close(s.stopChan) })
		close(s.serveDone)
	}()

	log.Printf("Carrot listening on %s", listener.Addr())
	go s.startActiveExpireLoop()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing || errors.Is(err, net.ErrClosed) {
				return nil
			}
			if temporary, ok := err.(net.Error); ok && temporary.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			s.mu.Lock()
			s.closing = true
			for activeConn := range s.active {
				_ = activeConn.Close()
			}
			s.mu.Unlock()
			return fmt.Errorf("accept client connection: %w", err)
		}

		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		if len(s.active) >= s.config.MaxConnections {
			s.mu.Unlock()
			log.Printf("rejecting client %s: connection limit reached", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}
		s.active[conn] = struct{}{}
		s.clientsWg.Add(1)
		s.mu.Unlock()
		go s.handleClient(conn)
	}
}

// Shutdown stops accepting requests and waits for clients to disconnect. If
// the context expires, active connections are closed so shutdown can finish.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context must not be nil")
	}
	s.mu.Lock()
	s.shutdownRequested = true
	if !s.started {
		s.closing = true
		s.stopOnce.Do(func() { close(s.stopChan) })
		s.mu.Unlock()
		return s.closePersistence()
	}
	s.closing = true
	listener := s.listener
	s.stopOnce.Do(func() { close(s.stopChan) })
	s.mu.Unlock()

	var closeErr error
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = fmt.Errorf("close listener: %w", err)
		}
	}
	<-s.serveDone

	done := make(chan struct{})
	go func() {
		s.clientsWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return errors.Join(closeErr, s.closePersistence())
	case <-ctx.Done():
		s.mu.Lock()
		for conn := range s.active {
			_ = conn.Close()
		}
		s.mu.Unlock()
		<-done
		return errors.Join(closeErr, ctx.Err(), s.closePersistence())
	}
}

// openPersistence recovers the AOF before clients can execute commands, then
// installs it as the executor's write-ahead journal.
func (s *Server) openPersistence() error {
	if !s.config.AOFEnabled {
		return nil
	}
	s.aofMu.Lock()
	defer s.aofMu.Unlock()
	if s.aof != nil {
		return nil
	}
	logFile, err := aof.Open(s.config.AOFPath, s.config.AOFSyncPolicy, s.store)
	if err != nil {
		return err
	}
	s.aof = logFile
	s.executor.SetJournal(logFile)
	return nil
}

// closePersistence removes the journal from the executor and closes the AOF.
// It is safe to call when AOF is disabled or was never successfully opened.
func (s *Server) closePersistence() error {
	s.aofMu.Lock()
	defer s.aofMu.Unlock()
	if s.aof == nil {
		return nil
	}
	err := s.aof.Close()
	s.aof = nil
	s.executor.SetJournal(nil)
	return err
}

func (s *Server) startActiveExpireLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.store.ActiveExpireCycle()
		case <-s.stopChan:
			return
		}
	}
}

// handleClient decodes each request, passes it through parsing and execution,
// then writes one bounded response before reading the next request.
func (s *Server) handleClient(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.active, conn)
		s.mu.Unlock()
		s.clientsWg.Done()
	}()

	c := client.NewClientWithLimit(conn, s.config.MaxRequestBytes)
	for {
		if err := c.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
			log.Printf("set read deadline for %s: %v", conn.RemoteAddr(), err)
			return
		}
		value, err := c.Decoder().Decode()
		if err != nil {
			var netErr net.Error
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && !(errors.As(err, &netErr) && netErr.Timeout()) {
				log.Printf("decode request from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		cmd, err := s.parser.Parse(value)
		if err != nil {
			if err := s.writeResponse(c, resp.NewError(err.Error())); err != nil {
				log.Printf("write protocol error to %s: %v", conn.RemoteAddr(), err)
				return
			}
			continue
		}
		response, err := s.executor.Execute(cmd)
		if err != nil {
			if err := s.writeResponse(c, resp.NewError(err.Error())); err != nil {
				log.Printf("write command error to %s: %v", conn.RemoteAddr(), err)
				return
			}
			continue
		}
		if err := s.writeResponse(c, response); err != nil {
			log.Printf("write response to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

// writeResponse encodes a response within the configured size limit before
// writing it, so an oversized response is not partially sent to the client.
func (s *Server) writeResponse(c *client.Client, value resp.Value) error {
	var output bytes.Buffer
	writer := bufio.NewWriter(&limitedWriter{writer: &output, limit: s.config.MaxResponseBytes})
	if err := resp.NewEncoder(writer).Encode(value); err != nil {
		return fmt.Errorf("encode response: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("response exceeds %d bytes: %w", s.config.MaxResponseBytes, err)
	}
	if err := c.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if _, err := c.Write(output.Bytes()); err != nil {
		return fmt.Errorf("write response: %w", err)
	}
	return nil
}

type limitedWriter struct {
	writer  io.Writer
	limit   int
	written int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.limit-w.written {
		return 0, fmt.Errorf("output limit exceeded")
	}
	n, err := w.writer.Write(p)
	w.written += n
	return n, err
}
