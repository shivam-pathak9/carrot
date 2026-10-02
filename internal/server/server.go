package server

import (
	"log"
	"net"
	"time"

	"github.com/shivam-pathak9/carrot/internal/client"
	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

type Server struct {
	config   config.Config
	listener net.Listener

	store    *storage.Store
	parser   *command.Parser
	executor *command.Executor

	stopChan chan struct{}
}

func NewServer(cfg config.Config) *Server {
	// NewServer constructs a server instance with the provided
	// configuration, a fresh storage engine, and initial command parser/executor.
	store := storage.NewStore()
	return &Server{
		config:   cfg,
		store:    store,
		parser:   command.NewParser(),
		executor: command.NewExecutor(store),
		stopChan: make(chan struct{}),
	}
}

func (s *Server) Start() error {

	address := s.config.Host + ":" + s.config.Port

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	s.listener = listener

	log.Printf("Carrot listening on %s\n", address)

	// Launch background Active Expiration loop for multi-threaded server.
	// Runs every 100ms in a dedicated background goroutine.
	go s.startActiveExpireLoop()

	// Accept loop: accept connections and start a goroutine to
	// handle each client independently.
	for {

		conn, err := s.listener.Accept()

		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("Client Connected: %s\n", conn.RemoteAddr())

		client := client.NewClient(conn)
		go s.handleClient(client)

	}
}

// startActiveExpireLoop runs a periodic cleanup pass for expired keys in the goroutine-based server.
//
// This is separate from client request handling so the store can reclaim stale TTL entries even when no
// client is actively reading or writing them. The loop wakes every 100ms and calls ActiveExpireCycle().
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

func (s *Server) handleClient(client *client.Client) {
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("failed to close client connection: %v", err)
		}
	}()

	if err := client.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return
	}

	// handleClient processes one client connection by repeatedly decoding a RESP request,
	// parsing it into a command, executing it, and writing the response back over the same socket.
	// A client disconnect, protocol error, or timeout ends the loop and closes the connection.
	for {
		// 1. Decode RESP request
		value, err := client.Decoder().Decode()
		if err != nil {
			return
		}
		if err := client.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return
		}

		// 2. Parse RESP into a Command
		cmd, err := s.parser.Parse(value)
		if err != nil {
			_ = client.Encoder().Encode(resp.NewError(err.Error()))
			_ = client.Flush()
			continue
		}

		// 3. Execute command
		response, err := s.executor.Execute(cmd)
		if err != nil {
			_ = client.Encoder().Encode(resp.NewError(err.Error()))
			_ = client.Flush()
			continue
		}

		// 4. Encode RESP response
		if err := client.Encoder().Encode(response); err != nil {
			return
		}

		// 5. Send it to the client
		// We call `Flush()` to ensure the buffered encoder output is
		// transmitted to the remote peer. If Flush fails it usually
		// indicates the connection has been closed or encounter IO
		// errors and we should terminate the handler.
		if err := client.Flush(); err != nil {
			return
		}
	}
}
