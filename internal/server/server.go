package server

import (
	"github.com/shivampathak/carrot/internal/client"
	"github.com/shivampathak/carrot/internal/command"
	"github.com/shivampathak/carrot/internal/config"
	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
	"log"
	"net"
)

type Server struct {
	config   config.Config
	listener net.Listener

	parser   *command.Parser
	executor *command.Executor
}

func NewServer(cfg config.Config) *Server {
	// NewServer constructs a server instance with the provided
	// configuration, a fresh storage engine, and initial command parser/executor.
	store := storage.NewStore()
	return &Server{
		config:   cfg,
		parser:   command.NewParser(),
		executor: command.NewExecutor(store),
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

func (s *Server) handleClient(client *client.Client) {
	// handleClient runs in a goroutine per connected client.
	// It follows a clear request-response pipeline:
	// 1. `Decode()` reads a RESP value from the client (may be an
	//    array representing a command).
	// 2. `Parse()` converts the RESP value to a `Command`.
	// 3. `Execute()` produces a response `Value`.
	// 4. `Encoder.Encode()` writes the response into the client's
	//    buffered writer.
	// 5. `Flush()` sends buffered data to the network.
	//
	// This separation allows batching multiple writes into a
	// single `Flush()` for throughput, while still providing
	// explicit flush points for protocol correctness.
	for {
		// 1. Decode RESP request
		value, err := client.Decoder().Decode()
		if err != nil {
			return
		}

		// 2. Parse RESP into a Command
		cmd, err := s.parser.Parse(value)
		if err != nil {
			_ = client.Encoder().Encode(resp.NewError(err.Error()))
			_ = client.Flush()
			continue
		}

		log.Printf("Command=%s Args=%v", cmd.Name, cmd.Args)

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
