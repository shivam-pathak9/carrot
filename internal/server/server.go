package server

import (
	"github.com/shivampathak/carrot/internal/client"
	"github.com/shivampathak/carrot/internal/command"
	"github.com/shivampathak/carrot/internal/config"
	"github.com/shivampathak/carrot/internal/protocol/resp"
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
	return &Server{
		config:   cfg,
		parser:   command.NewParser(),
		executor: command.NewExecutor(),
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
		if err := client.Flush(); err != nil {
			return
		}
	}
}
