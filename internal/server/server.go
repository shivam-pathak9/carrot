package server

import (
	"log"
	"net"
	"github.com/shivampathak/carrot/internal/client"
	"github.com/shivampathak/carrot/internal/config"
)

type Server struct {
	config   config.Config
	listener net.Listener
}


func NewServer(cfg config.Config) *Server {
	return &Server{
		config: cfg,
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


func (s *Server) handleClient(c *client.Client) {

	defer c.Close()

	buffer := make([]byte, 1024)

	for {
		n, err := c.Read(buffer)
		if err != nil {
			log.Printf("Client disconnected: %s", c.RemoteAddr())
			return
		}

		message := buffer[:n]

		log.Printf("Received: %s", string(message))

		if _, err := c.Write(message); err != nil {
			log.Printf("Write failed: %v", err)
			return
		}
	}
}
