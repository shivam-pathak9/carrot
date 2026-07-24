package server

import (
	"log"
	"net"

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

		go s.handleConnection(conn)

	}
}


func (s *Server) handleConnection(conn net.Conn) {

	defer conn.Close()

	buffer := make([]byte, 1024)

	for {

		n, err := conn.Read(buffer)

		if err != nil {
			log.Printf("Client disconnected: %s\n", conn.RemoteAddr())
			return
		}

		message := string(buffer[:n])

		log.Printf("Received: %s", message)

		conn.Write([]byte(message))

	}

}
