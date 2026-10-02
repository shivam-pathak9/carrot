//go:build linux

package main

import (
	"log"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/reactor"
)

func main() {
	// Entry point for the Carrot Reactor server binary.
	// Uses single-threaded epoll I/O multiplexing event loop.
	cfg := config.DefaultConfig()
	srv := reactor.NewServer(cfg)

	if err := srv.Start(); err != nil {
		log.Fatalf("Reactor server error: %v", err)
	}
}
