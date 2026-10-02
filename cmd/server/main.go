package main

import (
	"log"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/server"
)

func main() {
	// Entry point for the Carrot server binary.
	// Loads default configuration, constructs a server and starts it.
	// configuration loading
	cfg := config.DefaultConfig()
	// creating new server
	srv := server.NewServer(cfg)

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
