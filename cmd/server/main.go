// Package main starts the goroutine-based server and coordinates signal shutdown.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/cpuprofile"
	"github.com/shivam-pathak9/carrot/internal/server"
)

// main loads flags, starts the TCP server, and shuts it down on SIGINT or SIGTERM.
func main() {
	cfg := config.DefaultConfig()
	var cpuProfile string
	flag.StringVar(&cfg.Host, "host", cfg.Host, "TCP address to bind")
	flag.StringVar(&cfg.Port, "port", cfg.Port, "TCP port to bind")
	flag.IntVar(&cfg.MaxConnections, "max-connections", cfg.MaxConnections, "maximum simultaneous client connections")
	flag.IntVar(&cfg.MaxRequestBytes, "max-request-bytes", cfg.MaxRequestBytes, "maximum bytes per RESP request")
	flag.IntVar(&cfg.MaxResponseBytes, "max-response-bytes", cfg.MaxResponseBytes, "maximum bytes per RESP response")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", cfg.ReadTimeout, "client read inactivity timeout")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "client response write timeout")
	flag.BoolVar(&cfg.AOFEnabled, "aof-enabled", cfg.AOFEnabled, "enable append-only persistence")
	flag.StringVar(&cfg.AOFPath, "aof-file", cfg.AOFPath, "append-only persistence file")
	flag.StringVar(&cfg.AOFSyncPolicy, "aof-sync", cfg.AOFSyncPolicy, "AOF sync policy: always, everysec, or no")
	flag.StringVar(&cpuProfile, "cpu-profile", "", "write a Go CPU profile to this file")
	flag.Parse()

	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	if config.IsWildcardHost(cfg.Host) {
		log.Printf("WARNING: listening on all network interfaces without authentication or TLS")
	}
	stopProfile, err := cpuprofile.Start(cpuProfile)
	if err != nil {
		log.Fatalf("start CPU profile: %v", err)
	}
	defer func() {
		if err := stopProfile(); err != nil {
			log.Printf("stop CPU profile: %v", err)
		}
	}()

	srv := server.NewServer(cfg)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Start() }()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	select {
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("server stopped: %v", err)
		}
	case sig := <-signals:
		log.Printf("received %s; shutting down", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown completed with error: %v", err)
		}
		if err := <-serveErr; err != nil {
			log.Fatalf("server stopped: %v", err)
		}
	}
}
