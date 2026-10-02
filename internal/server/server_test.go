package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

func startTestServer(t *testing.T, cfg config.Config) (*Server, <-chan error) {
	t.Helper()
	srv := NewServer(cfg)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		result <- srv.Start()
		close(done)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for srv.Addr() == nil && time.Now().Before(deadline) {
		select {
		case err := <-result:
			t.Fatalf("server exited before listening: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("server did not start listening")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})
	return srv, result
}

func sendRESP(t *testing.T, conn net.Conn, values ...string) resp.Value {
	t.Helper()
	var request bytes.Buffer
	writer := bufio.NewWriter(&request)
	encoder := resp.NewEncoder(writer)
	args := make([]resp.Value, len(values))
	for i, value := range values {
		args[i] = resp.NewBulkString(value)
	}
	if err := encoder.Encode(resp.NewArray(args...)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(request.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	value, err := resp.NewDecoder(bufio.NewReader(conn)).Decode()
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return value
}

func TestServerTCPListFlowAndWrongType(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	srv, _ := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := sendRESP(t, conn, "PING"); got.Type != resp.SimpleString || got.String != "PONG" {
		t.Fatalf("PING response = %+v", got)
	}
	if got := sendRESP(t, conn, "LPUSH", "list", "a", "b"); got.Type != resp.Integer || got.Integer != 2 {
		t.Fatalf("LPUSH response = %+v", got)
	}
	if got := sendRESP(t, conn, "LRANGE", "list", "0", "-1"); got.Type != resp.Array || len(got.Array) != 2 || got.Array[0].String != "b" || got.Array[1].String != "a" {
		t.Fatalf("LRANGE response = %+v", got)
	}
	if got := sendRESP(t, conn, "GET", "list"); got.Type != resp.Error || got.String != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("GET list response = %+v", got)
	}
}

func TestServerProcessesPipelinedRequests(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	srv, _ := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	decoder := resp.NewDecoder(bufio.NewReader(conn))
	for i := 0; i < 2; i++ {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		value, err := decoder.Decode()
		if err != nil {
			t.Fatalf("decode pipelined response %d: %v", i, err)
		}
		if value.Type != resp.SimpleString || value.String != "PONG" {
			t.Fatalf("pipelined response %d = %+v", i, value)
		}
	}
}

func TestServerRequestAndResponseLimits(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.MaxRequestBytes = 128
	cfg.MaxResponseBytes = 16
	srv, _ := startTestServer(t, cfg)

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$200\r\n%s\r\n", bytes.Repeat([]byte("k"), 200))))
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	one := make([]byte, 1)
	if _, err := conn.Read(one); err == nil {
		t.Fatal("oversized request should close the connection without a response")
	}
	_ = conn.Close()

	conn, err = net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if got := sendRESP(t, conn, "SET", "large", "this-value-is-longer-than-sixteen"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("SET response = %+v", got)
	}
	if _, err := conn.Write([]byte("*2\r\n$3\r\nGET\r\n$5\r\nlarge\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := resp.NewDecoder(bufio.NewReader(conn)).Decode(); err == nil {
		t.Fatal("oversized response should close the connection")
	}
}

func TestServerConnectionLimitAndReadTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.MaxConnections = 1
	cfg.ReadTimeout = 100 * time.Millisecond
	srv, _ := startTestServer(t, cfg)

	first, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	_, _ = second.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection over limit should be closed")
	}

	_ = first.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := first.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection should be closed at the read timeout")
	}
}

func TestServerShutdownForcesCloseAfterContext(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	srv, result := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for {
		srv.mu.Lock()
		active := len(srv.active)
		srv.mu.Unlock()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not register the idle client")
		}
		time.Sleep(time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = srv.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline exceeded for active connection", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Start() returned %v on graceful shutdown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server accept loop did not stop")
	}
}

func TestServerShutdownWaitsForActiveClients(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	srv, result := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		srv.mu.Lock()
		active := len(srv.active)
		srv.mu.Unlock()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			_ = conn.Close()
			t.Fatal("server did not register the client")
		}
		time.Sleep(time.Millisecond)
	}

	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdown <- srv.Shutdown(ctx)
	}()
	_ = conn.Close()
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("Shutdown() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown() did not complete after client disconnect")
	}
	if err := <-result; err != nil {
		t.Fatalf("Start() returned %v after shutdown", err)
	}
}
