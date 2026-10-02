//go:build linux

package reactor

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

func startTestReactor(t *testing.T, cfg config.Config) (*Server, <-chan error) {
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
			t.Fatalf("reactor exited before listening: %v", err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if srv.Addr() == nil {
		t.Fatal("reactor did not start listening")
	}
	t.Cleanup(func() {
		srv.Stop()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("reactor did not stop")
		}
	})
	return srv, result
}

func reactorCommand(t *testing.T, conn net.Conn, args ...string) resp.Value {
	t.Helper()
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.NewBulkString(arg)
	}
	var wire bytes.Buffer
	writer := bufio.NewWriter(&wire)
	if err := resp.NewEncoder(writer).Encode(resp.NewArray(values...)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(wire.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	value, err := resp.NewDecoder(bufio.NewReader(conn)).Decode()
	if err != nil {
		t.Fatalf("decode reactor response: %v", err)
	}
	return value
}

func TestReactorTCPListFlowAndWrongType(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.ReadTimeout = time.Second
	srv, _ := startTestReactor(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := reactorCommand(t, conn, "PING"); got.Type != resp.SimpleString || got.String != "PONG" {
		t.Fatalf("PING response = %+v", got)
	}
	if got := reactorCommand(t, conn, "LPUSH", "list", "a", "b"); got.Type != resp.Integer || got.Integer != 2 {
		t.Fatalf("LPUSH response = %+v", got)
	}
	if got := reactorCommand(t, conn, "LRANGE", "list", "0", "-1"); got.Type != resp.Array || len(got.Array) != 2 || got.Array[0].String != "b" || got.Array[1].String != "a" {
		t.Fatalf("LRANGE response = %+v", got)
	}
	if got := reactorCommand(t, conn, "GET", "list"); got.Type != resp.Error || got.String != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Fatalf("GET list response = %+v", got)
	}
}

func TestReactorProcessesPipelinedRequests(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	srv, _ := startTestReactor(t, cfg)
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

func TestReactorConnectionAndBufferLimits(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.MaxConnections = 1
	cfg.MaxRequestBytes = 64
	cfg.MaxResponseBytes = 16
	cfg.ReadTimeout = 150 * time.Millisecond
	cfg.WriteTimeout = 100 * time.Millisecond
	srv, _ := startTestReactor(t, cfg)
	srv.store.Set("large", "this-value-is-larger-than-sixteen", 0)

	first, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = second.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection over limit should be closed")
	}
	_ = second.Close()

	if _, err := first.Write([]byte("*2\r\n$3\r\nGET\r\n$5\r\nlarge\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = first.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := resp.NewDecoder(bufio.NewReader(first)).Decode(); err == nil {
		t.Fatal("oversized response should close the connection")
	}
}

func TestReactorStopsAndClosesIdleClients(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.ReadTimeout = 100 * time.Millisecond
	srv, result := startTestReactor(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle client should be closed")
	}

	srv.Stop()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Start() returned %v after Stop()", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reactor did not stop")
	}
}
