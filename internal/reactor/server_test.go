//go:build linux

package reactor

import (
	"bufio"
	"bytes"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

func startTestReactor(t *testing.T, cfg config.Config) (*Server, <-chan error) {
	t.Helper()
	if cfg.AOFEnabled && (cfg.AOFPath == "" || cfg.AOFPath == "appendonly.aof") {
		cfg.AOFPath = filepath.Join(t.TempDir(), "appendonly.aof")
	}
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

func TestReactorIPv6Binding(t *testing.T) {
	probe, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("close IPv6 availability probe: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Host = "::1"
	cfg.Port = "0"
	cfg.ReadTimeout = time.Second
	srv, _ := startTestReactor(t, cfg)
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("IPv6 loopback probe succeeded but reactor dial failed: %v", err)
	}

	defer conn.Close()

	if got := reactorCommand(t, conn, "PING"); got.Type != resp.SimpleString || got.String != "PONG" {
		t.Fatalf("PING response over IPv6 = %+v", got)
	}
}

func TestReactorRecoversAOFBeforeAcceptingClients(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.AOFSyncPolicy = "always"
	cfg.AOFPath = filepath.Join(t.TempDir(), "restart.aof")

	first, firstResult := startTestReactor(t, cfg)
	conn, err := net.Dial("tcp", first.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if got := reactorCommand(t, conn, "SET", "durable", "value"); got.Type != resp.SimpleString {
		t.Fatalf("SET response = %+v", got)
	}
	if got := reactorCommand(t, conn, "RPUSH", "queue", "one", "two"); got.Type != resp.Integer {
		t.Fatalf("RPUSH response = %+v", got)
	}
	if got := reactorCommand(t, conn, "AOFREWRITE"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("AOFREWRITE response = %+v", got)
	}
	if got := reactorCommand(t, conn, "SET", "after-rewrite", "also-durable"); got.Type != resp.SimpleString {
		t.Fatalf("SET after AOFREWRITE response = %+v", got)
	}
	_ = conn.Close()
	first.Stop()
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first reactor returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first reactor did not stop")
	}

	second, _ := startTestReactor(t, cfg)
	recoveredConn, err := net.Dial("tcp", second.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredConn.Close()
	if got := reactorCommand(t, recoveredConn, "GET", "durable"); got.Type != resp.BulkString || got.String != "value" {
		t.Fatalf("recovered GET response = %+v", got)
	}
	if got := reactorCommand(t, recoveredConn, "GET", "after-rewrite"); got.Type != resp.BulkString || got.String != "also-durable" {
		t.Fatalf("post-rewrite recovered GET response = %+v", got)
	}
	if got := reactorCommand(t, recoveredConn, "LRANGE", "queue", "0", "-1"); got.Type != resp.Array ||
		len(got.Array) != 2 || got.Array[0].String != "one" || got.Array[1].String != "two" {
		t.Fatalf("recovered LRANGE response = %+v", got)
	}
}
