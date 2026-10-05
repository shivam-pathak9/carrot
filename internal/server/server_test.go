package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
)

func startTestServer(t *testing.T, cfg config.Config) (*Server, <-chan error) {
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

func TestServerRecoversAOFBeforeAcceptingClients(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.AOFSyncPolicy = "always"
	cfg.AOFPath = filepath.Join(t.TempDir(), "restart.aof")

	first, firstResult := startTestServer(t, cfg)
	conn, err := net.Dial("tcp", first.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	if got := sendRESP(t, conn, "SET", "durable", "value"); got.Type != resp.SimpleString {
		t.Fatalf("SET response = %+v", got)
	}
	if got := sendRESP(t, conn, "LPUSH", "queue", "one", "two"); got.Type != resp.Integer {
		t.Fatalf("LPUSH response = %+v", got)
	}
	if got := sendRESP(t, conn, "AOFREWRITE"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("AOFREWRITE response = %+v", got)
	}
	if got := sendRESP(t, conn, "SET", "after-rewrite", "also-durable"); got.Type != resp.SimpleString {
		t.Fatalf("SET after AOFREWRITE response = %+v", got)
	}
	_ = conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if err := <-firstResult; err != nil {
		t.Fatalf("first server returned: %v", err)
	}

	second, _ := startTestServer(t, cfg)
	recoveredConn, err := net.Dial("tcp", second.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredConn.Close()
	if got := sendRESP(t, recoveredConn, "GET", "durable"); got.Type != resp.BulkString || got.String != "value" {
		t.Fatalf("recovered GET response = %+v", got)
	}
	if got := sendRESP(t, recoveredConn, "GET", "after-rewrite"); got.Type != resp.BulkString || got.String != "also-durable" {
		t.Fatalf("post-rewrite recovered GET response = %+v", got)
	}
	if got := sendRESP(t, recoveredConn, "LRANGE", "queue", "0", "-1"); got.Type != resp.Array ||
		len(got.Array) != 2 || got.Array[0].String != "two" || got.Array[1].String != "one" {
		t.Fatalf("recovered LRANGE response = %+v", got)
	}
}

func TestCrashHarnessServerProcess(t *testing.T) {
	path := os.Getenv("CARROT_CRASH_HARNESS_AOF_PATH")
	if path == "" {
		return
	}
	cfg := config.DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = "0"
	cfg.AOFPath = path
	cfg.AOFSyncPolicy = os.Getenv("CARROT_CRASH_HARNESS_SYNC_POLICY")
	srv := NewServer(cfg)
	started := make(chan error, 1)
	go func() {
		started <- srv.Start()
	}()
	deadline := time.Now().Add(10 * time.Second)
	for srv.Addr() == nil && time.Now().Before(deadline) {
		select {
		case err := <-started:
			t.Fatalf("crash harness server exited before listen: %v", err)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if srv.Addr() == nil {
		t.Fatal("crash harness server did not start listening")
	}
	if _, err := fmt.Fprintf(os.Stdout, "CRASH_HARNESS_READY %s\n", srv.Addr()); err != nil {
		t.Fatal(err)
	}
	select {}
}

type crashHarnessProcess struct {
	cmd  *exec.Cmd
	addr string
	mu   sync.Mutex
	done bool
}

func startCrashHarnessProcess(t *testing.T, path, policy string) *crashHarnessProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHarnessServerProcess$")
	cmd.Env = append(os.Environ(),
		"CARROT_CRASH_HARNESS_AOF_PATH="+path,
		"CARROT_CRASH_HARNESS_SYNC_POLICY="+policy,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	process := &crashHarnessProcess{cmd: cmd}
	t.Cleanup(func() {
		_ = process.kill()
	})
	scanner := bufio.NewScanner(stdout)
	line := make(chan string, 1)
	scanErr := make(chan error, 1)
	go func() {
		if scanner.Scan() {
			line <- scanner.Text()
			return
		}
		scanErr <- scanner.Err()
	}()
	select {
	case got := <-line:
		const prefix = "CRASH_HARNESS_READY "
		if len(got) <= len(prefix) || got[:len(prefix)] != prefix {
			_ = process.kill()
			t.Fatalf("unexpected crash harness readiness output %q; stderr=%q", got, stderr.String())
		}
		process.addr = got[len(prefix):]
	case err := <-scanErr:
		_ = process.kill()
		t.Fatalf("crash harness exited before readiness: scan error=%v stderr=%q", err, stderr.String())
	case <-time.After(15 * time.Second):
		_ = process.kill()
		t.Fatalf("timed out waiting for crash harness readiness; stderr=%q", stderr.String())
	}
	return process
}

func (p *crashHarnessProcess) kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return nil
	}
	p.done = true
	if p.cmd.Process == nil {
		return nil
	}
	killErr := p.cmd.Process.Kill()
	waitErr := p.cmd.Wait()
	if killErr != nil {
		return fmt.Errorf("kill crash harness: %w", killErr)
	}
	if waitErr == nil {
		return errors.New("crash harness exited normally; expected forced termination")
	}
	return nil
}

func crashHarnessRequest(args ...string) ([]byte, error) {
	values := make([]resp.Value, len(args))
	for i, arg := range args {
		values[i] = resp.NewBulkString(arg)
	}
	var request bytes.Buffer
	writer := bufio.NewWriter(&request)
	if err := resp.NewEncoder(writer).Encode(resp.NewArray(values...)); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	return request.Bytes(), nil
}

func writeCrashHarnessCommand(conn net.Conn, decoder *resp.Decoder, args ...string) (resp.Value, error) {
	request, err := crashHarnessRequest(args...)
	if err != nil {
		return resp.Value{}, err
	}
	for len(request) > 0 {
		n, err := conn.Write(request)
		if err != nil {
			return resp.Value{}, err
		}
		request = request[n:]
	}
	return decoder.Decode()
}

func TestCrashHarnessAcknowledgedWritesSurviveSIGKILL(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("AOF crash-recovery harness requires Linux")
	}
	const acknowledgedTarget = 100
	for _, policy := range []string{"always", "everysec"} {
		t.Run(policy, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "appendonly.aof")
			writerProcess := startCrashHarnessProcess(t, path, policy)
			conn, err := net.DialTimeout("tcp", writerProcess.addr, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			decoder := resp.NewDecoder(bufio.NewReader(conn))
			acked := make(chan int, acknowledgedTarget*2)
			writerDone := make(chan error, 1)
			var attempted atomic.Int64
			go func() {
				for id := 0; ; id++ {
					attempted.Store(int64(id + 1))
					response, err := writeCrashHarnessCommand(
						conn, decoder, "SET",
						"crash-harness:"+strconv.Itoa(id), "value",
					)
					if err != nil {
						writerDone <- err
						return
					}
					if response.Type != resp.SimpleString || response.String != "OK" {
						writerDone <- fmt.Errorf("SET %d response = %+v", id, response)
						return
					}
					acked <- id
				}
			}()

			acknowledged := make([]int, 0, acknowledgedTarget)
			for len(acknowledged) < acknowledgedTarget {
				select {
				case id := <-acked:
					acknowledged = append(acknowledged, id)
				case err := <-writerDone:
					t.Fatalf("writer stopped before %d acknowledgements: %v", acknowledgedTarget, err)
				case <-time.After(10 * time.Second):
					t.Fatalf("timed out after %d acknowledgements", len(acknowledged))
				}
			}
			if err := writerProcess.kill(); err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			select {
			case <-writerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("write goroutine did not stop after process termination")
			}
			for len(acked) > 0 {
				acknowledged = append(acknowledged, <-acked)
			}
			attemptedCount := int(attempted.Load())
			ackedSet := make(map[int]struct{}, len(acknowledged))
			for _, id := range acknowledged {
				ackedSet[id] = struct{}{}
			}

			recoveryProcess := startCrashHarnessProcess(t, path, policy)
			defer recoveryProcess.kill()
			recoveryConn, err := net.DialTimeout("tcp", recoveryProcess.addr, 3*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer recoveryConn.Close()
			if err := recoveryConn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			recoveryDecoder := resp.NewDecoder(bufio.NewReader(recoveryConn))
			recoveredAcked, recoveredUnacked := 0, 0
			for id := 0; id < attemptedCount; id++ {
				value, err := writeCrashHarnessCommand(
					recoveryConn, recoveryDecoder,
					"GET", "crash-harness:"+strconv.Itoa(id),
				)
				if err != nil {
					t.Fatalf("GET key %d during recovery: %v", id, err)
				}
				present := value.Type == resp.BulkString && value.String == "value"
				if _, wasAcknowledged := ackedSet[id]; wasAcknowledged {
					if !present {
						t.Errorf("acknowledged key %d was not recovered", id)
					} else {
						recoveredAcked++
					}
				} else if present {
					recoveredUnacked++
				}
			}
			unacknowledged := attemptedCount - len(acknowledged)
			t.Logf("policy=%s acknowledged=%d recovered_acknowledged=%d lost_acknowledged=%d unacknowledged_attempts=%d recovered_unacknowledged=%d absent_unacknowledged=%d",
				policy, len(acknowledged), recoveredAcked, len(acknowledged)-recoveredAcked,
				unacknowledged, recoveredUnacked, unacknowledged-recoveredUnacked)
			if recoveredAcked != len(acknowledged) {
				t.Fatalf("%s lost acknowledged writes across server-process SIGKILL", policy)
			}
		})
	}
}
