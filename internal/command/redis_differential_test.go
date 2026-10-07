package command

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

func TestRedisDifferentialCommonCommands(t *testing.T) {
	redisServer, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server is not installed")
	}

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	process := exec.Command(redisServer,
		"--bind", "127.0.0.1",
		"--port", fmt.Sprint(port),
		"--save", "",
		"--appendonly", "no",
		"--dir", t.TempDir(),
		"--loglevel", "warning",
	)
	process.Stdout = io.Discard
	process.Stderr = io.Discard
	if err := process.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	t.Cleanup(func() {
		if process.Process != nil {
			_ = process.Process.Kill()
			_ = process.Wait()
		}
	})

	address := fmt.Sprintf("127.0.0.1:%d", port)
	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("connect to redis-server at %s: %v", address, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	redisReader := resp.NewDecoder(bufio.NewReader(conn))
	redisWriter := bufio.NewWriter(conn)

	executor := NewExecutor(storage.NewStore())
	commands := []Command{
		{Name: "PING"},
		{Name: "SET", Args: []string{"diff:key", "value"}},
		{Name: "GET", Args: []string{"diff:key"}},
		{Name: "GET", Args: []string{"diff:missing"}},
		{Name: "DEL", Args: []string{"diff:missing"}},
		{Name: "RPUSH", Args: []string{"diff:list", "one", "two"}},
		{Name: "LPUSH", Args: []string{"diff:list", "zero"}},
		{Name: "LLEN", Args: []string{"diff:list"}},
		{Name: "LRANGE", Args: []string{"diff:list", "0", "-1"}},
		{Name: "LINDEX", Args: []string{"diff:list", "1"}},
		{Name: "LPOP", Args: []string{"diff:list"}},
		{Name: "RPOP", Args: []string{"diff:list"}},
		{Name: "DEL", Args: []string{"diff:key", "diff:list"}},
	}
	for i, cmd := range commands {
		args := make([]resp.Value, 1, len(cmd.Args)+1)
		args[0] = resp.NewBulkString(cmd.Name)
		for _, arg := range cmd.Args {
			args = append(args, resp.NewBulkString(arg))
		}
		var wire bytes.Buffer
		wireWriter := bufio.NewWriter(&wire)
		if err := resp.NewEncoder(wireWriter).Encode(resp.NewArray(args...)); err != nil {
			t.Fatalf("encode command %d for redis-server: %v", i, err)
		}
		if err := wireWriter.Flush(); err != nil {
			t.Fatalf("flush command %d encoding: %v", i, err)
		}
		if _, err := redisWriter.Write(wire.Bytes()); err != nil {
			t.Fatalf("write command %d to redis-server: %v", i, err)
		}
		if err := redisWriter.Flush(); err != nil {
			t.Fatalf("flush command %d to redis-server: %v", i, err)
		}
		redisValue, err := redisReader.Decode()
		if err != nil {
			t.Fatalf("decode redis-server response for %v: %v", cmd, err)
		}
		carrotValue, err := executor.Execute(cmd)
		if err != nil {
			t.Fatalf("execute Carrot command %v: %v", cmd, err)
		}
		if !reflect.DeepEqual(carrotValue, redisValue) {
			t.Errorf("command %v: Carrot returned %+v, redis-server returned %+v", cmd, carrotValue, redisValue)
		}
	}
}
