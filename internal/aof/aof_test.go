package aof

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/command"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

func execute(t *testing.T, executor *command.Executor, args ...string) resp.Value {
	t.Helper()
	cmd := command.Command{Name: args[0], Args: args[1:]}
	value, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("execute %v: %v", args, err)
	}
	if value.Type == resp.Error {
		t.Fatalf("execute %v returned RESP error: %s", args, value.String)
	}
	return value
}

func TestAOFReplaysStringListsAndAbsoluteExpirations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	execute(t, executor, "SET", "persist", "value")
	execute(t, executor, "SET", "short-lived", "gone", "PX", "100")
	execute(t, executor, "RPUSH", "items", "first", "second")
	execute(t, executor, "EXPIRE", "items", "60")
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "PXAT") || !strings.Contains(string(data), "PEXPIREAT") {
		t.Fatalf("AOF should store absolute expiration commands, got %q", data)
	}
	time.Sleep(120 * time.Millisecond)

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredLog.Close()
	recoveredExecutor := command.NewExecutor(recovered)
	if got := execute(t, recoveredExecutor, "GET", "persist"); got.Type != resp.BulkString || got.String != "value" {
		t.Fatalf("recovered GET = %+v", got)
	}
	if got := execute(t, recoveredExecutor, "GET", "short-lived"); got.Type != resp.BulkString || !got.IsNull {
		t.Fatalf("expired key response = %+v", got)
	}
	if got := execute(t, recoveredExecutor, "LRANGE", "items", "0", "-1"); got.Type != resp.Array ||
		len(got.Array) != 2 || got.Array[0].String != "first" || got.Array[1].String != "second" {
		t.Fatalf("recovered list = %+v", got)
	}
}

func TestAOFTruncatesIncompleteFinalRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	execute(t, executor, "SET", "key", "value")
	for i := 0; i < 300; i++ {
		execute(t, executor, "SET", "buffered:key:"+strconv.Itoa(i), "buffered-value")
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	validSize := info.Size()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("*3\r\n$3\r\nSET\r\n$5\r\nother\r\n$20\r\npartial"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatal(err)
	}
	if err := recoveredLog.Close(); err != nil {
		t.Fatal(err)
	}
	recoveredKey, err := command.NewExecutor(recovered).Execute(command.Command{
		Name: "GET",
		Args: []string{"buffered:key:299"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if recoveredKey.Type != resp.BulkString || recoveredKey.String != "buffered-value" {
		t.Fatalf("last complete record was not recovered: %+v", recoveredKey)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != validSize {
		t.Fatalf("recovered AOF size = %d, want truncated size %d", info.Size(), validSize)
	}
}

func TestAOFCorruptionAndExclusiveLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	first, err := Open(path, "no", storage.NewStore())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path, "no", storage.NewStore()); err == nil {
		t.Fatal("second process should not open the same AOF")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(":not-an-integer\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, "no", storage.NewStore()); err == nil {
		t.Fatal("malformed complete record should fail recovery")
	}
}

func TestAOFRejectsInvalidOpenSettingsAndUnexpectedCommands(t *testing.T) {
	store := storage.NewStore()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	for _, test := range []struct {
		name   string
		path   string
		policy string
		store  *storage.Store
	}{
		{name: "empty path", policy: "always", store: store},
		{name: "invalid policy", path: path, policy: "sometimes", store: store},
		{name: "nil store", path: path, policy: "always"},
		{name: "missing parent", path: filepath.Join(t.TempDir(), "missing", "data.aof"), policy: "always", store: store},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Open(test.path, test.policy, test.store); err == nil {
				t.Fatal("Open() unexpectedly succeeded")
			}
		})
	}

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := bufio.NewWriter(file)
	encoder := resp.NewEncoder(writer)
	if err := encoder.Encode(resp.NewArray(resp.NewBulkString("PING"))); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, "always", storage.NewStore()); err == nil {
		t.Fatal("recovery should reject a read-only command in the AOF")
	}
}

func TestAOFPeriodicSyncAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "everysec", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	execute(t, executor, "SET", "periodic", "synced")
	time.Sleep(1100 * time.Millisecond)
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "everysec", recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredLog.Close()
	if got := execute(t, command.NewExecutor(recovered), "GET", "periodic"); got.Type != resp.BulkString || got.String != "synced" {
		t.Fatalf("recovered GET = %+v", got)
	}
}

func TestAOFRecordsMutationThatReturnsCommandError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	execute(t, executor, "SET", "string-key", "value")

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	response, err := executor.Execute(command.Command{
		Name: "LPUSH",
		Args: []string{"string-key", "item"},
	})
	if err != nil {
		t.Fatalf("LPUSH execution returned Go error: %v", err)
	}
	if response.Type != resp.Error {
		t.Fatalf("LPUSH response = %+v, want command error", response)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= before.Size() {
		t.Fatal("current write-ahead behavior should record the rejected mutation")
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatalf("replay of rejected mutation failed: %v", err)
	}
	defer recoveredLog.Close()
	got := execute(t, command.NewExecutor(recovered), "GET", "string-key")
	if got.Type != resp.BulkString || got.String != "value" {
		t.Fatalf("recovered value = %+v, want original string", got)
	}
}

func TestAOFSubprocessCrashWriter(t *testing.T) {
	path := os.Getenv("CARROT_AOF_CRASH_TEST_PATH")
	if path == "" {
		return
	}
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	for i := 0; i < 10; i++ {
		execute(t, executor, "SET", "crash:key:"+strconv.Itoa(i), "durable")
	}
	if _, err := os.Stdout.WriteString("AOF_READY\n"); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestAOFRecoversAfterAbruptProcessTermination(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("AOF file locking and crash recovery test requires Linux")
	}
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAOFSubprocessCrashWriter$")
	cmd.Env = append(os.Environ(), "CARROT_AOF_CRASH_TEST_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "AOF_READY" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("crash writer did not become ready: output=%q err=%v stderr=%q",
			scanner.Text(), scanner.Err(), stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		_ = cmd.Wait()
		t.Fatalf("kill crash writer: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("crash writer exited normally; expected forced termination")
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatalf("reopen AOF after forced termination: %v", err)
	}
	defer recoveredLog.Close()
	executor := command.NewExecutor(recovered)
	for i := 0; i < 10; i++ {
		got := execute(t, executor, "GET", "crash:key:"+strconv.Itoa(i))
		if got.Type != resp.BulkString || got.String != "durable" {
			t.Fatalf("recovered key %d = %+v, want durable value", i, got)
		}
	}
}

func TestCanonicalCommandSkipsMalformedExpirations(t *testing.T) {
	now := time.Now()
	for _, cmd := range []command.Command{
		{Name: "SET", Args: []string{"key"}},
		{Name: "SET", Args: []string{"key", "value", "EX", "invalid"}},
		{Name: "SET", Args: []string{"key", "value", "PX", "0"}},
		{Name: "SET", Args: []string{"key", "value", "PXAT"}},
		{Name: "SET", Args: []string{"key", "value", "NX"}},
		{Name: "EXPIRE", Args: []string{"key"}},
		{Name: "EXPIRE", Args: []string{"key", "invalid"}},
		{Name: "DEL"},
	} {
		if _, ok := canonicalCommand(cmd, now); ok {
			t.Errorf("canonicalCommand(%+v) unexpectedly accepted malformed mutation", cmd)
		}
	}

	if command.IsMutation("GET") {
		t.Fatal("GET must not be persisted as a mutation")
	}
}

func TestCanonicalCommandAcceptsEveryKnownMutationFamily(t *testing.T) {
	for _, cmd := range []command.Command{
		{Name: "SET", Args: []string{"key", "value"}},
		{Name: "DEL", Args: []string{"key"}},
		{Name: "EXPIRE", Args: []string{"key", "0"}},
		{Name: "PEXPIREAT", Args: []string{"key", "123"}},
		{Name: "LPUSH", Args: []string{"list", "value"}},
		{Name: "RPUSH", Args: []string{"list", "value"}},
		{Name: "LPUSHX", Args: []string{"list", "value"}},
		{Name: "RPUSHX", Args: []string{"list", "value"}},
		{Name: "LPOP", Args: []string{"list"}},
		{Name: "RPOP", Args: []string{"list"}},
		{Name: "LSET", Args: []string{"list", "0", "value"}},
		{Name: "LTRIM", Args: []string{"list", "0", "-1"}},
		{Name: "LREM", Args: []string{"list", "0", "value"}},
		{Name: "LINSERT", Args: []string{"list", "BEFORE", "pivot", "value"}},
		{Name: "LMOVE", Args: []string{"src", "dst", "LEFT", "RIGHT"}},
		{Name: "RPOPLPUSH", Args: []string{"src", "dst"}},
	} {
		if !command.IsMutation(cmd.Name) {
			t.Fatalf("%s is missing from mutation classification", cmd.Name)
		}
		if _, ok := canonicalCommand(cmd, time.Now()); !ok {
			t.Errorf("canonicalCommand(%+v) rejected a known mutation", cmd)
		}
	}
}

func TestAOFAppendFailureMarksLogUnhealthy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.aof")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	readOnly, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()

	logFile := &Log{
		file:   readOnly,
		writer: bufio.NewWriter(readOnly),
		policy: "no",
	}
	cmd := command.Command{Name: "SET", Args: []string{"key", "value"}}
	if _, err := logFile.Append(cmd); err == nil {
		t.Fatal("append to read-only file should fail")
	}
	if logFile.failed == nil {
		t.Fatal("failed append should poison the log")
	}
	if _, err := logFile.Append(cmd); err == nil {
		t.Fatal("subsequent append should fail after the log is unhealthy")
	}
}

func TestAOFRewriteCompactsAndPreservesCurrentState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)

	for i := 0; i < 100; i++ {
		execute(t, executor, "SET", "churn", "old-value-"+strconv.Itoa(i))
	}
	execute(t, executor, "SET", "removed", "should-not-return")
	execute(t, executor, "DEL", "removed")
	execute(t, executor, "SET", "persistent", "keep-me")
	execute(t, executor, "SET", "volatile", "ttl-value", "PX", "60000")
	execute(t, executor, "RPUSH", "list", "one", "two", "three")
	execute(t, executor, "EXPIRE", "list", "60")

	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := execute(t, executor, "AOFREWRITE"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("AOFREWRITE response = %+v", got)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if afterInfo.Size() >= beforeInfo.Size() {
		t.Fatalf("rewrite size = %d, want smaller than original %d", afterInfo.Size(), beforeInfo.Size())
	}
	if _, err := Open(path, "always", storage.NewStore()); err == nil {
		t.Fatal("replacement AOF should remain exclusively locked while the original Log is open")
	}

	// Commands after replacement must append to the new file and survive replay.
	execute(t, executor, "SET", "after-rewrite", "also-kept")
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredLog.Close()
	recoveredExecutor := command.NewExecutor(recovered)

	for key, want := range map[string]string{
		"churn":         "old-value-99",
		"persistent":    "keep-me",
		"after-rewrite": "also-kept",
		"volatile":      "ttl-value",
	} {
		got := execute(t, recoveredExecutor, "GET", key)
		if got.Type != resp.BulkString || got.String != want {
			t.Errorf("GET %q = %+v, want %q", key, got, want)
		}
	}
	if got := execute(t, recoveredExecutor, "GET", "removed"); got.Type != resp.BulkString || !got.IsNull {
		t.Errorf("deleted key after recovery = %+v, want null", got)
	}
	if got := execute(t, recoveredExecutor, "LRANGE", "list", "0", "-1"); got.Type != resp.Array ||
		len(got.Array) != 3 || got.Array[0].String != "one" || got.Array[1].String != "two" || got.Array[2].String != "three" {
		t.Errorf("recovered list = %+v", got)
	}
	if got := execute(t, recoveredExecutor, "TTL", "list"); got.Type != resp.Integer || got.Integer <= 0 {
		t.Errorf("recovered list TTL = %+v, want positive remaining TTL", got)
	}

	tempFiles, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".appendonly.aof.rewrite-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tempFiles) != 0 {
		t.Errorf("rewrite left temporary files behind: %v", tempFiles)
	}
}

func TestAOFRewriteRequiresEnabledRewritableJournal(t *testing.T) {
	executor := command.NewExecutor(storage.NewStore())
	if _, err := executor.Execute(command.Command{Name: "AOFREWRITE"}); err == nil {
		t.Fatal("AOFREWRITE should fail when no journal is installed")
	}
	if got, err := executor.Execute(command.Command{Name: "AOFREWRITE", Args: []string{"extra"}}); err != nil ||
		got.Type != resp.Error {
		t.Fatalf("AOFREWRITE with arguments = (%+v, %v), want RESP error", got, err)
	}
}

func TestAOFRewriteFailureKeepsOriginalLogAndRemovesTempFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "always", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)
	execute(t, executor, "SET", "original", "still-valid")

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite cannot emit a single RESP record larger than the recovery limit.
	// This makes snapshot generation fail after creating the temporary file,
	// exercising cleanup without relying on permissions that behave differently
	// when tests run as root.
	store.Set("oversized", strings.Repeat("x", maxRecordBytes+1), 0)
	if err := executor.RewriteAOF(); err == nil {
		t.Fatal("rewrite with an oversized current value should fail")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("failed rewrite changed the original AOF")
	}
	tempFiles, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".appendonly.aof.rewrite-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(tempFiles) != 0 {
		t.Errorf("failed rewrite left temporary files behind: %v", tempFiles)
	}

	execute(t, executor, "SET", "after-failure", "writable")
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "always", recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredLog.Close()
	recoveredExecutor := command.NewExecutor(recovered)
	for key, want := range map[string]string{
		"original":      "still-valid",
		"after-failure": "writable",
	} {
		got := execute(t, recoveredExecutor, "GET", key)
		if got.Type != resp.BulkString || got.String != want {
			t.Errorf("recovered GET %q = %+v, want %q", key, got, want)
		}
	}
}

func TestAOFRewriteDoesNotLoseConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	store := storage.NewStore()
	logFile, err := Open(path, "no", store)
	if err != nil {
		t.Fatal(err)
	}
	executor := command.NewExecutor(store)
	executor.SetJournal(logFile)

	for i := 0; i < 500; i++ {
		execute(t, executor, "SET", "before:"+strconv.Itoa(i), "value")
	}

	start := make(chan struct{})
	var writers sync.WaitGroup
	writeErrors := make(chan error, 64)
	for i := 0; i < 64; i++ {
		writers.Add(1)
		go func(key int) {
			defer writers.Done()
			<-start
			value, err := executor.Execute(command.Command{
				Name: "SET",
				Args: []string{"during:" + strconv.Itoa(key), "value"},
			})
			if err == nil && value.Type == resp.Error {
				err = errors.New(value.String)
			}
			writeErrors <- err
		}(i)
	}
	rewriteDone := make(chan error, 1)
	go func() {
		<-start
		_, err := executor.Execute(command.Command{Name: "AOFREWRITE"})
		rewriteDone <- err
	}()
	close(start)
	writers.Wait()
	close(writeErrors)
	for err := range writeErrors {
		if err != nil {
			t.Errorf("concurrent SET failed: %v", err)
		}
	}
	if err := <-rewriteDone; err != nil {
		t.Fatalf("AOFREWRITE during concurrent writes: %v", err)
	}
	if err := logFile.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := storage.NewStore()
	recoveredLog, err := Open(path, "no", recovered)
	if err != nil {
		t.Fatal(err)
	}
	defer recoveredLog.Close()
	recoveredExecutor := command.NewExecutor(recovered)
	for i := 0; i < 64; i++ {
		got := execute(t, recoveredExecutor, "GET", "during:"+strconv.Itoa(i))
		if got.Type != resp.BulkString || got.String != "value" {
			t.Errorf("concurrent key %d was lost: %+v", i, got)
		}
	}
}
