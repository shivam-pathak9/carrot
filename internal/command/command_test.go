package command

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

type failedJournal struct{}

func (failedJournal) Append(Command) (Command, error) {
	return Command{}, errors.New("disk unavailable")
}

func TestMutationIsNotAppliedWhenJournalAppendFails(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)
	executor.SetJournal(failedJournal{})
	if _, err := executor.Execute(Command{Name: "SET", Args: []string{"key", "value"}}); err == nil {
		t.Fatal("write should fail when persistence append fails")
	}
	value, err := executor.Execute(Command{Name: "GET", Args: []string{"key"}})
	if err != nil {
		t.Fatal(err)
	}
	if value.Type != resp.BulkString || !value.IsNull {
		t.Fatalf("GET after failed SET = %+v, want null", value)
	}
}

type blockingRewriteJournal struct {
	mu       sync.Mutex
	events   []string
	started  chan struct{}
	release  chan struct{}
	appended chan struct{}
}

func (j *blockingRewriteJournal) Append(cmd Command) (Command, error) {
	j.mu.Lock()
	j.events = append(j.events, "append:"+cmd.Args[0])
	j.mu.Unlock()
	close(j.appended)
	return cmd, nil
}

func (j *blockingRewriteJournal) Rewrite(*storage.Store) error {
	close(j.started)
	<-j.release
	j.mu.Lock()
	j.events = append(j.events, "rewrite")
	j.mu.Unlock()
	return nil
}

func TestAOFRewriteBlocksMutationsUntilReplacementIsReady(t *testing.T) {
	journal := &blockingRewriteJournal{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		appended: make(chan struct{}),
	}
	executor := NewExecutor(storage.NewStore())
	executor.SetJournal(journal)

	rewriteDone := make(chan error, 1)
	go func() {
		_, err := executor.Execute(Command{Name: "AOFREWRITE"})
		rewriteDone <- err
	}()
	select {
	case <-journal.started:
	case <-time.After(time.Second):
		t.Fatal("rewrite did not start")
	}

	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		close(writeStarted)
		_, err := executor.Execute(Command{Name: "SET", Args: []string{"during-rewrite", "value"}})
		writeDone <- err
	}()
	<-writeStarted
	select {
	case <-journal.appended:
		t.Fatal("mutation was appended before rewrite finished")
	case <-time.After(20 * time.Millisecond):
	}

	close(journal.release)
	select {
	case err := <-rewriteDone:
		if err != nil {
			t.Fatalf("AOFREWRITE failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rewrite did not finish after release")
	}
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("mutation after rewrite failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mutation did not continue after rewrite")
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if len(journal.events) != 2 || journal.events[0] != "rewrite" || journal.events[1] != "append:during-rewrite" {
		t.Fatalf("journal order = %v, want [rewrite append:during-rewrite]", journal.events)
	}
}

func TestAOFRewriteReturnsJournalFailure(t *testing.T) {
	journal := failingRewriteJournal{}
	executor := NewExecutor(storage.NewStore())
	executor.SetJournal(journal)
	if _, err := executor.Execute(Command{Name: "AOFREWRITE"}); err == nil || err.Error() != "rewrite failed" {
		t.Fatalf("AOFREWRITE error = %v, want rewrite failed", err)
	}
}

type failingRewriteJournal struct{}

func (failingRewriteJournal) Append(cmd Command) (Command, error) { return cmd, nil }
func (failingRewriteJournal) Rewrite(*storage.Store) error        { return fmt.Errorf("rewrite failed") }

// TestNewParser tests parser creation
func TestNewParser(t *testing.T) {
	parser := NewParser()
	if parser == nil {
		t.Fatal("NewParser() should return a non-nil parser")
	}
}

// TestParsePingCommand tests parsing PING command
func TestParsePingCommand(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(resp.NewBulkString("PING"))

	cmd, err := parser.Parse(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cmd.Name != "PING" {
		t.Errorf("Expected command name 'PING', got '%s'", cmd.Name)
	}
	if len(cmd.Args) != 0 {
		t.Errorf("Expected 0 arguments, got %d", len(cmd.Args))
	}
}

// TestParsePingWithMessage tests parsing PING with message
func TestParsePingWithMessage(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(
		resp.NewBulkString("PING"),
		resp.NewBulkString("hello"),
	)

	cmd, err := parser.Parse(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cmd.Name != "PING" {
		t.Errorf("Expected command name 'PING', got '%s'", cmd.Name)
	}
	if len(cmd.Args) != 1 {
		t.Errorf("Expected 1 argument, got %d", len(cmd.Args))
	}
	if cmd.Args[0] != "hello" {
		t.Errorf("Expected argument 'hello', got '%s'", cmd.Args[0])
	}
}

// TestParseGetCommand tests parsing GET command
func TestParseGetCommand(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(
		resp.NewBulkString("GET"),
		resp.NewBulkString("mykey"),
	)

	cmd, err := parser.Parse(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cmd.Name != "GET" {
		t.Errorf("Expected command name 'GET', got '%s'", cmd.Name)
	}
	if len(cmd.Args) != 1 {
		t.Errorf("Expected 1 argument, got %d", len(cmd.Args))
	}
	if cmd.Args[0] != "mykey" {
		t.Errorf("Expected argument 'mykey', got '%s'", cmd.Args[0])
	}
}

// TestParseSetCommand tests parsing SET command
func TestParseSetCommand(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(
		resp.NewBulkString("SET"),
		resp.NewBulkString("key"),
		resp.NewBulkString("value"),
	)

	cmd, err := parser.Parse(value)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cmd.Name != "SET" {
		t.Errorf("Expected command name 'SET', got '%s'", cmd.Name)
	}
	if len(cmd.Args) != 2 {
		t.Errorf("Expected 2 arguments, got %d", len(cmd.Args))
	}
}

// TestParseCommandCaseInsensitive tests that command names are case-insensitive
func TestParseCommandCaseInsensitive(t *testing.T) {
	parser := NewParser()

	testCases := []struct {
		input    string
		expected string
	}{
		{"ping", "PING"},
		{"Ping", "PING"},
		{"PING", "PING"},
		{"get", "GET"},
		{"Get", "GET"},
		{"set", "SET"},
		{"Set", "SET"},
	}

	for _, tc := range testCases {
		value := resp.NewArray(resp.NewBulkString(tc.input))
		cmd, err := parser.Parse(value)
		if err != nil {
			t.Errorf("Unexpected error for '%s': %v", tc.input, err)
		}
		if cmd.Name != tc.expected {
			t.Errorf("For input '%s': expected '%s', got '%s'", tc.input, tc.expected, cmd.Name)
		}
	}
}

// TestParseNonArrayValue tests parsing fails when value is not an array
func TestParseNonArrayValue(t *testing.T) {
	parser := NewParser()
	value := resp.NewSimpleString("PING")

	_, err := parser.Parse(value)
	if err == nil {
		t.Error("Expected error for non-array value")
	}
}

// TestParseEmptyArray tests parsing fails with empty array
func TestParseEmptyArray(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray()

	_, err := parser.Parse(value)
	if err == nil {
		t.Error("Expected error for empty array")
	}
}

// TestParseNonBulkStringCommand tests parsing fails when command is not bulk string
func TestParseNonBulkStringCommand(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(resp.NewSimpleString("PING"))

	_, err := parser.Parse(value)
	if err == nil {
		t.Error("Expected error for non-bulk-string command")
	}
}

// TestParseNonBulkStringArgument tests parsing fails when argument is not bulk string
func TestParseNonBulkStringArgument(t *testing.T) {
	parser := NewParser()
	value := resp.NewArray(
		resp.NewBulkString("GET"),
		resp.NewInteger(42), // Integer instead of bulk string
	)

	_, err := parser.Parse(value)
	if err == nil {
		t.Error("Expected error for non-bulk-string argument")
	}
}

// TestNewExecutor tests executor creation
func TestNewExecutor(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)
	if executor == nil {
		t.Fatal("NewExecutor() should return a non-nil executor")
	}
}

// TestExecutePing tests PING command execution
func TestExecutePing(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "PING",
		Args: []string{},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.SimpleString {
		t.Errorf("Expected SimpleString, got %v", result.Type)
	}
	if result.String != "PONG" {
		t.Errorf("Expected 'PONG', got '%s'", result.String)
	}
}

// TestExecutePingWithMessage tests PING command with message
func TestExecutePingWithMessage(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "PING",
		Args: []string{"hello"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.BulkString {
		t.Errorf("Expected BulkString, got %v", result.Type)
	}
	if result.String != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result.String)
	}
}

// TestExecuteGet tests GET command
func TestExecuteGet(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	// Set a value first
	store.Set("name", "john", -1)

	cmd := Command{
		Name: "GET",
		Args: []string{"name"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.BulkString {
		t.Errorf("Expected BulkString, got %v", result.Type)
	}
	if result.String != "john" {
		t.Errorf("Expected 'john', got '%s'", result.String)
	}
}

// TestExecuteGetNonExistent tests GET for non-existent key
func TestExecuteGetNonExistent(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "GET",
		Args: []string{"nonexistent"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.BulkString {
		t.Errorf("Expected BulkString, got %v", result.Type)
	}
	if !result.IsNull {
		t.Error("Expected null bulk string for non-existent key")
	}
}

// TestExecuteSet tests SET command
func TestExecuteSet(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "SET",
		Args: []string{"key", "value"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.SimpleString {
		t.Errorf("Expected SimpleString, got %v", result.Type)
	}
	if result.String != "OK" {
		t.Errorf("Expected 'OK', got '%s'", result.String)
	}

	// Verify value was stored
	value, exists := store.Get("key")
	if !exists {
		t.Error("Expected key to be stored")
	}
	if value != "value" {
		t.Errorf("Expected 'value', got '%s'", value)
	}
}

// TestExecuteSetWithEX tests SET with EX option
func TestExecuteSetWithEX(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "SET",
		Args: []string{"key", "value", "EX", "100"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.SimpleString {
		t.Errorf("Expected SimpleString, got %v", result.Type)
	}

	// Verify TTL was set
	ttl := store.TTL("key")
	if ttl <= 0 || ttl > 100 {
		t.Errorf("Expected TTL between 1 and 100, got %d", ttl)
	}
}

// TestExecuteSetWithPX tests SET with PX option
func TestExecuteSetWithPX(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "SET",
		Args: []string{"key", "value", "PX", "5000"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.SimpleString {
		t.Errorf("Expected SimpleString, got %v", result.Type)
	}
}

// TestExecuteDel tests DEL command
func TestExecuteDel(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	store.Set("key1", "value1", -1)
	store.Set("key2", "value2", -1)

	cmd := Command{
		Name: "DEL",
		Args: []string{"key1", "key2"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.Integer {
		t.Errorf("Expected Integer, got %v", result.Type)
	}
	if result.Integer != 2 {
		t.Errorf("Expected 2 keys deleted, got %d", result.Integer)
	}

	// Verify keys were deleted
	_, exists := store.Get("key1")
	if exists {
		t.Error("Expected key1 to be deleted")
	}
}

// TestExecuteTTL tests TTL command
func TestExecuteTTL(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	store.Set("persistent", "value", -1)

	cmd := Command{
		Name: "TTL",
		Args: []string{"persistent"},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.Integer {
		t.Errorf("Expected Integer, got %v", result.Type)
	}
	if result.Integer != -1 {
		t.Errorf("Expected -1 for persistent key, got %d", result.Integer)
	}
}

// TestExecuteExpire tests EXPIRE command
func TestExecuteExpire(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	store.Set("key", "value", -1)

	cmd := Command{
		Name: "EXPIRE",
		Args: []string{"key", "60"},
	}
	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.Integer {
		t.Errorf("Expected Integer, got %v", result.Type)
	}
	if result.Integer != 1 {
		t.Errorf("Expected 1 for successful expire, got %d", result.Integer)
	}
}

func TestExpirationCommandEdgeCases(t *testing.T) {
	nowMillis := time.Now().UnixMilli()
	tests := []struct {
		name       string
		setup      func(*storage.Store)
		command    Command
		wantType   resp.Type
		wantValue  int64
		wantExists bool
	}{
		{
			name:      "TTL missing key",
			command:   Command{Name: "TTL", Args: []string{"missing"}},
			wantType:  resp.Integer,
			wantValue: -2,
		},
		{
			name: "TTL persistent key",
			setup: func(store *storage.Store) {
				store.Set("key", "value", 0)
			},
			command:    Command{Name: "TTL", Args: []string{"key"}},
			wantType:   resp.Integer,
			wantValue:  -1,
			wantExists: true,
		},
		{
			name:     "SET rejects zero expiration",
			command:  Command{Name: "SET", Args: []string{"key", "value", "EX", "0"}},
			wantType: resp.Error,
		},
		{
			name: "EXPIRE zero removes existing key",
			setup: func(store *storage.Store) {
				store.Set("key", "value", 0)
			},
			command:   Command{Name: "EXPIRE", Args: []string{"key", "0"}},
			wantType:  resp.Integer,
			wantValue: 1,
		},
		{
			name: "EXPIRE negative removes existing key",
			setup: func(store *storage.Store) {
				store.Set("key", "value", 0)
			},
			command:   Command{Name: "EXPIRE", Args: []string{"key", "-1"}},
			wantType:  resp.Integer,
			wantValue: 1,
		},
		{
			name: "PEXPIREAT past removes existing key",
			setup: func(store *storage.Store) {
				store.Set("key", "value", 0)
			},
			command:   Command{Name: "PEXPIREAT", Args: []string{"key", strconv.FormatInt(nowMillis-1000, 10)}},
			wantType:  resp.Integer,
			wantValue: 1,
		},
		{
			name:      "PEXPIREAT missing key",
			command:   Command{Name: "PEXPIREAT", Args: []string{"missing", strconv.FormatInt(nowMillis-1000, 10)}},
			wantType:  resp.Integer,
			wantValue: 0,
		},
		{
			name: "SET PXAT past leaves key absent",
			command: Command{
				Name: "SET",
				Args: []string{"key", "value", "PXAT", strconv.FormatInt(nowMillis-1000, 10)},
			},
			wantType: resp.SimpleString,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := storage.NewStore()
			if test.setup != nil {
				test.setup(store)
			}
			result, err := NewExecutor(store).Execute(test.command)
			if err != nil {
				t.Fatalf("Execute(%s): %v", test.command.Name, err)
			}
			if result.Type != test.wantType {
				t.Fatalf("response type = %v, want %v (%+v)", result.Type, test.wantType, result)
			}
			if test.wantType == resp.Integer && result.Integer != test.wantValue {
				t.Fatalf("response integer = %d, want %d", result.Integer, test.wantValue)
			}
			if test.wantExists {
				if _, exists := store.Get("key"); !exists {
					t.Fatal("key unexpectedly absent")
				}
				return
			}
			if test.command.Name == "TTL" && test.command.Args[0] == "missing" {
				return
			}
			if _, exists := store.Get("key"); exists {
				t.Fatal("key unexpectedly exists after expiration command")
			}
		})
	}
}

// TestExecuteFullCommandFlow tests a complete flow of commands
func TestExecuteFullCommandFlow(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	// SET command
	setCmd := Command{Name: "SET", Args: []string{"user", "alice"}}
	result, _ := executor.Execute(setCmd)
	if result.String != "OK" {
		t.Error("SET failed")
	}

	// GET command
	getCmd := Command{Name: "GET", Args: []string{"user"}}
	result, _ = executor.Execute(getCmd)
	if result.String != "alice" {
		t.Error("GET failed")
	}

	// TTL command
	ttlCmd := Command{Name: "TTL", Args: []string{"user"}}
	result, _ = executor.Execute(ttlCmd)
	if result.Integer != -1 {
		t.Error("TTL for persistent key should be -1")
	}

	// DEL command
	delCmd := Command{Name: "DEL", Args: []string{"user"}}
	result, _ = executor.Execute(delCmd)
	if result.Integer != 1 {
		t.Error("DEL should return 1")
	}

	// GET after delete
	result, _ = executor.Execute(getCmd)
	if !result.IsNull {
		t.Error("GET after DEL should return null")
	}
}

// TestExecuteExpiredKeyHandling tests expired key handling
func TestExecuteExpiredKeyHandling(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	// Set with expiration
	setCmd := Command{Name: "SET", Args: []string{"temp", "value", "EX", "1"}}
	executor.Execute(setCmd)

	// Verify it exists
	getCmd := Command{Name: "GET", Args: []string{"temp"}}
	result, _ := executor.Execute(getCmd)
	if result.IsNull {
		t.Error("Key should exist before expiration")
	}

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Verify it's expired
	result, _ = executor.Execute(getCmd)
	if !result.IsNull {
		t.Error("Key should be expired")
	}
}

// TestExecuteUnknownCommand tests unknown command
func TestExecuteUnknownCommand(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	cmd := Command{
		Name: "UNKNOWN",
		Args: []string{},
	}

	result, err := executor.Execute(cmd)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.Type != resp.Error {
		t.Errorf("Expected Error, got %v", result.Type)
	}
}

// TestExecuteMultipleDeletes tests DEL with multiple keys
func TestExecuteMultipleDeletes(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	// Set multiple keys
	for i := 1; i <= 5; i++ {
		key := "key" + string(rune(48+i))
		store.Set(key, "value", -1)
	}

	// Delete some keys
	cmd := Command{
		Name: "DEL",
		Args: []string{"key1", "key2", "key3", "nonexistent"},
	}

	result, _ := executor.Execute(cmd)
	if result.Integer != 3 {
		t.Errorf("Expected 3 keys deleted, got %d", result.Integer)
	}
}

// TestExecuteSetOverwrite tests overwriting a key with SET
func TestExecuteSetOverwrite(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)

	// First SET
	cmd1 := Command{Name: "SET", Args: []string{"key", "value1"}}
	executor.Execute(cmd1)

	// Second SET to overwrite
	cmd2 := Command{Name: "SET", Args: []string{"key", "value2"}}
	executor.Execute(cmd2)

	// GET to verify
	getCmd := Command{Name: "GET", Args: []string{"key"}}
	result, _ := executor.Execute(getCmd)

	if result.String != "value2" {
		t.Errorf("Expected 'value2', got '%s'", result.String)
	}
}
