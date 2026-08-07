package command

import (
	"testing"
	"time"

	"github.com/shivampathak/carrot/internal/protocol/resp"
	"github.com/shivampathak/carrot/internal/storage"
)

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
