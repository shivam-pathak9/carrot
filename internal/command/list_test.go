package command

import (
	"reflect"
	"testing"

	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

func executeListTestCommand(t *testing.T, executor *Executor, name string, args ...string) resp.Value {
	t.Helper()
	value, err := executor.Execute(Command{Name: name, Args: args})
	if err != nil {
		t.Fatalf("%s returned Go error: %v", name, err)
	}
	return value
}

func responseStrings(t *testing.T, value resp.Value) []string {
	t.Helper()
	if value.Type != resp.Array {
		t.Fatalf("response type = %v, want array", value.Type)
	}
	result := make([]string, len(value.Array))
	for i, item := range value.Array {
		if item.Type != resp.BulkString {
			t.Fatalf("array item %d type = %v, want bulk string", i, item.Type)
		}
		result[i] = item.String
	}
	return result
}

func TestListCommandFlow(t *testing.T) {
	executor := NewExecutor(storage.NewStore())

	if got := executeListTestCommand(t, executor, "LPUSH", "items", "a", "b"); got.Type != resp.Integer || got.Integer != 2 {
		t.Fatalf("LPUSH response = %+v, want integer 2", got)
	}
	if got := executeListTestCommand(t, executor, "RPUSH", "items", "c", "d"); got.Integer != 4 {
		t.Fatalf("RPUSH response = %+v, want integer 4", got)
	}
	if got := executeListTestCommand(t, executor, "LRANGE", "items", "0", "-1"); !reflect.DeepEqual(responseStrings(t, got), []string{"b", "a", "c", "d"}) {
		t.Fatalf("LRANGE response = %+v", got)
	}
	if got := executeListTestCommand(t, executor, "LPUSHX", "missing", "x"); got.Integer != 0 {
		t.Fatalf("LPUSHX missing response = %+v, want integer 0", got)
	}
	if got := executeListTestCommand(t, executor, "RPUSHX", "items", "e"); got.Integer != 5 {
		t.Fatalf("RPUSHX response = %+v, want integer 5", got)
	}
	if got := executeListTestCommand(t, executor, "LINSERT", "items", "AFTER", "c", "inserted"); got.Integer != 6 {
		t.Fatalf("LINSERT response = %+v, want integer 6", got)
	}
	if got := executeListTestCommand(t, executor, "LINDEX", "items", "-1"); got.String != "e" {
		t.Fatalf("LINDEX response = %+v, want e", got)
	}
	if got := executeListTestCommand(t, executor, "LPOS", "items", "inserted"); got.Type != resp.Integer || got.Integer != 3 {
		t.Fatalf("LPOS response = %+v, want integer 3", got)
	}
	if got := executeListTestCommand(t, executor, "LPOS", "items", "missing", "COUNT", "2"); got.Type != resp.Array || len(got.Array) != 0 {
		t.Fatalf("LPOS COUNT response = %+v, want empty array", got)
	}
	if got := executeListTestCommand(t, executor, "LSET", "items", "0", "head"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("LSET response = %+v, want OK", got)
	}
	if got := executeListTestCommand(t, executor, "LREM", "items", "0", "a"); got.Type != resp.Integer || got.Integer != 1 {
		t.Fatalf("LREM response = %+v, want integer 1", got)
	}
	if got := executeListTestCommand(t, executor, "LTRIM", "items", "1", "-1"); got.Type != resp.SimpleString || got.String != "OK" {
		t.Fatalf("LTRIM response = %+v, want OK", got)
	}
	if got := executeListTestCommand(t, executor, "LMOVE", "items", "moved", "LEFT", "RIGHT"); got.Type != resp.BulkString || got.String != "c" {
		t.Fatalf("LMOVE response = %+v, want c", got)
	}
	if got := executeListTestCommand(t, executor, "RPOPLPUSH", "items", "moved"); got.Type != resp.BulkString || got.String != "e" {
		t.Fatalf("RPOPLPUSH response = %+v, want e", got)
	}
	if got := executeListTestCommand(t, executor, "LPOP", "moved"); got.Type != resp.BulkString || got.String != "e" {
		t.Fatalf("LPOP response = %+v, want e", got)
	}
	if got := executeListTestCommand(t, executor, "RPOP", "moved", "5"); !reflect.DeepEqual(responseStrings(t, got), []string{"c"}) {
		t.Fatalf("RPOP count response = %+v, want [c]", got)
	}
	if got := executeListTestCommand(t, executor, "LLEN", "moved"); got.Type != resp.Integer || got.Integer != 0 {
		t.Fatalf("LLEN empty response = %+v, want integer 0", got)
	}
	if got := executeListTestCommand(t, executor, "LPOP", "moved"); got.Type != resp.BulkString || !got.IsNull {
		t.Fatalf("LPOP empty response = %+v, want null bulk string", got)
	}
	if got := executeListTestCommand(t, executor, "LPOP", "moved", "0"); got.Type != resp.Array || len(got.Array) != 0 {
		t.Fatalf("LPOP count zero response = %+v, want empty array", got)
	}
}

func TestListCommandErrorsAndWrongType(t *testing.T) {
	store := storage.NewStore()
	executor := NewExecutor(store)
	store.Set("text", "value", -1)

	wrongTypeCommands := []Command{
		{Name: "LPUSH", Args: []string{"text", "item"}},
		{Name: "LLEN", Args: []string{"text"}},
		{Name: "LPOP", Args: []string{"text"}},
		{Name: "LRANGE", Args: []string{"text", "0", "-1"}},
		{Name: "LMOVE", Args: []string{"text", "destination", "LEFT", "RIGHT"}},
		{Name: "GET", Args: []string{"list"}},
	}
	_, _ = store.ListPush("list", []string{"item"}, false, false)
	for _, command := range wrongTypeCommands {
		result, err := executor.Execute(command)
		if err != nil {
			t.Fatalf("%s returned Go error: %v", command.Name, err)
		}
		if result.Type != resp.Error || result.String != storage.ErrWrongType.Error() {
			t.Errorf("%s response = %+v, want WRONGTYPE", command.Name, result)
		}
	}

	for _, command := range []Command{
		{Name: "LPOP", Args: []string{"key", "-1"}},
		{Name: "LPOP", Args: []string{"key", "not-a-number"}},
		{Name: "LINDEX", Args: []string{"key", "invalid"}},
		{Name: "LMOVE", Args: []string{"key", "other", "UP", "LEFT"}},
		{Name: "LPOS", Args: []string{"key", "x", "RANK", "0"}},
		{Name: "LPOS", Args: []string{"key", "x", "COUNT", "-1"}},
	} {
		result, err := executor.Execute(command)
		if err != nil {
			t.Fatalf("%s returned Go error: %v", command.Name, err)
		}
		if result.Type != resp.Error {
			t.Errorf("%s %v response = %+v, want error", command.Name, command.Args, result)
		}
	}

	missingIndex := executeListTestCommand(t, executor, "LSET", "absent", "0", "x")
	if missingIndex.Type != resp.Error || missingIndex.String != "ERR no such key" {
		t.Errorf("LSET missing response = %+v, want no such key error", missingIndex)
	}
}
