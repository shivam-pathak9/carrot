package command

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/shivam-pathak9/carrot/internal/config"
	"github.com/shivam-pathak9/carrot/internal/protocol/resp"
	"github.com/shivam-pathak9/carrot/internal/storage"
)

// fillStore adds count keys with 100-byte values. Tests then set the limit one
// byte below current usage, because the limit is checked before each command.
func fillStore(t *testing.T, e *Executor, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		run(t, e, "SET", fmt.Sprintf("key-%03d", i), strings.Repeat("x", 100))
	}
}

func run(t *testing.T, e *Executor, name string, args ...string) resp.Value {
	t.Helper()
	value, err := e.Execute(Command{Name: name, Args: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return value
}

func isOOM(v resp.Value) bool {
	return v.Type == resp.Error && strings.HasPrefix(v.String, "OOM")
}

func TestNoEvictionRejectsGrowthButAllowsShrinking(t *testing.T) {
	store := storage.NewStore()
	e := NewExecutor(store)
	fillStore(t, e, 10)
	e.SetMemoryLimit(store.UsedMemory()-1, config.PolicyNoEviction)

	for _, cmd := range [][]string{
		{"SET", "new", "v"},
		{"RPUSH", "list", "v"},
	} {
		if got := run(t, e, cmd[0], cmd[1:]...); !isOOM(got) {
			t.Fatalf("%s at the limit = %+v, want OOM", cmd[0], got)
		}
	}
	if _, ok := store.Get("new"); ok {
		t.Fatal("rejected SET must not be applied")
	}
	if got := run(t, e, "GET", "key-000"); got.String != strings.Repeat("x", 100) {
		t.Fatal("reads must keep working at the limit")
	}

	run(t, e, "DEL", "key-000")
	if got := run(t, e, "SET", "new", "v"); isOOM(got) {
		t.Fatal("SET should succeed after a DEL frees memory")
	}
}

func TestAllKeysLRUEvictsOldestToStayUnderLimit(t *testing.T) {
	store := storage.NewStore()
	e := NewExecutor(store)
	fillStore(t, e, 4) // fewer keys than the sample size, so sampling is exact
	e.SetMemoryLimit(store.UsedMemory()-1, config.PolicyAllKeysLRU)

	run(t, e, "GET", "key-000") // key-000 becomes most recently used
	if got := run(t, e, "SET", "key-new", strings.Repeat("x", 100)); isOOM(got) {
		t.Fatalf("SET should evict instead of failing, got %+v", got)
	}
	if _, ok := store.Get("key-001"); ok {
		t.Fatal("least recently used key-001 should have been evicted")
	}
	for _, key := range []string{"key-000", "key-new"} {
		if _, ok := store.Get(key); !ok {
			t.Fatalf("%s should have survived", key)
		}
	}
}

func TestEvictionIsJournaledAsDel(t *testing.T) {
	store := storage.NewStore()
	e := NewExecutor(store)
	fillStore(t, e, 3)
	journal := &recordingJournal{}
	e.SetJournal(journal)
	e.SetMemoryLimit(store.UsedMemory()-1, config.PolicyAllKeysLRU)

	run(t, e, "SET", "key-new", strings.Repeat("x", 100))
	names := journal.names()
	if len(names) != 2 || !strings.HasPrefix(names[0], "DEL key-") || names[1] != "SET key-new" {
		t.Fatalf("journal = %v, want an eviction DEL before the SET", names)
	}
}

func TestEvictionKeepsKeyWhenJournalFails(t *testing.T) {
	store := storage.NewStore()
	e := NewExecutor(store)
	fillStore(t, e, 3)
	e.SetJournal(&recordingJournal{err: errors.New("disk full")})
	e.SetMemoryLimit(store.UsedMemory()-1, config.PolicyAllKeysLRU)
	keysBefore := 3

	if _, err := e.Execute(Command{Name: "SET", Args: []string{"key-new", "v"}}); err == nil {
		t.Fatal("expected a persistence error")
	}
	for i := 0; i < keysBefore; i++ {
		if _, ok := store.Get(fmt.Sprintf("key-%03d", i)); !ok {
			t.Fatalf("key-%03d was evicted without a persisted DEL", i)
		}
	}
}

func TestBatchJournalRejectsOOMWithoutJournaling(t *testing.T) {
	store := storage.NewStore()
	e := NewExecutor(store)
	fillStore(t, e, 3)
	journal := &groupedTestJournal{}
	e.SetJournal(journal)
	e.SetMemoryLimit(store.UsedMemory()-1, config.PolicyNoEviction)

	if got := run(t, e, "SET", "new", "v"); !isOOM(got) {
		t.Fatalf("SET = %+v, want OOM", got)
	}
	if got := run(t, e, "DEL", "key-000"); got.Integer != 1 {
		t.Fatalf("DEL = %+v, want 1", got)
	}
	if got := run(t, e, "SET", "new", "v"); isOOM(got) {
		t.Fatal("SET should be accepted once memory is free")
	}
}

func TestNoLimitNeverRejects(t *testing.T) {
	e := NewExecutor(storage.NewStore())
	fillStore(t, e, 50)
	if got := run(t, e, "SET", "k", "v"); isOOM(got) {
		t.Fatal("unlimited executor rejected a write")
	}
}
