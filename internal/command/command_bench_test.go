package command

import (
	"fmt"
	"testing"

	"github.com/shivam-pathak9/carrot/internal/storage"
)

func BenchmarkExecutorSET(b *testing.B) {
	executor := NewExecutor(storage.NewStore())
	cmd := Command{Name: "SET", Args: []string{"bench-key", "value"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executor.Execute(cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecutorGET(b *testing.B) {
	store := storage.NewStore()
	store.Set("bench-key", "value", 0)
	executor := NewExecutor(store)
	cmd := Command{Name: "GET", Args: []string{"bench-key"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executor.Execute(cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListPushAndPop(b *testing.B) {
	executor := NewExecutor(storage.NewStore())
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-list-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if _, err := executor.Execute(Command{Name: "RPUSH", Args: []string{key, "value"}}); err != nil {
			b.Fatal(err)
		}
		if _, err := executor.Execute(Command{Name: "LPOP", Args: []string{key}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListRPUSH(b *testing.B) {
	store := storage.NewStore()
	executor := NewExecutor(store)
	keys := make([]string, 128)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-rpush-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if i >= len(keys) && i%len(keys) == 0 {
			if _, err := executor.Execute(Command{Name: "DEL", Args: []string{key}}); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := executor.Execute(Command{Name: "RPUSH", Args: []string{key, "value"}}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListRange(b *testing.B) {
	store := storage.NewStore()
	values := make([]string, 128)
	for i := range values {
		values[i] = "value"
	}
	if _, err := store.ListPush("bench-list", values, false, false); err != nil {
		b.Fatal(err)
	}
	executor := NewExecutor(store)
	cmd := Command{Name: "LRANGE", Args: []string{"bench-list", "0", "-1"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := executor.Execute(cmd); err != nil {
			b.Fatal(err)
		}
	}
}
