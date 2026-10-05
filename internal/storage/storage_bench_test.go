package storage

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func BenchmarkStoreParallelSetGet(b *testing.B) {
	const keyCount = 1 << 16
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("parallel-key-%d", i)
	}

	store := NewStore()
	var nextWorker atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := int(nextWorker.Add(1)-1) * 16
		index := 0
		for pb.Next() {
			key := keys[(base+index)&(keyCount-1)]
			store.Set(key, "value", 0)
			if _, _, err := store.GetString(key); err != nil {
				b.Errorf("GetString(%q): %v", key, err)
				return
			}
			index = (index + 1) & 15
		}
	})
}
