package storage

import (
	"fmt"
	"testing"
	"time"
)

// checkAccounting verifies the memory estimate and key index against data.
func checkAccounting(t *testing.T, s *Store) {
	t.Helper()
	var want int64
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key, obj := range sh.data {
			want += obj.memorySize(key)
			if _, ok := sh.all.pos[key]; !ok {
				t.Fatalf("key %q missing from all index", key)
			}
		}
		if sh.all.size() != len(sh.data) {
			t.Fatalf("shard %d: all index has %d keys, data has %d", i, sh.all.size(), len(sh.data))
		}
		sh.mu.RUnlock()
	}
	if got := s.UsedMemory(); got != want {
		t.Fatalf("UsedMemory = %d, recomputed %d", got, want)
	}
}

func TestUsedMemoryTracksStringsAndLists(t *testing.T) {
	s := NewStore()
	if s.UsedMemory() != 0 {
		t.Fatalf("empty store uses %d bytes", s.UsedMemory())
	}

	s.Set("a", "short", 0)
	small := s.UsedMemory()
	s.Set("a", "a much longer value than before", 0)
	if s.UsedMemory() <= small {
		t.Fatal("overwriting with a longer value should increase usage")
	}
	checkAccounting(t, s)

	if _, err := s.ListPush("l", []string{"x", "yy", "zzz"}, false, false); err != nil {
		t.Fatal(err)
	}
	checkAccounting(t, s)
	if err := s.ListSet("l", 0, "replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListInsert("l", "yy", "inserted", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListRem("l", 0, "zzz"); err != nil {
		t.Fatal(err)
	}
	if err := s.ListTrim("l", 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListPop("l", true, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ListMove("l", "l2", true, false); err != nil {
		t.Fatal(err)
	}
	checkAccounting(t, s)

	s.Del("a")
	s.Del("l")
	s.Del("l2")
	checkAccounting(t, s)
	if s.UsedMemory() != 0 {
		t.Fatalf("store with no keys uses %d bytes", s.UsedMemory())
	}
}

func TestUsedMemoryFollowsExpiry(t *testing.T) {
	s := NewStore()
	s.SetAt("gone", "v", time.Now().Add(20*time.Millisecond))
	s.Set("kept", "v", 0)
	time.Sleep(40 * time.Millisecond)

	s.ActiveExpireCycle()
	checkAccounting(t, s)
	if _, ok := s.getRawObj("gone"); ok {
		t.Fatal("expired key should be removed")
	}
}

func TestLRUVictimPrefersLeastRecentlyUsed(t *testing.T) {
	s := NewStore()
	for i := 0; i < lruSampleSize; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "v", 0)
		time.Sleep(time.Millisecond)
	}
	// With no more keys than the sample size, every key is always sampled.
	if victim, ok := s.LRUVictim(); !ok || victim != "key-0" {
		t.Fatalf("victim = %q, %v; want key-0", victim, ok)
	}

	s.Get("key-0") // a read makes key-0 the most recently used
	if victim, _ := s.LRUVictim(); victim != "key-1" {
		t.Fatalf("victim after read = %q, want key-1", victim)
	}
}

func TestLRUVictimOnEmptyStore(t *testing.T) {
	if victim, ok := NewStore().LRUVictim(); ok {
		t.Fatalf("empty store returned victim %q", victim)
	}
}
