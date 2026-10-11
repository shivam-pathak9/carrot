// memory.go estimates memory use and selects LRU eviction victims.
package storage

import (
	"math"
	"math/rand/v2"
)

const (
	// entryOverhead approximates the fixed per-key cost: the Obj struct, its
	// map slot, and the key index entries. It is an estimate, not a measurement.
	entryOverhead = 192
	// stringHeaderBytes is the size of one string header in a list buffer.
	stringHeaderBytes = 16
	// lruSampleSize is how many random keys are compared per eviction, like
	// Redis's default maxmemory-samples.
	lruSampleSize = 5
)

// memorySize estimates the heap bytes used by the entry stored under key.
func (o Obj) memorySize(key string) int64 {
	return entryOverhead + int64(len(key)+len(o.Value)) + o.list.memorySize()
}

func (l *listValue) memorySize() int64 {
	return int64(len(l.buffer))*stringHeaderBytes + int64(l.bytes)
}

// UsedMemory returns the estimated bytes held by all keys and values. It
// excludes connection buffers, the AOF, and Go runtime overhead.
func (s *Store) UsedMemory() int64 {
	return s.usedBytes.Load()
}

// LRUVictim returns the least recently used key among a small random sample,
// or false if the store is empty. Like Redis, this approximates LRU: a
// sampled key is never guaranteed to be the globally oldest.
func (s *Store) LRUVictim() (string, bool) {
	var victim string
	oldest := int64(math.MaxInt64)
	found := false
	sampled := 0

	start := rand.IntN(numShards)
	for i := 0; i < numShards && sampled < lruSampleSize; i++ {
		sh := &s.shards[(start+i)%numShards]
		sh.mu.RLock()
		if n := sh.all.size(); n > 0 {
			key := sh.all.at(rand.IntN(n))
			if access := sh.data[key].lastAccess; !found || access < oldest {
				victim, oldest, found = key, access, true
			}
			sampled++
		}
		sh.mu.RUnlock()
	}
	return victim, found
}

// keyIndex is a set of keys with O(1) add, remove, and positional access.
// Removal swaps the last key into the freed slot, so order is not preserved.
type keyIndex struct {
	keys []string
	pos  map[string]int
}

func (ix *keyIndex) size() int { return len(ix.keys) }

func (ix *keyIndex) at(i int) string { return ix.keys[i] }

func (ix *keyIndex) add(key string) {
	if ix.pos == nil {
		ix.pos = make(map[string]int)
	}
	if _, exists := ix.pos[key]; exists {
		return
	}
	ix.pos[key] = len(ix.keys)
	ix.keys = append(ix.keys, key)
}

func (ix *keyIndex) remove(key string) {
	i, exists := ix.pos[key]
	if !exists {
		return
	}
	last := len(ix.keys) - 1
	moved := ix.keys[last]
	ix.keys[i] = moved
	ix.pos[moved] = i
	ix.keys = ix.keys[:last]
	delete(ix.pos, key)
}
