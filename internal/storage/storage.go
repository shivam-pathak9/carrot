// Package storage provides synchronized in-memory values and expiration handling.
package storage

import (
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const activeExpirationLogInterval = time.Minute

// Obj represents a typed entry stored inside the in-memory storage engine.
type Obj struct {
	Value     string
	ExpiresAt time.Time
	Kind      ValueKind
	list      listValue

	lastAccess int64 // Unix nanoseconds of the last read or write, for LRU eviction
}

// SnapshotEntry is a stable copy of one live key returned during an AOF
// rewrite. List values are copied so callers can encode them after the shard
// lock has been released.
type SnapshotEntry struct {
	Key       string
	Value     string
	ExpiresAt time.Time
	Kind      ValueKind
	List      []string
}

// IsExpired reports whether the object has an expiration timestamp set and is expired as of time `now`.
func (o Obj) IsExpired(now time.Time) bool {
	return !o.ExpiresAt.IsZero() && !now.Before(o.ExpiresAt)
}

// ValueKind identifies the Redis-style value type stored at a key.
type ValueKind uint8

const (
	StringKind ValueKind = iota
	ListKind
)

// numShards specifies the number of independent memory partitions in the store.
// 256 is a fixed striping choice intended to reduce lock contention; it has not
// been selected through a workload-specific shard-count benchmark.
const numShards = 256

// shard represents an isolated partition of the storage engine containing its own RWMutex and map.
// Every write goes through put and remove so the key indexes and the
// store-wide memory estimate stay consistent with data.
type shard struct {
	mu   sync.RWMutex
	data map[string]Obj

	// all holds exactly the keys in data and supports O(1) random sampling for
	// eviction.
	all keyIndex
	// volatile holds keys that may carry an expiration for active expiry. It
	// can contain stale entries (a key overwritten as persistent); sampling
	// discards them.
	volatile keyIndex

	used *atomic.Int64 // store-wide estimated bytes, shared by all shards
}

// put stores obj under key, stamps it as just accessed, and updates the memory
// estimate. The caller holds sh.mu.
func (sh *shard) put(key string, obj Obj) {
	obj.lastAccess = time.Now().UnixNano()
	delta := obj.memorySize(key)
	if old, exists := sh.data[key]; exists {
		delta -= old.memorySize(key)
	} else {
		sh.all.add(key)
	}
	sh.data[key] = obj
	sh.used.Add(delta)
}

// touch records a read of obj for LRU eviction without changing its size.
// The caller holds sh.mu for writing.
func (sh *shard) touch(key string, obj Obj, now time.Time) Obj {
	obj.lastAccess = now.UnixNano()
	sh.data[key] = obj
	return obj
}

// remove deletes key from the map and both indexes. The caller holds sh.mu.
func (sh *shard) remove(key string) {
	if old, exists := sh.data[key]; exists {
		sh.used.Add(-old.memorySize(key))
		delete(sh.data, key)
		sh.all.remove(key)
	}
	sh.volatile.remove(key)
}

// Store is the in-memory typed key-value database used by Carrot. Its shard
// locks allow independent storage calls on different shards to proceed
// concurrently. The command Executor uses an additional global mutation lock
// only when a journal is installed to preserve persistence ordering.
type Store struct {
	shards            [numShards]shard
	usedBytes         atomic.Int64
	expirationLogMu   sync.Mutex
	lastExpirationLog time.Time
	pendingExpired    int
}

// fnv32a hashes a string key into a 32-bit unsigned integer using FNV-1a.
func fnv32a(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := uint32(offset32)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}

// getShardIndex returns the index of the shard responsible for key.
func (s *Store) getShardIndex(key string) int {
	return int(fnv32a(key) % numShards)
}

// getShard returns a pointer to the shard responsible for key.
func (s *Store) getShard(key string) *shard {
	return &s.shards[s.getShardIndex(key)]
}

// NewStore constructs and initializes a new sharded Store instance.
func NewStore() *Store {
	s := &Store{}
	for i := 0; i < numShards; i++ {
		s.shards[i].data = make(map[string]Obj)
		s.shards[i].used = &s.usedBytes
	}
	return s
}

// Set stores a value under key and optionally assigns an expiration timestamp.
//
// A ttl <= 0 means the key is persistent and will not expire unless explicitly removed or
// overwritten by a later SET/DELETE operation.
func (s *Store) Set(key string, value string, ttl time.Duration) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	sh.put(key, Obj{
		Value:     value,
		ExpiresAt: expiresAt,
		Kind:      StringKind,
	})
	if expiresAt.IsZero() {
		sh.volatile.remove(key)
	} else {
		sh.volatile.add(key)
	}
}

// SetAt stores a string with an absolute expiration deadline. A deadline that
// has already passed removes the key instead of creating a persistent value.
func (s *Store) SetAt(key, value string, expiresAt time.Time) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if !time.Now().Before(expiresAt) {
		sh.remove(key)
		return
	}
	sh.put(key, Obj{
		Value:     value,
		ExpiresAt: expiresAt,
		Kind:      StringKind,
	})
	sh.volatile.add(key)
}

// Get returns the stored value for key if it still exists and has not expired.
//
// Expired keys are removed lazily on access, which matches the common Redis pattern for
// passive expiration handling.
func (s *Store) Get(key string) (string, bool) {
	value, exists, err := s.GetString(key)
	return value, exists && err == nil
}

// TTL returns the remaining lifetime of key in seconds using Redis-style semantics.
//
// Values are:
//   - -2: key does not exist or has already expired,
//   - -1: key exists without an expiration,
//   - >=0: seconds remaining before expiry.
func (s *Store) TTL(key string) int64 {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := sh.data[key]
	if !exists {
		return -2
	}

	now := time.Now()
	// Check passive expiration
	if obj.IsExpired(now) {
		sh.remove(key) // Passive deletion
		return -2
	}

	if obj.ExpiresAt.IsZero() {
		return -1
	}

	remaining := obj.ExpiresAt.Sub(now).Seconds()
	if remaining < 0 {
		sh.remove(key)
		return -2
	}

	return int64(remaining)
}

// Del removes key from the store and returns true only if the key was present and valid.
//
// If the key had already expired, it is treated as absent and removed lazily before returning false.
func (s *Store) Del(key string) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := sh.data[key]
	if !exists {
		return false
	}

	now := time.Now()
	// Check passive expiration
	if obj.IsExpired(now) {
		sh.remove(key) // Passive deletion
		return false
	}

	sh.remove(key)
	return true
}

// Expire updates the expiration of an existing key in seconds.
//
// A non-positive seconds value removes the key. If the key does not exist or is already expired,
// the call returns false.
func (s *Store) Expire(key string, seconds int64) bool {
	deadline := time.Now()
	if seconds > 0 {
		deadline = deadline.Add(time.Duration(seconds) * time.Second)
	}
	return s.ExpireAt(key, deadline)
}

// ExpireAt assigns an absolute expiration deadline to an existing key.
// A deadline in the past deletes the key and still reports success.
func (s *Store) ExpireAt(key string, expiresAt time.Time) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := sh.data[key]
	if !exists {
		return false
	}

	now := time.Now()
	// Check if the key is already passively expired
	if obj.IsExpired(now) {
		sh.remove(key)
		return false
	}

	if !now.Before(expiresAt) {
		sh.remove(key)
		return true
	}

	obj.ExpiresAt = expiresAt
	sh.put(key, obj)
	sh.volatile.add(key)
	return true
}

// ActiveExpireCycle runs a bounded Redis-style expiry pass and deletes the
// expired keys it finds. See ActiveExpireCycleWith.
func (s *Store) ActiveExpireCycle() int {
	return s.ActiveExpireCycleWith(nil)
}

// ActiveExpireCycleWith samples volatile keys from the per-shard TTL index,
// starting at a random shard and random offset so no key is systematically
// favored, and removes the expired ones. It repeats while more than 25% of a
// sample was expired, for at most 16 iterations or about 25ms (checked between
// iterations).
//
// If remove is nil, expired keys are deleted directly. Otherwise remove is
// handed each batch of expired candidates and must delete them (typically
// after journaling) and return how many it deleted. Candidates may be
// re-checked by DeleteExpired, which only deletes keys that are still expired.
// A remove error ends the cycle.
func (s *Store) ActiveExpireCycleWith(remove func(keys []string) (int, error)) int {
	const (
		sampleSize     = 20
		thresholdRatio = 0.25
		maxIterations  = 16
		maxDuration    = 25 * time.Millisecond
	)

	startTime := time.Now()
	totalDeleted := 0

	for iteration := 0; iteration < maxIterations; iteration++ {
		if time.Since(startTime) >= maxDuration {
			break
		}

		sampled, expired := s.sampleExpired(sampleSize)
		if sampled == 0 {
			break
		}

		if len(expired) > 0 {
			if remove == nil {
				totalDeleted += s.DeleteExpired(expired)
			} else {
				deleted, err := remove(expired)
				totalDeleted += deleted
				if err != nil {
					log.Printf("[Active Expire] stopping cycle: %v", err)
					break
				}
			}
		}

		if float64(len(expired))/float64(sampled) <= thresholdRatio {
			break
		}
	}

	s.reportActiveExpiration(totalDeleted)
	return totalDeleted
}

// sampleExpired visits up to limit tracked volatile keys and returns how many
// were sampled and which of them are expired. It does not delete expired keys.
// Stale index entries are dropped and not counted as samples.
func (s *Store) sampleExpired(limit int) (int, []string) {
	const perShard = 5

	now := time.Now()
	start := rand.IntN(numShards)
	sampled := 0
	var expired []string

	for i := 0; i < numShards && sampled < limit; i++ {
		sh := &s.shards[(start+i)%numShards]
		sh.mu.Lock()
		n := sh.volatile.size()
		if n == 0 {
			sh.mu.Unlock()
			continue
		}
		offset := rand.IntN(n)
		take := min(n, perShard, limit-sampled)
		var stale []string
		for j := 0; j < take; j++ {
			key := sh.volatile.at((offset + j) % n)
			obj, exists := sh.data[key]
			if !exists || obj.ExpiresAt.IsZero() {
				stale = append(stale, key)
				continue
			}
			sampled++
			if obj.IsExpired(now) {
				expired = append(expired, key)
			}
		}
		for _, key := range stale {
			sh.volatile.remove(key)
		}
		sh.mu.Unlock()
	}
	return sampled, expired
}

// FilterExpired returns the keys from candidates that currently exist and are
// expired.
func (s *Store) FilterExpired(candidates []string) []string {
	now := time.Now()
	var out []string
	for _, key := range candidates {
		sh := s.getShard(key)
		sh.mu.RLock()
		obj, exists := sh.data[key]
		sh.mu.RUnlock()
		if exists && obj.IsExpired(now) {
			out = append(out, key)
		}
	}
	return out
}

// DeleteExpired deletes each candidate that is still expired and returns the
// number deleted. A candidate that was overwritten with a live value since it
// was sampled is left untouched.
func (s *Store) DeleteExpired(candidates []string) int {
	now := time.Now()
	deleted := 0
	for _, key := range candidates {
		sh := s.getShard(key)
		sh.mu.Lock()
		if obj, exists := sh.data[key]; exists && obj.IsExpired(now) {
			sh.remove(key)
			deleted++
		}
		sh.mu.Unlock()
	}
	return deleted
}

func (s *Store) reportActiveExpiration(deleted int) {
	s.expirationLogMu.Lock()
	s.pendingExpired += deleted
	now := time.Now()
	if s.pendingExpired == 0 ||
		(!s.lastExpirationLog.IsZero() && now.Sub(s.lastExpirationLog) < activeExpirationLogInterval) {
		s.expirationLogMu.Unlock()
		return
	}
	reported := s.pendingExpired
	s.pendingExpired = 0
	s.lastExpirationLog = now
	s.expirationLogMu.Unlock()

	log.Printf("[Active Expire] Cleaned %d expired key(s) since last report", reported)
}

// ForEachSnapshot visits every currently live key without copying the entire
// database at once. It stores only one shard's key names and copies one value
// (including that key's list, if any) at a time. The callback runs after the
// shard lock is released so slow disk writes do not hold storage locks. The
// caller must prevent normal writes while taking the snapshot if it needs a
// point-in-time view across the whole database.
func (s *Store) ForEachSnapshot(visit func(SnapshotEntry) error) error {
	now := time.Now()
	for shardIndex := range s.shards {
		sh := &s.shards[shardIndex]
		keys := make([]string, 0)
		sh.mu.RLock()
		for key := range sh.data {
			keys = append(keys, key)
		}
		sh.mu.RUnlock()

		for _, key := range keys {
			sh.mu.RLock()
			obj, exists := sh.data[key]
			if !exists || obj.IsExpired(now) {
				sh.mu.RUnlock()
				continue
			}
			entry := SnapshotEntry{
				Key:       key,
				Value:     obj.Value,
				ExpiresAt: obj.ExpiresAt,
				Kind:      obj.Kind,
			}
			if obj.Kind == ListKind {
				entry.List = obj.list.rangeCopy(0, obj.list.Len())
			}
			sh.mu.RUnlock()

			if err := visit(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// SnapshotEntries copies all current live values for point-in-time operations
// that need to continue after releasing the caller's mutation barrier.
func (s *Store) SnapshotEntries() ([]SnapshotEntry, error) {
	entries := make([]SnapshotEntry, 0)
	err := s.ForEachSnapshot(func(entry SnapshotEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}
