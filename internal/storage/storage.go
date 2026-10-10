// Package storage provides synchronized in-memory values and expiration handling.
package storage

import (
	"log"
	"sync"
	"time"
)

const activeExpirationLogInterval = time.Minute

// Obj represents a typed entry stored inside the in-memory storage engine.
type Obj struct {
	Value     string
	ExpiresAt time.Time
	Kind      ValueKind
	list      listValue
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
type shard struct {
	mu   sync.RWMutex
	data map[string]Obj
}

// Store is the in-memory typed key-value database used by Carrot. Its shard
// locks allow independent storage calls on different shards to proceed
// concurrently. The command Executor uses an additional global mutation lock
// only when a journal is installed to preserve persistence ordering.
type Store struct {
	shards            [numShards]shard
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

	sh.data[key] = Obj{
		Value:     value,
		ExpiresAt: expiresAt,
		Kind:      StringKind,
	}
}

// SetAt stores a string with an absolute expiration deadline. A deadline that
// has already passed removes the key instead of creating a persistent value.
func (s *Store) SetAt(key, value string, expiresAt time.Time) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if !time.Now().Before(expiresAt) {
		delete(sh.data, key)
		return
	}
	sh.data[key] = Obj{
		Value:     value,
		ExpiresAt: expiresAt,
		Kind:      StringKind,
	}
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
		delete(sh.data, key) // Passive deletion
		return -2
	}

	if obj.ExpiresAt.IsZero() {
		return -1
	}

	remaining := obj.ExpiresAt.Sub(now).Seconds()
	if remaining < 0 {
		delete(sh.data, key)
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
		delete(sh.data, key) // Passive deletion
		return false
	}

	delete(sh.data, key)
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
		delete(sh.data, key)
		return false
	}

	if !now.Before(expiresAt) {
		delete(sh.data, key)
		return true
	}

	obj.ExpiresAt = expiresAt
	sh.data[key] = obj
	return true
}

// ActiveExpireCycle does a bounded cleanup pass for expired volatile keys.
// It visits Go map entries in iteration order, taking one shard lock at a
// time. Go map iteration order is unspecified; this is a bounded scan sample,
// not a uniform random sample. The time limit is cooperative rather than a
// hard upper bound on elapsed time. Cleanup counts are logged at most once per
// minute per Store, with counts aggregated between reports.
func (s *Store) ActiveExpireCycle() int {
	const (
		sampleSize     = 20                    // Maximum volatile keys visited per iteration
		thresholdRatio = 0.25                  // 25% counter-limit threshold ratio (5 / 20)
		maxIterations  = 16                    // Maximum loop iterations per tick
		maxDuration    = 25 * time.Millisecond // Hard CPU latency cap per cycle
	)

	startTime := time.Now()
	totalDeleted := 0

	for iteration := 0; iteration < maxIterations; iteration++ {
		if time.Since(startTime) >= maxDuration {
			break
		}

		expiredInSample := 0
		sampledCount := 0
		now := time.Now()

		for shIdx := 0; shIdx < numShards; shIdx++ {
			if time.Since(startTime) >= maxDuration {
				break
			}
			sh := &s.shards[shIdx]

			sh.mu.Lock()
			for key, obj := range sh.data {
				if time.Since(startTime) >= maxDuration {
					break
				}

				if obj.ExpiresAt.IsZero() {
					continue
				}

				sampledCount++

				if obj.IsExpired(now) {
					delete(sh.data, key)
					expiredInSample++
					totalDeleted++
				}

				if sampledCount >= sampleSize {
					break
				}
			}
			sh.mu.Unlock()

			if sampledCount >= sampleSize {
				break
			}
		}

		if sampledCount == 0 {
			break
		}

		ratio := float64(expiredInSample) / float64(sampledCount)
		if ratio <= thresholdRatio {
			break
		}
	}

	s.reportActiveExpiration(totalDeleted)
	return totalDeleted
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
