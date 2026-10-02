// Package storage provides synchronized in-memory values and expiration handling.
package storage

import (
	"log"
	"sync"
	"time"
)

// Obj represents a typed entry stored inside the in-memory storage engine.
type Obj struct {
	Value     string
	ExpiresAt time.Time
	Kind      ValueKind
	list      listValue
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
// 256 shards ensure high concurrency by dividing key access across 256 separate RWMutexes.
const numShards = 256

// shard represents an isolated partition of the storage engine containing its own RWMutex and map.
type shard struct {
	mu   sync.RWMutex
	data map[string]Obj
}

// Store is the in-memory typed key-value database used by Carrot.
//
// DESIGN DECISION: LOCK STRIPING / SHARDING FOR MULTI-CORE SCALABILITY
// ------------------------------------------------------------------
// Issue & Motivation:
// A single global sync.RWMutex guarding a single map[string]Obj creates severe lock contention
// under high concurrent read/write throughput on multi-core CPUs. Every concurrent operation
// (SET, GET, DEL, LPUSH, etc.) contends for the same single lock instance.
//
// Solution:
// Store partitions the key space into 256 independent shards (shards [256]shard).
// Each shard owns an independent RWMutex and map[string]Obj. Keys are mapped to shards using
// the FNV-1a hash algorithm: shardIndex = fnv32a(key) % 256.
//
// Benefits:
// 1. Concurrent operations targeting different keys execute in parallel across CPU cores without blocking.
// 2. Background Active Expiration sweeps lock single shards briefly instead of locking the entire database.
type Store struct {
	shards [numShards]shard
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

	remaining := time.Until(obj.ExpiresAt).Seconds()
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

	if seconds <= 0 {
		delete(sh.data, key)
		return true
	}

	obj.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	sh.data[key] = obj
	return true
}

// ActiveExpireCycle does a bounded cleanup pass for expired volatile keys.
//
// It iterates across shards, acquiring a lock on one shard at a time to sample volatile entries.
// This ensures background expiration sweeps never block client requests targeting other shards.
func (s *Store) ActiveExpireCycle() int {
	const (
		sampleSize     = 20                    // Number of volatile keys sampled per iteration
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

	if totalDeleted > 0 {
		log.Printf("[Active Expire] Cleaned %d expired key(s) from memory", totalDeleted)
	}

	return totalDeleted
}

// getRawObj returns the internal Obj stored at key (used for tests and internal assertions).
func (s *Store) getRawObj(key string) (Obj, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	obj, exists := sh.data[key]
	return obj, exists
}

// setRawObj directly sets an internal Obj for key (used for tests to seed expired state).
func (s *Store) setRawObj(key string, obj Obj) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.data[key] = obj
}
