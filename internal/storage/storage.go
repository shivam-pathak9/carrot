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

// ValueKind identifies the Redis-style value type stored at a key.
type ValueKind uint8

const (
	StringKind ValueKind = iota
	ListKind
)

// Store is the in-memory typed key-value database used by Carrot.
//
// It keeps a single Go map guarded by a mutex. This is intentionally simple and safe for the
// current server design: all store access is synchronized, and the reactor path may also trigger
// background expiration sweeps while client handlers are running.
// Store is the shared, mutex-protected database used by command executors.
// Values are typed so a command can reject operations against the wrong kind.
type Store struct {
	mu   sync.RWMutex
	data map[string]Obj
}

// NewStore constructs and initializes a new Store instance.
func NewStore() *Store {
	return &Store{
		data: make(map[string]Obj),
	}
}

// Set stores a value under key and optionally assigns an expiration timestamp.
//
// A ttl <= 0 means the key is persistent and will not expire unless explicitly removed or
// overwritten by a later SET/DELETE operation.
func (s *Store) Set(key string, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	s.data[key] = Obj{
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
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, exists := s.data[key]
	if !exists {
		return -2
	}

	// Check passive expiration
	if !obj.ExpiresAt.IsZero() && time.Now().After(obj.ExpiresAt) {
		delete(s.data, key) // Passive deletion
		return -2
	}

	if obj.ExpiresAt.IsZero() {
		return -1
	}

	remaining := time.Until(obj.ExpiresAt).Seconds()
	if remaining < 0 {
		delete(s.data, key)
		return -2
	}

	return int64(remaining)
}

// Del removes key from the store and returns true only if the key was present and valid.
//
// If the key had already expired, it is treated as absent and removed lazily before returning false.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, exists := s.data[key]
	if !exists {
		return false
	}

	// Check passive expiration
	if !obj.ExpiresAt.IsZero() && time.Now().After(obj.ExpiresAt) {
		delete(s.data, key) // Passive deletion
		return false
	}

	delete(s.data, key)
	return true
}

// Expire updates the expiration of an existing key in seconds.
//
// A non-positive seconds value removes the key. If the key does not exist or is already expired,
// the call returns false.
func (s *Store) Expire(key string, seconds int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, exists := s.data[key]
	if !exists {
		return false
	}

	// Check if the key is already passively expired
	if !obj.ExpiresAt.IsZero() && time.Now().After(obj.ExpiresAt) {
		delete(s.data, key)
		return false
	}

	if seconds <= 0 {
		delete(s.data, key)
		return true
	}

	obj.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = obj
	return true
}

// ActiveExpireCycle does a bounded cleanup pass for expired volatile keys.
//
// This is a lightweight maintenance sweep that helps avoid stale keys accumulating in memory when
// they are never accessed again. It samples a small number of keys, removes those that are expired,
// and stops when the time budget or a low-density condition is reached.
//
// The goal is not a full database scan; it is a bounded opportunistic cleanup that keeps the event loop
// responsive while still reclaiming expired entries in the background.
func (s *Store) ActiveExpireCycle() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	const (
		sampleSize     = 20                    // Number of volatile keys sampled per iteration
		thresholdRatio = 0.25                  // 25% counter-limit threshold ratio (5 / 20)
		maxIterations  = 16                    // Maximum loop iterations per tick
		maxDuration    = 25 * time.Millisecond // Hard CPU latency cap per cycle
	)

	startTime := time.Now()
	totalDeleted := 0

	for iteration := 0; iteration < maxIterations; iteration++ {
		// Enforce time budget cap to protect client I/O latency,
		// including while iterating the map.
		if time.Since(startTime) >= maxDuration {
			break
		}

		expiredInSample := 0
		sampledCount := 0

		// Iterate over map entries to extract a random sample of keys with TTLs.
		// In Go, map iteration order is randomized by the runtime, providing natural pseudo-random sampling.
		for key, obj := range s.data {
			if time.Since(startTime) >= maxDuration {
				break
			}

			// Skip persistent keys (ExpiresAt is zero)
			if obj.ExpiresAt.IsZero() {
				continue
			}

			sampledCount++

			// Check if key is expired
			if time.Now().After(obj.ExpiresAt) {
				delete(s.data, key)
				expiredInSample++
				totalDeleted++
			}

			// Stop when sample size N = 20 is reached
			if sampledCount >= sampleSize {
				break
			}
		}

		// If no volatile keys were found in database, exit early
		if sampledCount == 0 {
			break
		}

		// Calculate sample expiration ratio p_hat
		ratio := float64(expiredInSample) / float64(sampledCount)

		// THE 25% COUNTER-LIMIT CHECK:
		// If ratio <= 25%, global expired key density is low enough that further sweeping
		// yields diminishing returns. Exit cycle until next tick.
		if ratio <= thresholdRatio {
			break
		}
	}

	if totalDeleted > 0 {
		log.Printf("[Active Expire] Cleaned %d expired key(s) from memory", totalDeleted)
	}

	return totalDeleted
}
