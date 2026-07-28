package storage

import (
	"sync"
	"time"
)

// Obj represents an entry stored inside the in-memory storage engine.
//
// Struct Fields:
//   - Value    : The string payload associated with the key.
//   - ExpiresAt: Expiration timestamp. A zero value (time.Time{}) indicates the key does not expire (persistent key).
type Obj struct {
	Value     string
	ExpiresAt time.Time
}

// Store is an in-memory key-value data store supporting optional key expiration (TTL).
//
// Thread-Safety & Design:
//   - Uses an internal map (`data`) to hold key-value objects.
//   - Guarded by a RWMutex to ensure thread-safety across concurrent reads/writes if needed,
//     while being lightweight enough for single-threaded reactor execution.
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

// Set stores a key-value pair in memory with an optional TTL (time-to-live duration).
// If ttl <= 0, the key is treated as persistent with no expiration.
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

// Get retrieves the value associated with key.
// Returns (value, true) if found and not expired.
// If the key is expired, it is lazily deleted on access and returns ("", false).
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	obj, exists := s.data[key]
	if !exists {
		return "", false
	}

	// Check passive expiration
	if !obj.ExpiresAt.IsZero() && time.Now().After(obj.ExpiresAt) {
		delete(s.data, key) // Passive deletion
		return "", false
	}

	return obj.Value, true
}

// TTL calculates the remaining Time-To-Live duration in seconds for a given key.
//
// Return values matching Redis TTL specification:
//   - -2: Key does not exist or has already expired.
//   - -1: Key exists but has no expiration set (persistent key).
//   - >0: Remaining TTL in seconds.
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

// Del removes a key from the storage engine.
// Returns true if key existed and was deleted, false otherwise.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		return true
	}
	return false
}
