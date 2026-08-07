package storage

import (
	"log"
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
// If the key was passively expired, it is removed and false is returned.
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

// Expire sets a TTL (time-to-live) in seconds on an existing key.
//
// Returns:
//   - true  if the timeout was set or key was deleted due to non-positive seconds.
//   - false if the key does not exist or has already expired.
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

// ActiveExpireCycle performs proactive, background memory reclamation of expired keys.
//
//              THE 25% STATISTICAL COUNTER-LIMIT THEOREM & PROBABILISTIC SAMPLING
// 
//
// 1. THE PROBLEM WITH PASSIVE DELETION ALONE:
//    Passive (lazy) expiration only deletes a key when a client explicitly queries it via GET,
//    TTL, or DEL. If millions of keys are written with a TTL and never requested again, they
//    would sit in memory indefinitely, causing a memory leak.
//
// 2. WHY NOT SCAN ALL KEYS?
//    Iterating through a database of 10,000,000 keys every 100ms would require O(N) linear scan,
//    freezing the server, causing massive latency spikes, and thrashing CPU caches.
//
// 3. THE 25% PROBABILISTIC COUNTER-LIMIT THEOREM (REDIS ACTIVE EXPIRE RATIO):
//    Instead of scanning every key, we draw a random sample of N = 20 keys with expiration times.
//    By the Law of Large Numbers and Central Limit Theorem, the sample proportion:
//
//          p_hat = (Number of Expired Keys in Sample) / N
//
//    is an unbiased point estimator of the true global ratio 'p' of expired keys across the database.
//
//    - THRESHOLD CHOICE (25% or 5 out of 20 keys):
//      - If p_hat > 25% (more than 5 out of 20 keys in our sample are expired):
//        Statistically, it indicates that > 25% of ALL volatile keys in the database are currently expired.
//        Because expired key density is high, the memory reclamation yield per CPU cycle is high.
//        Therefore, we IMMEDIATELY REPEAT the sampling cycle in a loop to purge more dead memory!
//
//      - If p_hat <= 25% (5 or fewer out of 20 keys in our sample are expired):
//        Statistically, it indicates that the global proportion of expired keys has dropped below 25%.
//        Continuing to sweep memory yields diminishing returns per CPU cycle. We HALT the cycle
//        and wait for the next scheduled tick (e.g., 100ms later).
//
// 4. LATENCY CAP (25ms MAX TIME BUDGET & MAX ITERATIONS):
//    To prevent severe latency spikes during mass key expiration events (e.g., 1,000,000 keys expiring
//    at the exact same second), each ActiveExpireCycle enforces a hard time budget cap of 25ms
//    (or 16 maximum loop iterations). Once 25ms elapses, the loop breaks regardless of p_hat,
//    returning control to client request handlers to guarantee ultra-low latency.
//
// 5. CONCRETE STEP-BY-STEP EXAMPLE:
//    Suppose the database contains 1,000,000 keys. 400,000 of them expire simultaneously at T0.
//
//    At T0 + 100ms (Active Expire Cycle triggers):
//      - Round 1: Draw N = 20 random volatile keys.
//                 Found 14 expired keys (p_hat = 14/20 = 70%). Delete 14 keys.
//                 Condition 70% > 25% holds -> REPEAT CYCLE IMMEDIATELY.
//      - Round 2: Draw N = 20 random volatile keys.
//                 Found 9 expired keys (p_hat = 9/20 = 45%). Delete 9 keys.
//                 Condition 45% > 25% holds -> REPEAT CYCLE IMMEDIATELY.
//      - Round 3: Draw N = 20 random volatile keys.
//                 Found 3 expired keys (p_hat = 3/20 = 15%). Delete 3 keys.
//                 Condition 15% <= 25% holds -> STOP CYCLE & WAIT FOR NEXT TICK.
//
//    Result: Purged dozens of expired keys in < 1ms without scanning 1,000,000 keys!
//
func (s *Store) ActiveExpireCycle() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	const (
		sampleSize     = 20                      // Number of volatile keys sampled per iteration
		thresholdRatio = 0.25                    // 25% counter-limit threshold ratio (5 / 20)
		maxIterations  = 16                      // Maximum loop iterations per tick
		maxDuration    = 25 * time.Millisecond   // Hard CPU latency cap per cycle
	)

	startTime := time.Now()
	totalDeleted := 0

	for iteration := 0; iteration < maxIterations; iteration++ {
		// Enforce time budget cap to protect client I/O latency
		if time.Since(startTime) >= maxDuration {
			break
		}

		expiredInSample := 0
		sampledCount := 0

		// Iterate over map entries to extract a random sample of keys with TTLs.
		// In Go, map iteration order is randomized by the runtime, providing natural pseudo-random sampling.
		for key, obj := range s.data {
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


