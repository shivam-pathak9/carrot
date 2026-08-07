package storage

import (
	"fmt"
	"testing"
	"time"
)

// TestNewStore tests the creation of a new Store instance
func TestNewStore(t *testing.T) {
	store := NewStore()
	if store == nil {
		t.Fatal("NewStore() should return a non-nil store")
	}
}

// TestSetAndGet tests basic Set and Get operations
func TestSetAndGet(t *testing.T) {
	store := NewStore()

	// Test: Set and retrieve a persistent key
	store.Set("name", "shivam", -1)
	value, exists := store.Get("name")

	if !exists {
		t.Error("Expected key 'name' to exist")
	}
	if value != "shivam" {
		t.Errorf("Expected value 'shivam', got '%s'", value)
	}
}

// TestSetWithExpiry tests key expiration
func TestSetWithExpiry(t *testing.T) {
	store := NewStore()

	// Test: Set a key with short TTL
	store.Set("temp", "value", 100*time.Millisecond)
	value, exists := store.Get("temp")

	if !exists {
		t.Error("Expected key 'temp' to exist immediately after setting")
	}
	if value != "value" {
		t.Errorf("Expected value 'value', got '%s'", value)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)
	value, exists = store.Get("temp")

	if exists {
		t.Error("Expected key 'temp' to be expired")
	}
	if value != "" {
		t.Errorf("Expected empty value for expired key, got '%s'", value)
	}
}

// TestGetNonExistent tests getting a key that doesn't exist
func TestGetNonExistent(t *testing.T) {
	store := NewStore()

	value, exists := store.Get("nonexistent")
	if exists {
		t.Error("Expected nonexistent key to not exist")
	}
	if value != "" {
		t.Errorf("Expected empty value, got '%s'", value)
	}
}

// TestDel tests deletion of keys
func TestDel(t *testing.T) {
	store := NewStore()

	// Test: Delete an existing key
	store.Set("deletekey", "value", -1)
	deleted := store.Del("deletekey")

	if !deleted {
		t.Error("Expected Del to return true for existing key")
	}

	value, exists := store.Get("deletekey")
	if exists {
		t.Error("Expected deleted key to not exist")
	}
	if value != "" {
		t.Errorf("Expected empty value, got '%s'", value)
	}
}

// TestDelNonExistent tests deletion of non-existent key
func TestDelNonExistent(t *testing.T) {
	store := NewStore()

	deleted := store.Del("nonexistent")
	if deleted {
		t.Error("Expected Del to return false for non-existent key")
	}
}

// TestTTLPersistentKey tests TTL for persistent key
func TestTTLPersistentKey(t *testing.T) {
	store := NewStore()

	store.Set("persistent", "value", -1)
	ttl := store.TTL("persistent")

	if ttl != -1 {
		t.Errorf("Expected TTL -1 for persistent key, got %d", ttl)
	}
}

// TestTTLNonExistent tests TTL for non-existent key
func TestTTLNonExistent(t *testing.T) {
	store := NewStore()

	ttl := store.TTL("nonexistent")
	if ttl != -2 {
		t.Errorf("Expected TTL -2 for non-existent key, got %d", ttl)
	}
}

// TestTTLExpiring tests TTL for key with expiration
func TestTTLExpiring(t *testing.T) {
	store := NewStore()

	store.Set("expiring", "value", 2*time.Second)
	ttl := store.TTL("expiring")

	if ttl <= 0 || ttl > 2 {
		t.Errorf("Expected TTL between 1 and 2 seconds, got %d", ttl)
	}
}

// TestTTLExpired tests TTL for expired key
func TestTTLExpired(t *testing.T) {
	store := NewStore()

	store.Set("expiring", "value", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	ttl := store.TTL("expiring")
	if ttl != -2 {
		t.Errorf("Expected TTL -2 for expired key, got %d", ttl)
	}
}

// TestExpire tests setting expiration on existing key
func TestExpire(t *testing.T) {
	store := NewStore()

	store.Set("expire_test", "value", -1)
	success := store.Expire("expire_test", 2) // 2 seconds

	if !success {
		t.Error("Expected Expire to return true for existing key")
	}

	// Verify TTL was set
	ttl := store.TTL("expire_test")
	if ttl <= 0 || ttl > 2 {
		t.Errorf("Expected TTL between 1 and 2, got %d", ttl)
	}

	// Wait for expiration
	time.Sleep(2100 * time.Millisecond)
	_, exists := store.Get("expire_test")
	if exists {
		t.Error("Expected key to be expired after TTL")
	}
}

// TestExpireNonExistent tests expiring a non-existent key
func TestExpireNonExistent(t *testing.T) {
	store := NewStore()

	success := store.Expire("nonexistent", 1)
	if success {
		t.Error("Expected Expire to return false for non-existent key")
	}
}

// TestMultipleKeys tests storing and retrieving multiple keys
func TestMultipleKeys(t *testing.T) {
	store := NewStore()

	// Set multiple keys
	store.Set("key1", "value1", -1)
	store.Set("key2", "value2", -1)
	store.Set("key3", "value3", -1)

	// Retrieve and verify
	tests := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, tt := range tests {
		value, exists := store.Get(tt.key)
		if !exists {
			t.Errorf("Expected key '%s' to exist", tt.key)
		}
		if value != tt.value {
			t.Errorf("For key '%s': expected '%s', got '%s'", tt.key, tt.value, value)
		}
	}
}

// TestOverwriteKey tests overwriting an existing key
func TestOverwriteKey(t *testing.T) {
	store := NewStore()

	store.Set("key", "value1", -1)
	value, _ := store.Get("key")
	if value != "value1" {
		t.Errorf("Expected 'value1', got '%s'", value)
	}

	// Overwrite
	store.Set("key", "value2", -1)
	value, _ = store.Get("key")
	if value != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value)
	}
}

// TestConcurrentAccess tests concurrent read/write operations
func TestConcurrentAccess(t *testing.T) {
	store := NewStore()
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			store.Set("key", "value", -1)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			store.Get("key")
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done
}

// TestEmptyStringValue tests storing and retrieving empty string
func TestEmptyStringValue(t *testing.T) {
	store := NewStore()

	store.Set("empty", "", -1)
	value, exists := store.Get("empty")

	if !exists {
		t.Error("Expected empty string key to exist")
	}
	if value != "" {
		t.Errorf("Expected empty string, got '%s'", value)
	}
}

// TestSpecialCharacters tests storing keys and values with special characters
func TestSpecialCharacters(t *testing.T) {
	store := NewStore()

	testCases := []struct {
		key   string
		value string
	}{
		{"key:1", "value:1"},
		{"key|special", "value|special"},
		{"key\nwith\nnewline", "value\nwith\nnewline"},
		{"key\twith\ttabs", "value\twith\ttabs"},
	}

	for _, tc := range testCases {
		store.Set(tc.key, tc.value, -1)
		value, exists := store.Get(tc.key)
		if !exists {
			t.Errorf("Expected key '%s' to exist", tc.key)
		}
		if value != tc.value {
			t.Errorf("For key '%s': expected '%s', got '%s'", tc.key, tc.value, value)
		}
	}
}

// TestLargeValue tests storing very large values
func TestLargeValue(t *testing.T) {
	store := NewStore()

	// Create a large value (1MB)
	largeValue := ""
	for i := 0; i < 1024; i++ {
		largeValue += "This is a line of text. "
	}

	store.Set("large", largeValue, -1)
	value, exists := store.Get("large")

	if !exists {
		t.Error("Expected large key to exist")
	}
	if value != largeValue {
		t.Error("Large value not retrieved correctly")
	}
}

// TestZeroTTL tests setting TTL to 0 (should be persistent)
func TestZeroTTL(t *testing.T) {
	store := NewStore()

	store.Set("zerottl", "value", 0)
	ttl := store.TTL("zerottl")

	if ttl != -1 {
		t.Errorf("Expected TTL -1 for zero TTL (persistent), got %d", ttl)
	}
}

// TestNegativeTTL tests setting negative TTL (should be persistent)
func TestNegativeTTL(t *testing.T) {
	store := NewStore()

	store.Set("negativettl", "value", -10*time.Second)
	ttl := store.TTL("negativettl")

	if ttl != -1 {
		t.Errorf("Expected TTL -1 for negative TTL (persistent), got %d", ttl)
	}
}

// TestExpireNegativeSeconds tests EXPIRE with negative seconds
func TestExpireNegativeSeconds(t *testing.T) {
	store := NewStore()

	store.Set("key", "value", -1)
	success := store.Expire("key", -5)

	if !success {
		t.Error("Expected Expire to return true even with negative seconds")
	}

	// Key should be deleted when negative seconds is passed
	ttl := store.TTL("key")
	if ttl != -2 {
		t.Errorf("Expected key to be deleted (TTL -2), got %d", ttl)
	}
}

// TestExpireZeroSeconds tests EXPIRE with zero seconds
func TestExpireZeroSeconds(t *testing.T) {
	store := NewStore()

	store.Set("key", "value", -1)
	success := store.Expire("key", 0)

	if !success {
		t.Error("Expected Expire to return true with zero seconds")
	}

	// Key should be deleted
	ttl := store.TTL("key")
	if ttl != -2 {
		t.Errorf("Expected key to be deleted (TTL -2), got %d", ttl)
	}
}

// TestGetAfterExpire tests GET after key expires
func TestGetAfterExpire(t *testing.T) {
	store := NewStore()

	store.Set("temp", "value", 100*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	value, exists := store.Get("temp")
	if exists {
		t.Error("Expected key to be expired and not exist")
	}
	if value != "" {
		t.Errorf("Expected empty value for expired key, got '%s'", value)
	}
}

// TestMultipleDeletions tests deleting the same key multiple times
func TestMultipleDeletions(t *testing.T) {
	store := NewStore()

	store.Set("key", "value", -1)

	// First deletion should succeed
	deleted := store.Del("key")
	if !deleted {
		t.Error("First deletion should succeed")
	}

	// Second deletion should fail
	deleted = store.Del("key")
	if deleted {
		t.Error("Second deletion should fail")
	}
}

func TestPassiveDeletionOnExpiredAccess(t *testing.T) {
	store := NewStore()

	store.Set("get_expired", "value", 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)

	value, exists := store.Get("get_expired")
	if exists {
		t.Fatal("expected expired key to be removed during passive deletion")
	}
	if value != "" {
		t.Fatalf("expected empty value for expired key, got %q", value)
	}
	if _, exists := store.data["get_expired"]; exists {
		t.Fatal("passive deletion should remove expired key from the map")
	}

	store.Set("ttl_expired", "value", 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)

	if ttl := store.TTL("ttl_expired"); ttl != -2 {
		t.Fatalf("expected TTL -2 for expired key, got %d", ttl)
	}
	if _, exists := store.data["ttl_expired"]; exists {
		t.Fatal("TTL should remove expired key from the map")
	}

	store.Set("del_expired", "value", 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)

	if deleted := store.Del("del_expired"); deleted {
		t.Fatal("expired key should be treated as already deleted and return false")
	}
	if _, exists := store.data["del_expired"]; exists {
		t.Fatal("Del should remove expired key from the map")
	}
}

func TestActiveExpireCycleRemovesExpiredKeys(t *testing.T) {
	store := NewStore()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("vol-%d", i)
		if i < 6 {
			store.data[key] = Obj{Value: "expired", ExpiresAt: time.Now().Add(-1 * time.Second)}
			continue
		}
		store.data[key] = Obj{Value: fmt.Sprintf("live-%d", i), ExpiresAt: time.Now().Add(5 * time.Second)}
	}
	store.data["persistent"] = Obj{Value: "keep", ExpiresAt: time.Time{}}

	deleted := store.ActiveExpireCycle()
	if deleted != 6 {
		t.Fatalf("expected active expiry to delete 6 expired keys, got %d", deleted)
	}
	if _, exists := store.data["persistent"]; !exists {
		t.Fatal("persistent keys should be ignored by active expiry")
	}
	for i := 0; i < 6; i++ {
		if _, exists := store.data[fmt.Sprintf("vol-%d", i)]; exists {
			t.Fatalf("expired key vol-%d should have been removed by active expiry", i)
		}
	}
	for i := 6; i < 20; i++ {
		if _, exists := store.data[fmt.Sprintf("vol-%d", i)]; !exists {
			t.Fatalf("active key vol-%d should remain after expiry cycle", i)
		}
	}
}

func TestActiveExpireCycleStopsBelowThreshold(t *testing.T) {
	store := NewStore()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("edge-%d", i)
		if i < 4 {
			store.data[key] = Obj{Value: "expired", ExpiresAt: time.Now().Add(-1 * time.Second)}
			continue
		}
		store.data[key] = Obj{Value: fmt.Sprintf("live-%d", i), ExpiresAt: time.Now().Add(5 * time.Second)}
	}

	deleted := store.ActiveExpireCycle()
	if deleted != 4 {
		t.Fatalf("expected 4 expired keys to be removed, got %d", deleted)
	}
	for i := 0; i < 4; i++ {
		if _, exists := store.data[fmt.Sprintf("edge-%d", i)]; exists {
			t.Fatalf("expired key edge-%d should have been removed", i)
		}
	}
	for i := 4; i < 20; i++ {
		if _, exists := store.data[fmt.Sprintf("edge-%d", i)]; !exists {
			t.Fatalf("live key edge-%d should still exist after active expiry", i)
		}
	}
}

func TestExpireOnExpiredKeyReturnsFalse(t *testing.T) {
	store := NewStore()
	store.data["stale"] = Obj{Value: "value", ExpiresAt: time.Now().Add(-1 * time.Second)}

	if ok := store.Expire("stale", 10); ok {
		t.Fatal("expired key should be treated as already deleted")
	}
	if _, exists := store.data["stale"]; exists {
		t.Fatal("Expire should remove an already expired key from the store")
	}
}
