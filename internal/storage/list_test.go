package storage

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestListPushPopAndTTL(t *testing.T) {
	store := NewStore()

	length, err := store.ListPush("items", []string{"first", "second"}, true, false)
	if err != nil || length != 2 {
		t.Fatalf("ListPush() = (%d, %v), want (2, nil)", length, err)
	}
	length, err = store.ListPush("items", []string{"third", "fourth"}, false, false)
	if err != nil || length != 4 {
		t.Fatalf("ListPush() = (%d, %v), want (4, nil)", length, err)
	}
	if !store.Expire("items", 60) {
		t.Fatal("Expire() did not set the list TTL")
	}

	values, err := store.ListRange("items", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"second", "first", "third", "fourth"}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("ListRange() = %v, want %v", values, want)
	}
	if ttl := store.TTL("items"); ttl <= 0 {
		t.Fatalf("list mutation lost key TTL, got %d", ttl)
	}

	popped, err := store.ListPop("items", true, 2)
	if err != nil || !reflect.DeepEqual(popped, []string{"second", "first"}) {
		t.Fatalf("ListPop(left) = (%v, %v)", popped, err)
	}
	popped, err = store.ListPop("items", false, 5)
	if err != nil || !reflect.DeepEqual(popped, []string{"fourth", "third"}) {
		t.Fatalf("ListPop(right) = (%v, %v)", popped, err)
	}
	if ttl := store.TTL("items"); ttl != -2 {
		t.Fatalf("empty list should remove its key, TTL() = %d", ttl)
	}
}

func TestListIndexSetTrimAndRemove(t *testing.T) {
	store := NewStore()
	_, _ = store.ListPush("items", []string{"a", "b", "a", "c", "a"}, false, false)

	value, found, err := store.ListIndex("items", -2)
	if err != nil || !found || value != "c" {
		t.Fatalf("ListIndex() = (%q, %t, %v), want (c, true, nil)", value, found, err)
	}
	if err := store.ListSet("items", -2, "changed"); err != nil {
		t.Fatal(err)
	}
	removed, err := store.ListRem("items", -1, "a")
	if err != nil || removed != 1 {
		t.Fatalf("ListRem(from right) = (%d, %v), want (1, nil)", removed, err)
	}
	removed, err = store.ListRem("items", 0, "a")
	if err != nil || removed != 2 {
		t.Fatalf("ListRem(all) = (%d, %v), want (2, nil)", removed, err)
	}
	if err := store.ListTrim("items", 1, -1); err != nil {
		t.Fatal(err)
	}
	values, err := store.ListRange("items", 0, -1)
	if err != nil || !reflect.DeepEqual(values, []string{"changed"}) {
		t.Fatalf("ListRange after trim = (%v, %v), want ([changed], nil)", values, err)
	}
}

func TestListDequeWraparoundAndGrowth(t *testing.T) {
	store := NewStore()
	want := make([]string, 0)
	for i := 0; i < 40; i++ {
		value := strconv.Itoa(i)
		_, _ = store.ListPush("items", []string{value}, true, false)
		want = append([]string{value}, want...)
	}
	for i := 0; i < 25; i++ {
		got, err := store.ListPop("items", true, 1)
		if err != nil || len(got) != 1 || got[0] != want[0] {
			t.Fatalf("left pop %d = (%v, %v), want %q", i, got, err, want[0])
		}
		want = want[1:]
	}
	for i := 40; i < 100; i++ {
		value := strconv.Itoa(i)
		_, _ = store.ListPush("items", []string{value}, false, false)
		want = append(want, value)
	}
	for i := 100; i < 140; i++ {
		value := strconv.Itoa(i)
		_, _ = store.ListPush("items", []string{value}, true, false)
		want = append([]string{value}, want...)
	}
	got, err := store.ListRange("items", 0, -1)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("range after ring wraparound = (%v, %v), want %v", got, err, want)
	}
}

func TestListInsertPositionAndMove(t *testing.T) {
	store := NewStore()
	_, _ = store.ListPush("source", []string{"a", "b", "a", "c"}, false, false)

	length, err := store.ListInsert("source", "b", "before", true)
	if err != nil || length != 5 {
		t.Fatalf("ListInsert() = (%d, %v), want (5, nil)", length, err)
	}
	positions, err := store.ListPosition("source", "a", 1, 0, 0)
	if err != nil || !reflect.DeepEqual(positions, []int64{0, 3}) {
		t.Fatalf("ListPosition() = (%v, %v), want ([0 3], nil)", positions, err)
	}
	positions, err = store.ListPosition("source", "a", -1, 1, 0)
	if err != nil || !reflect.DeepEqual(positions, []int64{3}) {
		t.Fatalf("reverse ListPosition() = (%v, %v), want ([3], nil)", positions, err)
	}

	value, found, err := store.ListMove("source", "destination", false, true)
	if err != nil || !found || value != "c" {
		t.Fatalf("ListMove() = (%q, %t, %v), want (c, true, nil)", value, found, err)
	}
	values, err := store.ListRange("destination", 0, -1)
	if err != nil || !reflect.DeepEqual(values, []string{"c"}) {
		t.Fatalf("destination = (%v, %v), want ([c], nil)", values, err)
	}
}

func TestListWrongTypeAndMissingKey(t *testing.T) {
	store := NewStore()
	store.Set("string", "value", -1)

	if _, err := store.ListPush("string", []string{"x"}, false, false); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ListPush() error = %v, want ErrWrongType", err)
	}
	if _, _, err := store.ListIndex("string", 0); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ListIndex() error = %v, want ErrWrongType", err)
	}
	if err := store.ListSet("missing", 0, "x"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("ListSet() error = %v, want ErrNoSuchKey", err)
	}
	if err := store.ListSet("string", 0, "x"); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ListSet() error = %v, want ErrWrongType", err)
	}
	if _, err := store.ListPop("missing", true, 1); err != nil {
		t.Fatalf("ListPop(missing) error = %v, want nil", err)
	}
}

func TestListMoveToWrongTypeDoesNotPopSource(t *testing.T) {
	store := NewStore()
	_, _ = store.ListPush("source", []string{"keep"}, false, false)
	store.Set("destination", "string", -1)

	if _, _, err := store.ListMove("source", "destination", true, true); !errors.Is(err, ErrWrongType) {
		t.Fatalf("ListMove() error = %v, want ErrWrongType", err)
	}
	values, err := store.ListRange("source", 0, -1)
	if err != nil || !reflect.DeepEqual(values, []string{"keep"}) {
		t.Fatalf("source after failed move = (%v, %v), want ([keep], nil)", values, err)
	}
}

func TestListRemMinimumIntegerCount(t *testing.T) {
	store := NewStore()
	_, _ = store.ListPush("items", []string{"x", "y", "x"}, false, false)
	removed, err := store.ListRem("items", -1<<63, "x")
	if err != nil || removed != 2 {
		t.Fatalf("ListRem(min int) = (%d, %v), want (2, nil)", removed, err)
	}
}

func TestListPushExistingAndExpiredKey(t *testing.T) {
	store := NewStore()
	length, err := store.ListPush("missing", []string{"x"}, true, true)
	if err != nil || length != 0 {
		t.Fatalf("LPUSHX-like push = (%d, %v), want (0, nil)", length, err)
	}
	store.Set("expired", "old", time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	length, err = store.ListPush("expired", []string{"new"}, false, false)
	if err != nil || length != 1 {
		t.Fatalf("push to expired key = (%d, %v), want (1, nil)", length, err)
	}
}

func TestConcurrentListPush(t *testing.T) {
	store := NewStore()
	const workers = 8
	const pushes = 100
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < pushes; i++ {
				if _, err := store.ListPush("items", []string{"x"}, false, false); err != nil {
					t.Errorf("ListPush() error = %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	length, err := store.ListLen("items")
	if err != nil || length != workers*pushes {
		t.Fatalf("ListLen() = (%d, %v), want (%d, nil)", length, err, workers*pushes)
	}
}
