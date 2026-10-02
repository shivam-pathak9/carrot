// list.go implements list values and their atomic operations under Store.mu.
package storage

import (
	"errors"
	"time"
)

var (
	ErrWrongType      = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey      = errors.New("no such key")
	ErrListIndexRange = errors.New("list index out of range")
)

// listValue is a growable circular buffer: pushes and pops at either end are O(1).
// head identifies the first element and size is the number of live elements.
type listValue struct {
	buffer []string
	head   int
	size   int
}

func (l *listValue) Len() int {
	return l.size
}

func (l *listValue) grow() {
	if l.size < len(l.buffer) {
		return
	}
	capacity := len(l.buffer) * 2
	if capacity == 0 {
		capacity = 8
	}
	buffer := make([]string, capacity)
	for i := 0; i < l.size; i++ {
		buffer[i] = l.at(i)
	}
	l.buffer = buffer
	l.head = 0
}

func (l *listValue) at(index int) string {
	return l.buffer[(l.head+index)%len(l.buffer)]
}

func (l *listValue) set(index int, value string) {
	l.buffer[(l.head+index)%len(l.buffer)] = value
}

func (l *listValue) push(value string, left bool) {
	l.grow()
	if left {
		l.head = (l.head - 1 + len(l.buffer)) % len(l.buffer)
	} else {
		l.set(l.size, value)
	}
	if left {
		l.buffer[l.head] = value
	}
	l.size++
}

func (l *listValue) pop(left bool) (string, bool) {
	if l.size == 0 {
		return "", false
	}
	if left {
		value := l.at(0)
		l.buffer[l.head] = ""
		l.head = (l.head + 1) % len(l.buffer)
		l.size--
		return value, true
	}
	last := l.size - 1
	value := l.at(last)
	l.set(last, "")
	l.size--
	return value, true
}

func (l *listValue) insert(index int, value string) {
	l.grow()
	if index < l.size-index {
		l.head = (l.head - 1 + len(l.buffer)) % len(l.buffer)
		for i := 0; i < index; i++ {
			l.set(i, l.at(i+1))
		}
	} else {
		for i := l.size; i > index; i-- {
			l.set(i, l.at(i-1))
		}
	}
	l.set(index, value)
	l.size++
}

func (l *listValue) removeAt(index int) {
	if index < l.size-index-1 {
		for i := index; i > 0; i-- {
			l.set(i, l.at(i-1))
		}
		l.buffer[l.head] = ""
		l.head = (l.head + 1) % len(l.buffer)
	} else {
		for i := index; i < l.size-1; i++ {
			l.set(i, l.at(i+1))
		}
		l.set(l.size-1, "")
	}
	l.size--
}

func (l *listValue) rangeCopy(start, end int) []string {
	items := make([]string, end-start)
	for i := start; i < end; i++ {
		items[i-start] = l.at(i)
	}
	return items
}

func (l *listValue) replace(items []string) {
	l.buffer = make([]string, len(items))
	copy(l.buffer, items)
	l.head = 0
	l.size = len(items)
}

func (l *listValue) index(index int64) (int, bool) {
	length := int64(l.size)
	if index < 0 {
		index += length
	}
	if index < 0 || index >= length {
		return 0, false
	}
	return int(index), true
}

func (s *Store) liveObjectLocked(sh *shard, key string) (Obj, bool) {
	obj, exists := sh.data[key]
	if exists && obj.IsExpired(time.Now()) {
		delete(sh.data, key)
		return Obj{}, false
	}
	return obj, exists
}

// GetString returns a string value and reports WRONGTYPE for non-string keys.
func (s *Store) GetString(key string) (string, bool, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return "", false, nil
	}
	if obj.Kind != StringKind {
		return "", false, ErrWrongType
	}
	return obj.Value, true, nil
}

// ListPush pushes values in argument order onto one end of a list.
func (s *Store) ListPush(key string, values []string, left, onlyExisting bool) (int64, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		if onlyExisting {
			return 0, nil
		}
		obj = Obj{Kind: ListKind}
	}
	if obj.Kind != ListKind {
		return 0, ErrWrongType
	}
	for _, value := range values {
		obj.list.push(value, left)
	}
	sh.data[key] = obj
	return int64(obj.list.Len()), nil
}

// ListPop removes up to count values from one end of a list.
func (s *Store) ListPop(key string, left bool, count int64) ([]string, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return nil, nil
	}
	if obj.Kind != ListKind {
		return nil, ErrWrongType
	}

	n := count
	if n > int64(obj.list.Len()) {
		n = int64(obj.list.Len())
	}
	values := make([]string, 0, n)
	for i := int64(0); i < n; i++ {
		value, _ := obj.list.pop(left)
		values = append(values, value)
	}
	if obj.list.Len() == 0 {
		delete(sh.data, key)
	} else {
		sh.data[key] = obj
	}
	return values, nil
}

// ListLen returns zero for a missing list.
func (s *Store) ListLen(key string) (int64, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return 0, nil
	}
	if obj.Kind != ListKind {
		return 0, ErrWrongType
	}
	return int64(obj.list.Len()), nil
}

// ListRange returns the inclusive range from start to stop, supporting negative indexes.
func (s *Store) ListRange(key string, start, stop int64) ([]string, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return []string{}, nil
	}
	if obj.Kind != ListKind {
		return nil, ErrWrongType
	}
	length := int64(obj.list.Len())
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start >= length || start > stop || length == 0 {
		return []string{}, nil
	}
	return obj.list.rangeCopy(int(start), int(stop+1)), nil
}

// ListIndex returns an item by index, supporting negative indexes.
func (s *Store) ListIndex(key string, index int64) (string, bool, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return "", false, nil
	}
	if obj.Kind != ListKind {
		return "", false, ErrWrongType
	}
	i, ok := obj.list.index(index)
	if !ok {
		return "", false, nil
	}
	return obj.list.at(i), true, nil
}

// ListSet updates an item by index, supporting negative indexes.
func (s *Store) ListSet(key string, index int64, value string) error {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return ErrNoSuchKey
	}
	if obj.Kind != ListKind {
		return ErrWrongType
	}
	i, ok := obj.list.index(index)
	if !ok {
		return ErrListIndexRange
	}
	obj.list.set(i, value)
	sh.data[key] = obj
	return nil
}

// ListTrim keeps the inclusive range and removes the key when no elements remain.
func (s *Store) ListTrim(key string, start, stop int64) error {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return nil
	}
	if obj.Kind != ListKind {
		return ErrWrongType
	}
	length := int64(obj.list.Len())
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if length == 0 || start >= length || start > stop {
		delete(sh.data, key)
		return nil
	}
	obj.list.replace(obj.list.rangeCopy(int(start), int(stop+1)))
	sh.data[key] = obj
	return nil
}

// ListRem removes count occurrences. A zero count removes every matching item.
func (s *Store) ListRem(key string, count int64, value string) (int64, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return 0, nil
	}
	if obj.Kind != ListKind {
		return 0, ErrWrongType
	}
	removed := int64(0)
	limit := count
	if limit < 0 {
		if limit == -1<<63 {
			limit = 1<<63 - 1
		} else {
			limit = -limit
		}
	}
	if count >= 0 {
		for i := 0; i < obj.list.Len(); {
			if obj.list.at(i) == value && (count == 0 || removed < limit) {
				obj.list.removeAt(i)
				removed++
				continue
			}
			i++
		}
	} else {
		for i := obj.list.Len() - 1; i >= 0; i-- {
			if obj.list.at(i) == value && removed < limit {
				obj.list.removeAt(i)
				removed++
			}
		}
	}
	if obj.list.Len() == 0 {
		delete(sh.data, key)
	} else {
		sh.data[key] = obj
	}
	return removed, nil
}

// ListInsert inserts value before or after the first matching pivot.
// It returns zero for a missing key and -1 when the pivot is not present.
func (s *Store) ListInsert(key, pivot, value string, before bool) (int64, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return 0, nil
	}
	if obj.Kind != ListKind {
		return 0, ErrWrongType
	}
	for i := 0; i < obj.list.Len(); i++ {
		item := obj.list.at(i)
		if item != pivot {
			continue
		}
		insertAt := i
		if !before {
			insertAt++
		}
		obj.list.insert(insertAt, value)
		sh.data[key] = obj
		return int64(obj.list.Len()), nil
	}
	return -1, nil
}

// ListPosition returns matching indexes using Redis LPOS rank/count/maxlen semantics.
func (s *Store) ListPosition(key, value string, rank, count, maxLen int64) ([]int64, error) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if rank == -1<<63 {
		return nil, errors.New("invalid rank")
	}
	obj, exists := s.liveObjectLocked(sh, key)
	if !exists {
		return []int64{}, nil
	}
	if obj.Kind != ListKind {
		return nil, ErrWrongType
	}

	length := int64(obj.list.Len())
	start, end, step := int64(0), length, int64(1)
	if rank < 0 {
		start, end, step = length-1, -1, -1
		rank = -rank
	}
	seen := int64(0)
	examined := int64(0)
	positions := make([]int64, 0)
	for index := start; index != end; index += step {
		if maxLen > 0 && examined >= maxLen {
			break
		}
		examined++
		if obj.list.at(int(index)) != value {
			continue
		}
		seen++
		if seen < rank {
			continue
		}
		positions = append(positions, index)
		if count > 0 && int64(len(positions)) >= count {
			break
		}
	}
	return positions, nil
}

// ListMove atomically pops from one end of src and pushes onto one end of dst.
// Both keys are checked and updated under shard locks acquired in sorted index order to prevent deadlocks.
func (s *Store) ListMove(src, dst string, fromLeft, toLeft bool) (string, bool, error) {
	idx1 := s.getShardIndex(src)
	idx2 := s.getShardIndex(dst)

	if idx1 == idx2 {
		sh := &s.shards[idx1]
		sh.mu.Lock()
		defer sh.mu.Unlock()
		return s.listMoveLocked(sh, sh, src, dst, fromLeft, toLeft)
	}

	// Always acquire lower shard index first to prevent deadlocks when concurrent LMOVE operations specify opposite key order.
	if idx1 < idx2 {
		s.shards[idx1].mu.Lock()
		defer s.shards[idx1].mu.Unlock()
		s.shards[idx2].mu.Lock()
		defer s.shards[idx2].mu.Unlock()
	} else {
		s.shards[idx2].mu.Lock()
		defer s.shards[idx2].mu.Unlock()
		s.shards[idx1].mu.Lock()
		defer s.shards[idx1].mu.Unlock()
	}

	return s.listMoveLocked(&s.shards[idx1], &s.shards[idx2], src, dst, fromLeft, toLeft)
}

func (s *Store) listMoveLocked(srcShard, dstShard *shard, src, dst string, fromLeft, toLeft bool) (string, bool, error) {
	source, exists := s.liveObjectLocked(srcShard, src)
	if !exists {
		return "", false, nil
	}
	if source.Kind != ListKind {
		return "", false, ErrWrongType
	}
	if src == dst {
		value, _ := source.list.pop(fromLeft)
		source.list.push(value, toLeft)
		srcShard.data[src] = source
		return value, true, nil
	}

	destination, destExists := s.liveObjectLocked(dstShard, dst)
	if destExists && destination.Kind != ListKind {
		return "", false, ErrWrongType
	}
	value, _ := source.list.pop(fromLeft)
	if destExists {
		destination.list.push(value, toLeft)
	} else {
		destination = Obj{Kind: ListKind}
		destination.list.push(value, toLeft)
	}
	if source.list.Len() == 0 {
		delete(srcShard.data, src)
	} else {
		srcShard.data[src] = source
	}
	dstShard.data[dst] = destination
	return value, true, nil
}
