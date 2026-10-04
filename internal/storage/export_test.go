package storage

func (s *Store) getRawObj(key string) (Obj, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	obj, exists := sh.data[key]
	return obj, exists
}

func (s *Store) setRawObj(key string, obj Obj) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.data[key] = obj
}
