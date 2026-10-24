package caimandb

import (
	"sync"
)

type shardedLockManager struct {
	shards  []*sync.RWMutex
	shardFn func(string) int
}

func newShardedLockManager(shards int) *shardedLockManager {
	if shards <= 0 {
		shards = 100000
	}
	s := &shardedLockManager{
		shards: make([]*sync.RWMutex, shards),
	}
	for i := range s.shards {
		s.shards[i] = &sync.RWMutex{}
	}
	s.shardFn = func(key string) int {
		h := fnv32a(key)
		return int(h) % len(s.shards)
	}
	return s
}

func (s *shardedLockManager) Lock(key string) {
	s.shards[s.shardFn(key)].Lock()
}

func (s *shardedLockManager) Unlock(key string) {
	s.shards[s.shardFn(key)].Unlock()
}

func (s *shardedLockManager) RLock(key string) {
	s.shards[s.shardFn(key)].RLock()
}

func (s *shardedLockManager) RUnlock(key string) {
	s.shards[s.shardFn(key)].RUnlock()
}

func fnv32a(s string) uint32 {
	const prime = 0x01000193
	var hash uint32 = 0x811c9dc5
	for i := 0; i < len(s); i++ {
		hash ^= uint32(s[i])
		hash *= prime
	}
	return hash
}
