package caimandb

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu       sync.RWMutex
	clients  map[string]*clientLimiter
	rate     int
	window   time.Duration
	stopChan chan struct{}
}

type clientLimiter struct {
	tokens    int
	lastReset time.Time
	mu        sync.Mutex
}

func NewRateLimiter(rate int, window time.Duration) *RateLimiter {
	r := &RateLimiter{
		clients:  make(map[string]*clientLimiter),
		rate:     rate,
		window:   window,
		stopChan: make(chan struct{}),
	}
	safeGoLoop("ratelimit_cleanup_loop", r.cleanupLoop)
	return r
}

func (r *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(r.window)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.cleanup()
		case <-r.stopChan:
			return
		}
	}
}

func (r *RateLimiter) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for key, cl := range r.clients {
		cl.mu.Lock()
		if now.Sub(cl.lastReset) > r.window*2 {
			cl.mu.Unlock()
			delete(r.clients, key)
			continue
		}
		cl.mu.Unlock()
	}
}

func (r *RateLimiter) Allow(key string) bool {
	r.mu.RLock()
	cl, ok := r.clients[key]
	r.mu.RUnlock()

	if !ok {
		r.mu.Lock()
		cl = &clientLimiter{
			tokens:    r.rate - 1,
			lastReset: time.Now(),
		}
		r.clients[key] = cl
		r.mu.Unlock()
		return true
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	now := time.Now()
	if now.Sub(cl.lastReset) >= r.window {
		cl.tokens = r.rate - 1
		cl.lastReset = now
		return true
	}

	if cl.tokens > 0 {
		cl.tokens--
		return true
	}
	return false
}

func (r *RateLimiter) Close() {
	close(r.stopChan)
}
