package limiter

import (
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token-bucket rate limiter.
// It uses a lazy calculation model to refill tokens based on elapsed time
// without requiring a background goroutine for every tenant.

type TokenBucket struct {
	mu         sync.Mutex
	capacity   float64   // Maximum burst limit (max tokens in bucket)
	refillRate float64   // Tokens added per second
	tokens     float64   // Current available token count
	lastRefill time.Time // Timestamp of last token replenishment calculation
}

// NewTokenBucket initializes a new TokenBucket with specified burst capacity and refill rate (tokens/sec).
func NewTokenBucket(capacity, refillRate float64) *TokenBucket {
	if capacity <= 0 {
		capacity = 1.0
	}

	if refillRate <= 0 {
		refillRate = 1.0
	}

	return &TokenBucket{
		capacity:   capacity,
		refillRate: refillRate,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
}

// Allow attempts to consume 1 token from the bucket.
// Returns true if the token was available and consumed; false if rate limited.
func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1.0)
}

// AllowN attempts to consume n tokens from the bucket.
// Returns true if n tokens were available and consumed; false otherwise.
func (tb *TokenBucket) AllowN(n float64) bool {
	if n <= 0 {
		return true
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.refillLocked(now)

	if tb.tokens >= n {
		tb.tokens -= n
		return true
	}
	return false
}

// refillLocked calculates elapsed time since lastRefill and replenishes tokens up to capacity.
// Must be called with tb.mu held.
func (tb *TokenBucket) refillLocked(now time.Time) {

	elapsed := now.Sub(tb.lastRefill).Seconds()

	if elapsed <= 0 {
		return
	}

	deltaTokens := elapsed * tb.refillRate
	tb.tokens += deltaTokens
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	tb.lastRefill = now
}

// UpdateLimits dynamically updates the burst capacity and refill rate of the bucket.
func (tb *TokenBucket) UpdateLimits(capacity, refillRate float64) {
	if capacity <= 0 || refillRate <= 0 {
		return
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.refillLocked(now)

	tb.capacity = capacity
	tb.refillRate = refillRate

	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// RefillRate returns the refill rate (tokens per second) of the bucket.
func (tb *TokenBucket) RefillRate() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.refillRate
}

// Capacity returns the maximum capacity of the bucket.
func (tb *TokenBucket) Capacity() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.capacity
}

// Tokens returns the current number of available tokens in the bucket after applying refill calculations.
func (tb *TokenBucket) Tokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refillLocked(time.Now())
	return tb.tokens
}

// Every exported function in token_bucket.go (AllowN, Tokens, Capacity, RefillRate, UpdateLimits) requests tb.mu.Lock().
// If 10 threads call functions on the same TokenBucket simultaneously, Go places them in tb.mu's thread-safe queue.
// They execute one-by-one sequentially, taking less than 1 nanosecond each, so there are zero data races and zero corrupted numbers in RAM!
