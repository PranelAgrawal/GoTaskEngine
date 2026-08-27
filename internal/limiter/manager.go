package limiter

import (
	"sync"
)

// LimiterManager manages per-tenant token-bucket rate limiters in a thread-safe manner.
type LimiterManager struct {
	mu                sync.RWMutex
	limiters          map[string]*TokenBucket
	defaultCapacity   float64
	defaultRefillRate float64
}

// NewManager creates a new LimiterManager with specified default tenant burst capacity and refill rate.
func NewManager(defaultCapacity, defaultRefillRate float64) *LimiterManager {

	if defaultCapacity <= 0 {
		defaultCapacity = 10.0
	}

	if defaultRefillRate <= 0 {
		defaultRefillRate = 5.0
	}

	return &LimiterManager{
		limiters:          make(map[string]*TokenBucket),
		defaultCapacity:   defaultCapacity,
		defaultRefillRate: defaultRefillRate,
	}
}

// GetLimiter retrieves the TokenBucket for a given tenant.
// If no limiter exists for the tenant, one is created using default settings.
func (m *LimiterManager) GetLimiter(tenantID string) *TokenBucket {
	if tenantID == "" {
		tenantID = "default"
	}

	m.mu.RLock()
	tb, exists := m.limiters[tenantID] // tb stores the tenantId, exists stores the bool
	m.mu.RUnlock()

	if exists {
		return tb
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1st Check (RLock): Speeds up performance for existing tenants so 1,000 threads can read in parallel without waiting.
	//2nd Check (Lock): Prevents race conditions when 2 threads try to register the same brand-new tenant at the exact same instant!

	if tb, exists = m.limiters[tenantID]; exists {
		return tb
	}

	tb = NewTokenBucket(m.defaultCapacity, m.defaultRefillRate)
	m.limiters[tenantID] = tb
	return tb
}

// Allow evaluates if a single action is permitted for the specified tenant under their rate limit.
func (m *LimiterManager) Allow(tenantID string) bool {
	return m.GetLimiter(tenantID).Allow() //calls the Allow() of tokenbucket
} // RemoveTenant removes a tenant's rate limiter from the manager map.

// AllowN evaluates if n actions are permitted for the specified tenant under their rate limit.
func (m *LimiterManager) AllowN(tenantID string, n float64) bool {
	return m.GetLimiter(tenantID).AllowN(n)
}

// ConfigureTenant sets or updates explicit burst capacity and refill rate limits for a specific tenant.
func (m *LimiterManager) ConfigureTenant(tenantID string, capacity, refillRate float64) {

	if tenantID == "" {
		tenantID = "default"
	}

	m.GetLimiter(tenantID).UpdateLimits(capacity, refillRate)
}

// RemoveTenant removes a tenant's rate limiter from the manager map.
func (m *LimiterManager) RemoveTenant(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.limiters, tenantID) // checks if tenantID exists in map, if does go removes the key-value pair from m.limiters, The pointer to that tenant's TokenBucket is removed; else if it doesnt, go does nothing.
}

// TenantCount returns the total number of actively registered tenant limiters.
func (m *LimiterManager) TenantCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.limiters)
}
