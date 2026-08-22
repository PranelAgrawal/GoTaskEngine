package limiter_test

import (
	"sync"
	"testing"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/limiter"
)

func TestTokenBucket_BurstCapacity(t *testing.T) {
	capacity := 5.0
	refillRate := 1.0 // 1 token per sec
	tb := limiter.NewTokenBucket(capacity, refillRate)

	// Bucket starts full at capacity 5
	for i := 0; i < 5; i++ {
		if !tb.Allow() {
			t.Fatalf("expected token %d to be allowed under burst capacity", i+1)
		}
	}

	// 6th request immediately after should be throttled
	if tb.Allow() {
		t.Fatal("expected 6th immediate request to be throttled")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	capacity := 2.0
	refillRate := 10.0 // 10 tokens per sec (1 token per 100ms)
	tb := limiter.NewTokenBucket(capacity, refillRate)

	// Consume all 2 tokens
	tb.Allow()
	tb.Allow()
	if tb.Allow() {
		t.Fatal("expected request to be throttled when bucket is empty")
	}

	// Wait 250ms -> should refill ~2.5 tokens, capped at capacity 2.0
	time.Sleep(250 * time.Millisecond)

	if !tb.Allow() {
		t.Fatal("expected request to succeed after refill window")
	}
}

func TestTokenBucket_ConcurrentAccess(t *testing.T) {
	capacity := 100.0
	refillRate := 50.0
	tb := limiter.NewTokenBucket(capacity, refillRate)

	var wg sync.WaitGroup
	allowedCount := 0
	var mu sync.Mutex

	goroutines := 50
	requestsPerGoroutine := 10

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				if tb.Allow() {
					mu.Lock()
					allowedCount++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	if allowedCount <= 0 || float64(allowedCount) > capacity+10 {
		t.Fatalf("unexpected allowed count under concurrent access: %d (capacity: %.0f)", allowedCount, capacity)
	}
}

func TestLimiterManager_MultiTenant(t *testing.T) {
	mgr := limiter.NewManager(5.0, 2.0)

	// Tenant A consumes all 5 tokens
	tenantA := "tenant-a"
	for i := 0; i < 5; i++ {
		if !mgr.Allow(tenantA) {
			t.Fatalf("tenant A request %d should be allowed", i+1)
		}
	}

	// Tenant A should be throttled
	if mgr.Allow(tenantA) {
		t.Fatal("tenant A should be throttled after exhausting capacity")
	}

	// Tenant B should still have full 5 tokens (tenant isolation)
	tenantB := "tenant-b"
	for i := 0; i < 5; i++ {
		if !mgr.Allow(tenantB) {
			t.Fatalf("tenant B request %d should be allowed (tenant isolation failed)", i+1)
		}
	}

	if mgr.TenantCount() != 2 {
		t.Fatalf("expected 2 active tenants, got %d", mgr.TenantCount())
	}
}
