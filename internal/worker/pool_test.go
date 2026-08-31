package worker_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/limiter"
	"github.com/pranelagrawal/gotaskengine/internal/models"
	"github.com/pranelagrawal/gotaskengine/internal/worker"
)

func TestWorkerPool_SuccessfulExecution(t *testing.T) {
	limiterMgr := limiter.NewManager(100, 100)
	var processedCount uint64

	handler := func(ctx context.Context, task *models.Task) error {
		atomic.AddUint64(&processedCount, 1)
		return nil
	}

	exec := worker.NewDefaultExecutor(limiterMgr, handler)
	inChan := make(chan *models.Task, 10)

	pool := worker.NewWorkerPool(3, inChan, exec, nil)
	pool.Start()

	numTasks := 5
	for i := 0; i < numTasks; i++ {
		inChan <- &models.Task{
			ID:       "task-succ",
			TenantID: "tenant-1",
			Action:   "test",
		}
	}

	close(inChan)
	pool.Stop()

	stats := pool.Stats()
	if stats.Completed != uint64(numTasks) {
		t.Fatalf("expected %d completed tasks, got %d", numTasks, stats.Completed)
	}
}

func TestWorkerPool_RateLimitingAndRequeue(t *testing.T) {
	// Limiter capacity 1, refill rate 1 per sec
	limiterMgr := limiter.NewManager(1.0, 1.0)

	var requeuedCount uint64
	var mu sync.Mutex
	requeueTasks := make([]*models.Task, 0)

	requeueFn := func(task *models.Task) {
		atomic.AddUint64(&requeuedCount, 1)
		mu.Lock()
		requeueTasks = append(requeueTasks, task)
		mu.Unlock()
	}

	exec := worker.NewDefaultExecutor(limiterMgr, nil)
	inChan := make(chan *models.Task, 5)

	pool := worker.NewWorkerPool(2, inChan, exec, requeueFn)
	pool.Start()

	// Push 3 tasks for same tenant immediately
	for i := 0; i < 3; i++ {
		inChan <- &models.Task{
			ID:       "task-throttled",
			TenantID: "tenant-limited",
			Action:   "test",
		}
	}

	close(inChan)
	// Give workers time to process and trigger requeue
	time.Sleep(100 * time.Millisecond)
	pool.Stop()

	stats := pool.Stats()
	if stats.Throttled < 1 {
		t.Fatalf("expected at least 1 throttled task, got %d", stats.Throttled)
	}
}

func TestWorkerPool_RetryAndDLQ(t *testing.T) {
	limiterMgr := limiter.NewManager(100, 100)

	failHandler := func(ctx context.Context, task *models.Task) error {
		return errors.New("simulated execution error")
	}

	var requeuedTasks []*models.Task
	var mu sync.Mutex

	requeueFn := func(task *models.Task) {
		mu.Lock()
		requeuedTasks = append(requeuedTasks, task)
		mu.Unlock()
	}

	exec := worker.NewDefaultExecutor(limiterMgr, failHandler)
	inChan := make(chan *models.Task, 5)

	pool := worker.NewWorkerPool(2, inChan, exec, requeueFn)
	pool.Start()

	// Task with MaxRetries = 1
	task := &models.Task{
		ID:         "task-fail",
		TenantID:   "tenant-1",
		Action:     "fail_job",
		MaxRetries: 1,
		RetryCount: 0,
	}

	inChan <- task
	close(inChan)

	time.Sleep(100 * time.Millisecond)
	pool.Stop()

	stats := pool.Stats()
	if stats.Failed != 1 {
		t.Fatalf("expected 1 failed task, got %d", stats.Failed)
	}

	mu.Lock()
	if len(requeuedTasks) != 1 {
		t.Fatalf("expected 1 task requeued for retry, got %d", len(requeuedTasks))
	}
	mu.Unlock()
}
