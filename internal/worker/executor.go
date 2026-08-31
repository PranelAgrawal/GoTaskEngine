package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/limiter"
	"github.com/pranelagrawal/gotaskengine/internal/models"
)

var (
	ErrRateLimited = errors.New("tenant rate limit exceeded")
)

// TaskHandler represents a custom function type for executing a task's payload.
type TaskHandler func(ctx context.Context, task *models.Task) error

// Executor defines the contract for processing a task unit.
type Executor interface {
	Execute(ctx context.Context, task *models.Task) error
}

// DefaultExecutor handles rate-limiting checks, task execution, retries, and DLQ state transitions.
type DefaultExecutor struct {
	limiterManager *limiter.LimiterManager
	handler        TaskHandler
}

// NewDefaultExecutor creates an executor with the given rate limiter manager and execution handler.
// If handler is nil, a default simulated execution handler is used.
func NewDefaultExecutor(limiterManager *limiter.LimiterManager, handler TaskHandler) *DefaultExecutor {
	if handler == nil {
		handler = defaultSimulatedHandler
	}
	return &DefaultExecutor{
		limiterManager: limiterManager,
		handler:        handler,
	}
}

// Execute checks tenant rate limits and runs the task execution lifecycle.
func (e *DefaultExecutor) Execute(ctx context.Context, task *models.Task) error {
	if task == nil {
		return errors.New("cannot execute nil task")
	}

	// 1. Check Multi-Tenant Token-Bucket Rate Limiter
	if e.limiterManager != nil {
		if !e.limiterManager.Allow(task.TenantID) {
			return ErrRateLimited
		}
	}

	// 2. Mark task as RUNNING
	task.Status = models.StatusRunning
	task.UpdatedAt = time.Now()

	// 3. Perform Execution Handler
	err := e.handler(ctx, task)
	task.UpdatedAt = time.Now()

	if err == nil {
		task.Status = models.StatusCompleted
		return nil
	}

	// 4. Handle Execution Failure & Retries
	task.LastError = err.Error()
	if task.CanRetry() {
		task.RetryCount++
		task.Status = models.StatusFailed
		// Calculate next execution time with exponential backoff
		backoff := task.NextBackoff(1 * time.Second)
		task.ExecuteAt = time.Now().Add(backoff)
		return fmt.Errorf("task execution failed (will retry %d/%d): %w", task.RetryCount, task.MaxRetries, err)
	}

	// Exceeded retries -> Route to Dead-Letter Queue (DLQ)
	task.Status = models.StatusDLQ
	return fmt.Errorf("task exceeded max retries (%d), routed to DLQ: %w", task.MaxRetries, err)
}

func defaultSimulatedHandler(ctx context.Context, task *models.Task) error {
	// Simulate lightweight work
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Millisecond):
		return nil
	}
}
