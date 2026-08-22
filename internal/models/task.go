package models

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// TaskStatus represents the lifecycle state of a Task.
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"
	StatusRunning   TaskStatus = "RUNNING"
	StatusCompleted TaskStatus = "COMPLETED"
	StatusFailed    TaskStatus = "FAILED"
	StatusDLQ       TaskStatus = "DLQ"
)

// Common model validation errors.
var (
	ErrInvalidTaskID    = errors.New("task ID cannot be empty")
	ErrInvalidTenantID  = errors.New("tenant ID cannot be empty")
	ErrInvalidAction    = errors.New("task action cannot be empty")
	ErrNegativeMaxRetry = errors.New("max_retries cannot be negative")
)

// Task represents a delayed or immediate background job unit in the system.
type Task struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id"`
	Action         string         `json:"action"`
	Endpoint       string         `json:"endpoint,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
	ExecuteAt      time.Time      `json:"execute_at"`
	MaxRetries     int            `json:"max_retries"`
	RetryCount     int            `json:"retry_count"`
	Status         TaskStatus     `json:"status"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	LastError      string         `json:"last_error,omitempty"`
}

// Validate checks if essential fields of the Task are populated and valid.
func (t *Task) Validate() error {
	if t.ID == "" {
		return ErrInvalidTaskID
	}
	if t.TenantID == "" {
		return ErrInvalidTenantID
	}
	if t.Action == "" {
		return ErrInvalidAction
	}
	if t.MaxRetries < 0 {
		return ErrNegativeMaxRetry
	}
	return nil
}

// IsDue reports whether the task's scheduled execution time is at or before the given target time.
func (t *Task) IsDue(target time.Time) bool {
	return !t.ExecuteAt.After(target) // executeAt.After() returns true if executeAt is strictly in the future compared to target
}

// CanRetry determines if the task is eligible for another execution retry based on MaxRetries.
func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries
}

// NextBackoff calculates the exponential backoff delay for the next retry attempt.
// Formula: baseDelay * 2^(RetryCount) with capped maximum to avoid overflow.
func (t *Task) NextBackoff(baseDelay time.Duration) time.Duration {
	if baseDelay <= 0 {
		baseDelay = time.Second
	}
	multiplier := math.Pow(2, float64(t.RetryCount))
	backoff := time.Duration(multiplier) * baseDelay

	// Cap max backoff at 1 hour for safety
	maxBackoff := 1 * time.Hour
	if backoff > maxBackoff || backoff < 0 { // check overflow
		return maxBackoff
	}
	return backoff
}

// String returns a human-readable string representation of the Task.
func (t *Task) String() string {
	return fmt.Sprintf("Task[ID=%s, Tenant=%s, Action=%s, Status=%s, Retry=%d/%d, ExecuteAt=%s]",
		t.ID, t.TenantID, t.Action, t.Status, t.RetryCount, t.MaxRetries, t.ExecuteAt.Format(time.RFC3339))
}
