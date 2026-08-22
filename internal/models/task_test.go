package models_test

import (
	"testing"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/models"
)

func TestTask_Validate(t *testing.T) {
	validTask := &models.Task{
		ID:         "task-1",
		TenantID:   "tenant-1",
		Action:     "send_webhook",
		MaxRetries: 3,
	}
	if err := validTask.Validate(); err != nil {
		t.Fatalf("expected valid task to pass validation, got: %v", err)
	}

	invalidTask := &models.Task{
		ID:       "",
		TenantID: "tenant-1",
		Action:   "send_webhook",
	}
	if err := invalidTask.Validate(); err == nil {
		t.Fatal("expected error for empty task ID")
	}

	invalidTenant := &models.Task{
		ID:       "task-1",
		TenantID: "",
		Action:   "send_webhook",
	}
	if err := invalidTenant.Validate(); err == nil {
		t.Fatal("expected error for empty tenant ID")
	}
}

func TestTask_IsDueAndBackoff(t *testing.T) {
	now := time.Now()
	task := &models.Task{
		ID:         "task-due",
		TenantID:   "tenant-1",
		Action:     "action",
		ExecuteAt:  now.Add(-100 * time.Millisecond), // 100 ms in the past of current time
		MaxRetries: 3,
		RetryCount: 1,
	}

	if !task.IsDue(now) { // since the task has already passed its execution time, the function inside task.go return true, which is then negated by !, so it becomes false.
		t.Fatal("expected task with past ExecuteAt to be due")
	}

	if !task.CanRetry() {
		t.Fatal("expected task with RetryCount < MaxRetries to be retryable")
	}

	// RetryCount = 1 -> Backoff should be baseDelay * 2^1 = 2 * baseDelay
	backoff := task.NextBackoff(1 * time.Second) // time.Second is simply 1s
	if backoff != 2*time.Second {                // since hardcoded it just checks whether i receive 2 or not
		t.Fatalf("expected 2s backoff, got %v", backoff)
	}
}

func TestTask_MaxBackoffCap(t *testing.T) {
	// High retry count (2^30 seconds would be ~34 years), should cap at 1 hour
	task := &models.Task{
		ID:         "task-overflow",
		TenantID:   "tenant-1",
		Action:     "action",
		RetryCount: 30,
	}

	backoff := task.NextBackoff(1 * time.Second)
	if backoff != 1*time.Hour {
		t.Fatalf("expected backoff capped at 1h, got %v", backoff)
	}
}
