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
		ExecuteAt:  now.Add(-100 * time.Millisecond),
		MaxRetries: 3,
		RetryCount: 1,
	}

	if !task.IsDue(now) {
		t.Fatal("expected task with past ExecuteAt to be due")
	}

	if !task.CanRetry() {
		t.Fatal("expected task with RetryCount < MaxRetries to be retryable")
	}

	// RetryCount = 1 -> Backoff should be baseDelay * 2^1 = 2 * baseDelay
	backoff := task.NextBackoff(1 * time.Second)
	if backoff != 2*time.Second {
		t.Fatalf("expected 2s backoff, got %v", backoff)
	}
}
