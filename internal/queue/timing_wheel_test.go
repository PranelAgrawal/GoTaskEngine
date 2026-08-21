package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/models"
	"github.com/pranelagrawal/gotaskengine/internal/queue"
)

func TestTimingWheel_ImmediateTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tw := queue.NewTimingWheel(50*time.Millisecond, 10, 10)
	tw.Start(ctx)
	defer tw.Stop()

	task := &models.Task{
		ID:        "task-immediate",
		TenantID:  "tenant-1",
		Action:    "send_email",
		ExecuteAt: time.Now().Add(-1 * time.Second), // past
	}

	err := tw.AddTask(task)
	if err != nil {
		t.Fatalf("unexpected error adding immediate task: %v", err)
	}

	select {
	case received := <-tw.TasksChan():
		if received.ID != task.ID {
			t.Fatalf("expected task %s, got %s", task.ID, received.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for immediate task dispatch")
	}
}

func TestTimingWheel_DelayedTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tickInterval := 50 * time.Millisecond
	tw := queue.NewTimingWheel(tickInterval, 10, 10)
	tw.Start(ctx)
	defer tw.Stop()

	// Task delayed by ~150ms (3 ticks)
	delay := 150 * time.Millisecond
	startTime := time.Now()
	task := &models.Task{
		ID:        "task-delayed",
		TenantID:  "tenant-1",
		Action:    "process_payment",
		ExecuteAt: startTime.Add(delay),
	}

	err := tw.AddTask(task)
	if err != nil {
		t.Fatalf("failed to add delayed task: %v", err)
	}

	select {
	case received := <-tw.TasksChan():
		elapsed := time.Since(startTime)
		if received.ID != task.ID {
			t.Fatalf("expected task %s, got %s", task.ID, received.ID)
		}
		if elapsed < 100*time.Millisecond {
			t.Fatalf("task executed too early: elapsed %v", elapsed)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for delayed task dispatch")
	}
}

func TestTimingWheel_ConcurrentAdds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tw := queue.NewTimingWheel(20*time.Millisecond, 20, 100)
	tw.Start(ctx)
	defer tw.Stop()

	var wg sync.WaitGroup
	numTasks := 30

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := &models.Task{
				ID:        "task-concurrent",
				TenantID:  "tenant-1",
				Action:    "ping",
				ExecuteAt: time.Now().Add(time.Duration(id*10) * time.Millisecond),
			}
			_ = tw.AddTask(task)
		}(i)
	}

	wg.Wait()

	receivedCount := 0
	timeout := time.After(2 * time.Second)

	for receivedCount < numTasks {
		select {
		case <-tw.TasksChan():
			receivedCount++
		case <-timeout:
			t.Fatalf("timed out receiving tasks: got %d/%d", receivedCount, numTasks)
		}
	}
}
