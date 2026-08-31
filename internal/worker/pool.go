package worker

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/models"
)

// RequeueFunc is a callback invoked when a task needs to be re-queued (e.g. rate limited or retry scheduled).
type RequeueFunc func(task *models.Task)

// PoolStats holds atomic snapshot counters of worker pool performance.
type PoolStats struct {
	Processed uint64 `json:"processed"`
	Completed uint64 `json:"completed"`
	Failed    uint64 `json:"failed"`
	Throttled uint64 `json:"throttled"`
	DLQ       uint64 `json:"dlq"`
} //DLQ stands for Dead Letter Queue.

// WorkerPool manages a fixed set of goroutines processing tasks concurrently.
type WorkerPool struct {
	numWorkers int
	inChan     <-chan *models.Task
	executor   Executor
	requeueFn  RequeueFunc

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics counters
	processed uint64
	completed uint64
	failed    uint64
	throttled uint64
	dlq       uint64
}

// NewWorkerPool initializes a fixed-capacity WorkerPool.
func NewWorkerPool(numWorkers int, inChan <-chan *models.Task, executor Executor, requeueFn RequeueFunc) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = 10
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		numWorkers: numWorkers,
		inChan:     inChan,
		executor:   executor,
		requeueFn:  requeueFn,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start launches the worker pool goroutines.
func (p *WorkerPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

// Stop initiates zero-loss graceful shutdown.
// It cancels the pool context and waits until all active worker goroutines finish processing their in-flight tasks.
func (p *WorkerPool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// Stats returns a snapshot of task metrics processed by the pool.
func (p *WorkerPool) Stats() PoolStats {
	return PoolStats{
		Processed: atomic.LoadUint64(&p.processed),
		Completed: atomic.LoadUint64(&p.completed),
		Failed:    atomic.LoadUint64(&p.failed),
		Throttled: atomic.LoadUint64(&p.throttled),
		DLQ:       atomic.LoadUint64(&p.dlq),
	}
}

// workerLoop is the main event loop for an individual worker goroutine.
func (p *WorkerPool) workerLoop(workerID int) {
	defer p.wg.Done()

	// Example of how workerID is used during debugging/logging:
	log.Printf("[Worker #%d] Started executing task", workerID)

	for {
		select {

		// CASE A: Normal Operation - Reading a task from the TimingWheel channel (p.inChan)
		case task, ok := <-p.inChan:
			if !ok {
				return // Channel was closed (server shutdown). Exit worker
			}
			if task != nil {
				p.processTask(task) // Execute the task
			}

		// CASE B: Shutdown Signal Received (p.ctx.Done())
		case <-p.ctx.Done():

			// ZERO-LOSS DRAIN: Process any remaining tasks left in the channel buffer before exiting!
			for {
				select {
				case task, ok := <-p.inChan:
					if !ok {
						return
					}
					if task != nil {
						p.processTask(task)
					}
				default:
					return
				}
			}
		}
	}
}

// processTask executes a single task and updates metrics & retry routing.
func (p *WorkerPool) processTask(task *models.Task) {
	atomic.AddUint64(&p.processed, 1)

	// Execute task under background context so in-flight execution completes cleanly
	err := p.executor.Execute(context.Background(), task)

	if err == nil {
		atomic.AddUint64(&p.completed, 1)
		return
	}

	if errors.Is(err, ErrRateLimited) {
		atomic.AddUint64(&p.throttled, 1)
		if p.requeueFn != nil {
			task.ExecuteAt = time.Now().Add(200 * time.Millisecond)
			go p.requeueFn(task) // Calls the callback function to re-queue the task!
		}
		return
	}

	// In distributed systems, when a task fails repeatedly (for example, it fails 3 out of 3 retries because a target API is down), then it is set to DLQ (Dead Letter Queue)
	if task.Status == models.StatusDLQ {
		atomic.AddUint64(&p.dlq, 1)
	} else {
		atomic.AddUint64(&p.failed, 1)
		if p.requeueFn != nil {
			go p.requeueFn(task)
		}
	}
}
