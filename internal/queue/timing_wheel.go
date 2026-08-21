package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/models"
)

var (
	ErrTimingWheelStopped = errors.New("timing wheel is stopped")
	ErrNilTask            = errors.New("cannot schedule nil task")
)

// taskHolder wraps a Task with its remaining revolution round count in the wheel.
type taskHolder struct {
	task   *models.Task
	rounds int64
}

// slot represents a discrete time bucket in the timing wheel ring buffer.
type slot struct {
	tasks []*taskHolder
}

// TimingWheel implements an in-memory O(1) circular ring buffer scheduler for delayed tasks.
type TimingWheel struct {
	mu           sync.RWMutex
	tickInterval time.Duration
	wheelSize    int
	currentSlot  int
	slots        []*slot
	outChan      chan *models.Task
	stopChan     chan struct{}
	running      bool
	wg           sync.WaitGroup
}

// NewTimingWheel constructs a TimingWheel ring buffer.
// tickInterval is the duration per slot tick (e.g., 100ms or 1s).
// wheelSize is the total number of slots in the ring buffer (e.g. 60 or 3600).
// bufferSize is the capacity of the output channel emitting due tasks.
func NewTimingWheel(tickInterval time.Duration, wheelSize int, bufferSize int) *TimingWheel {
	if tickInterval <= 0 {
		tickInterval = time.Second
	}
	if wheelSize <= 0 {
		wheelSize = 60
	}
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	slots := make([]*slot, wheelSize)
	for i := 0; i < wheelSize; i++ {
		slots[i] = &slot{
			tasks: make([]*taskHolder, 0),
		}
	}

	return &TimingWheel{
		tickInterval: tickInterval,
		wheelSize:    wheelSize,
		currentSlot:  0,
		slots:        slots,
		outChan:      make(chan *models.Task, bufferSize),
		stopChan:     make(chan struct{}),
	}
}

// TasksChan exposes the read-only output channel emitting tasks that have reached their execution time.
func (tw *TimingWheel) TasksChan() <-chan *models.Task {
	return tw.outChan
}

// Start launches the background ticker goroutine that advances the wheel slots.
func (tw *TimingWheel) Start(ctx context.Context) {
	tw.mu.Lock()
	if tw.running {
		tw.mu.Unlock()
		return
	}
	tw.running = true
	tw.mu.Unlock()

	tw.wg.Add(1)
	go tw.run(ctx)
}

// Stop gracefully halts the timing wheel background ticker loop and closes the output channel.
func (tw *TimingWheel) Stop() {
	tw.mu.Lock()
	if !tw.running {
		tw.mu.Unlock()
		return
	}
	tw.running = false
	close(tw.stopChan)
	tw.mu.Unlock()

	tw.wg.Wait()
	close(tw.outChan)
}

// AddTask places a task into the timing wheel. If the task is due immediately or in the past,
// it is dispatched directly to the output channel.
func (tw *TimingWheel) AddTask(task *models.Task) error {
	if task == nil {
		return ErrNilTask
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.running && isClosed(tw.stopChan) {
		return ErrTimingWheelStopped
	}

	now := time.Now()

	// If task execution time is now or in the past, dispatch directly to output channel
	if task.IsDue(now) {
		select {
		case tw.outChan <- task:
		default:
			// If outChan is full, spawn a transient goroutine so caller is non-blocking
			go func(t *models.Task) {
				tw.outChan <- t
			}(task)
		}
		return nil
	}

	// Calculate delay and slot target
	delay := task.ExecuteAt.Sub(now)
	totalTicks := int64(delay / tw.tickInterval)
	if totalTicks < 1 {
		totalTicks = 1
	}

	targetSlot := (tw.currentSlot + int(totalTicks%int64(tw.wheelSize))) % tw.wheelSize
	rounds := totalTicks / int64(tw.wheelSize)

	holder := &taskHolder{
		task:   task,
		rounds: rounds,
	}

	tw.slots[targetSlot].tasks = append(tw.slots[targetSlot].tasks, holder)
	return nil
}

// PendingCount returns the total number of tasks currently buffered across all slots in the timing wheel.
func (tw *TimingWheel) PendingCount() int {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	total := 0
	for _, s := range tw.slots {
		total += len(s.tasks)
	}
	return total
}

// run is the internal background ticking loop.
func (tw *TimingWheel) run(ctx context.Context) {
	defer tw.wg.Done()
	ticker := time.NewTicker(tw.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tw.stopChan:
			return
		case <-ticker.C:
			tw.tick()
		}
	}
}

// tick advances currentSlot by 1 and processes all tasks in the slot.
func (tw *TimingWheel) tick() {
	tw.mu.Lock()

	tw.currentSlot = (tw.currentSlot + 1) % tw.wheelSize
	activeSlot := tw.slots[tw.currentSlot]

	if len(activeSlot.tasks) == 0 {
		tw.mu.Unlock()
		return
	}

	dueTasks := make([]*models.Task, 0)
	remainingTasks := make([]*taskHolder, 0, len(activeSlot.tasks))

	for _, holder := range activeSlot.tasks {
		if holder.rounds <= 0 {
			dueTasks = append(dueTasks, holder.task)
		} else {
			holder.rounds--
			remainingTasks = append(remainingTasks, holder)
		}
	}

	activeSlot.tasks = remainingTasks
	tw.mu.Unlock()

	// Emit due tasks outside of lock
	for _, task := range dueTasks {
		select {
		case tw.outChan <- task:
		case <-tw.stopChan:
			return
		}
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
