package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pranelagrawal/gotaskengine/internal/models"
)

var (
	ErrTimingWHeelStopped = errors.New("Timing wheel is stopped")
	ErrNIlTask            = errors.New("Cannot schedule nil task")
)

// THis function will have information about how many full clock rounds does the task need before it can be executed/
// For example, if the task is scheduled after 120 s but our wheel has 60 slots, then round=2.
type taskHolder struct {
	task   *models.Task
	rounds int64
}

// slot will have informaiton about tasks that needs to be performed for that second
// here we take taskHOlder and not task bcz for task with T=10 and T=70 s, if wheel is of 60 s, both will have 10 but having extra information abotu rounds will help us not to choose the later one
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

	slots := make([]*slot, wheelSize) //making "wheelsize" times array of slot
	for i := 0; i < wheelSize; i++ {
		slots[i] = &slot{ // & means slots[i] contains the pointer to the slot
			tasks: make([]*taskHolder, 0), // for each slot, a taskholder array is created with nothing inside it, size=0
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

// AddTask places a task into the timing wheel. If the task is due immediately or in the past,
// it is dispatched directly to the output channel.
func (tw *TimingWheel) AddTask(task *models.Task) error {
	if task == nil {
		return ErrNIlTask
	}

	tw.mu.Lock()
	defer tw.mu.Unlock() // this makes sure that no matter whether there is an error or not the timingwheel will get unlocked once the process is completed

	if !tw.running && isClosed(tw.stopChan) { // this checks if the timingwheel is stopped, bcz after that it doesnt make sense to add anything to it and about closed see the isclosed function
		return ErrTimingWHeelStopped
	}

	now := time.Now()

	if task.IsDue(now) {
		select {
		case tw.outChan <- task:
		default: // this function is called if the outchan buffer is full
			go func(t *models.Task) { // go function launches a new thread whose work in here is to stay until the outchan has the space to include this task, once it has it can independently add the task into outchan
				tw.outChan <- t
			}(task)
		}
		return nil // this is called immediately and the lock is released, the go func has a diff thread which wont cause any delay and work concurrently.
	}

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

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true // if the channel is closed, then it will read zero value meaning it will go inside this block and return true, this means that the channel is closed and that it isnt running
	default:
		return false // here, if the channel is open, then go will have to read ch, however go does not wait and hence it skips the case and enters this block and return false, meaning that the channel is not closed and can read.
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

	for _, holder := range activeSlot.tasks { // _ is just the index, i dont require the use of index in this and hence _
		if holder.rounds <= 0 {
			dueTasks = append(dueTasks, holder.task)
		} else {
			holder.rounds--
			remainingTasks = append(remainingTasks, holder)
		}
	}

	activeSlot.tasks = remainingTasks
	tw.mu.Unlock() // we do unlock first bcz sending items to channel make take some time, so by unlocking before, i ensure that other threads calling addtask() are not blocked.

	for _, task := range dueTasks {
		select {
		case tw.outChan <- task: // this basically adds the task in the outchan and send it to the worker node
		case <-tw.stopChan:
			return //if someone triggers stop() then it will close preventing tick() from hanging forever if workers have stopped reading outchan
		}
	}
}

// PendingCount returns the total number of tasks currently buffered across all slots in the timing wheel.
func (tw *TimingWheel) PendingCount() int {
	tw.mu.RLock() // we do r lock bcz if there are 100 reading requests, they dont need to wait in line, they can read these parallely
	defer tw.mu.RUnlock()

	total := 0
	for _, s := range tw.slots {
		total += len(s.tasks) // returns the total tasks that are inside all slots
	}
	return total
}

// run is the internal background ticking loop.
func (tw *TimingWheel) run(ctx context.Context) {
	defer tw.wg.Done() //When run() eventually exits (on shutdown), defer tw.wg.Done() decrements the counter
	ticker := time.NewTicker(tw.tickInterval)
	defer ticker.Stop() //ensures that when run() exits, the OS timer is turned off so it doesn't waste CPU cycles or leak resources.

	for {
		select {
		case <-ctx.Done():
			return // Signal 1: Parent Context canceled (e.g. Ctrl+C or SIGTERM). Exit loop!
		case <-tw.stopChan:
			return // Signal 2: Manual tw.Stop() was called. Exit loop!
		case <-ticker.C:
			tw.tick() // Signal 3: 100ms elapsed! Advance clock hand by calling tick()!
		}
	}
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

	tw.wg.Add(1)   //1 background thread is active
	go tw.run(ctx) // here we write go bcz it launches the run(ctx) in different thread since it is a forever loop.
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

// so lets say there are 60 slot inside slots, now the task is in slot 59, so till 59 it will exit in less than 1 ns and go to sleep and then execute the task in slot 59.
// and if all slot are empty it just keeps on exiting in 1 ns and sleeps for the remaining of the 1s for every slot
