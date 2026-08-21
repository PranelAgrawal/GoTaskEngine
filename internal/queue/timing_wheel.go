package queue

import (
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

func (tw *TimingWheel) TasksChan() <-chan *models.Task {
	return tw.outChan
}
