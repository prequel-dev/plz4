package wpool

// Lightweight worker pool implementation.
// Supports dispatch of tasks to workers in order of Submit.
// Supports dynamic scaling of workers between min and max limits,
// with idle workers being drained after a configurable timeout.
//
// Note: In modern golang versions, the built-in goroutine scheduler
// is quite efficient at managing large numbers of goroutines, so
// this worker pool is primarily useful for limiting concurrency
// rather than for performance optimization.

import (
	"runtime"
	"slices"
	"sync"
	"time"
)

const (
	drainTick = time.Second * 10
	drainMax  = time.Second * 30
)

type Opts func(optsT) optsT

type optsT struct {
	maxWorkers  int
	minWorkers  int
	preWorkers  int
	tskPrealloc int
	drainTick   time.Duration
	drainMax    time.Duration
}

func WithMaxWorkers(n int) Opts {
	return func(o optsT) optsT {
		o.maxWorkers = n
		return o
	}
}

func WithMinWorkers(n int) Opts {
	return func(o optsT) optsT {
		o.minWorkers = n
		return o
	}
}

func WithPreallocWorkers(n int) Opts {
	return func(o optsT) optsT {
		o.preWorkers = n
		return o
	}
}

func WithTaskPrealloc(n int) Opts {
	return func(o optsT) optsT {
		o.tskPrealloc = n
		return o
	}
}

func WithDrainTick(d time.Duration) Opts {
	return func(o optsT) optsT {
		o.drainTick = d
		return o
	}
}

func WithDrainMax(d time.Duration) Opts {
	return func(o optsT) optsT {
		o.drainMax = d
		return o
	}
}

func parseOpts(optList ...Opts) optsT {
	o := optsT{
		maxWorkers: runtime.NumCPU(),
		drainTick:  drainTick,
		drainMax:   drainMax,
	}

	for _, opt := range optList {
		o = opt(o)
	}

	return o
}

type WorkerPool struct {
	mux        sync.Mutex
	qTasks     []func()
	qIdle      []*workerT
	taskIdx    int
	numWorkers int
	maxWorkers int
	minWorkers int
}

type workerT struct {
	stamp int64
	ch    chan func()
}

func NewWorkerPool(opts ...Opts) *WorkerPool {
	o := parseOpts(opts...)

	nTask := o.tskPrealloc
	if nTask <= 0 {
		nTask = o.maxWorkers
	}

	wp := &WorkerPool{
		qTasks:     make([]func(), 0, nTask),
		qIdle:      make([]*workerT, 0, o.maxWorkers),
		maxWorkers: o.maxWorkers,
		minWorkers: o.minWorkers,
	}

	if o.minWorkers < 0 {
		o.minWorkers = 0
	}

	nAlloc := min(o.maxWorkers, max(o.preWorkers, o.minWorkers))
	for i := 0; i < nAlloc; i++ {
		w := newWorker()
		wp.numWorkers++
		wp.qIdle = append(wp.qIdle, w)
		go w.run(wp)
	}

	if wp.maxWorkers > 0 {
		go wp.drainIdleWorkers(o.drainTick, o.drainMax)
	}

	return wp
}

func (wp *WorkerPool) drainIdleWorkers(drainTick, drainMax time.Duration) {
	ticker := time.NewTicker(drainTick)
	defer ticker.Stop()

	for range ticker.C {
		if !wp.runGC(drainMax) {
			return
		}
	}
}

func (wp *WorkerPool) runGC(drainMax time.Duration) bool {
	wp.mux.Lock()
	defer wp.mux.Unlock()

	if wp.maxWorkers <= 0 {
		return false
	}

	now := time.Now().UnixNano()

	i := 0

	for ; i < len(wp.qIdle); i++ {

		// Break out of work if we reach minWorkers
		if len(wp.qIdle)-i <= wp.minWorkers {
			break
		}

		w := wp.qIdle[i]

		age := now - w.stamp

		if age < int64(drainMax) {
			break
		}

		// Force the worker closed
		close(w.ch)
	}

	// Fix up the idleQ
	if i > 0 {
		wp.qIdle = wp.qIdle[i:]
		wp.numWorkers -= i
	}

	// Slide tasks down if necessary;
	// TODO consider trim down allocation if too large
	if wp.taskIdx > 0 {
		wp.qTasks = slices.Delete(wp.qTasks, 0, wp.taskIdx)
		wp.taskIdx = 0
	}

	return true
}

func (wp *WorkerPool) Close() {
	wp.mux.Lock()
	defer wp.mux.Unlock()

	wp.maxWorkers = 0

	for _, w := range wp.qIdle {
		close(w.ch)
	}
	wp.qIdle = nil
	wp.qTasks = nil
	wp.taskIdx = 0
}

func (wp *WorkerPool) Submit(task func()) {
	wp.mux.Lock()

	// If there is an idle worker, give it the task
	if nq := len(wp.qIdle); nq > 0 {
		w := wp.qIdle[nq-1]
		wp.qIdle = wp.qIdle[:nq-1]
		wp.mux.Unlock()

		w.submit(task)
		return
	}

	if wp.numWorkers < wp.maxWorkers {
		// Create a new worker
		wp.numWorkers++
		wp.mux.Unlock()

		w := newWorker()
		go w.run(wp)
		w.submit(task)
		return
	}

	if wp.maxWorkers > 0 {
		// We are at max workers; queue the task
		wp.qTasks = append(wp.qTasks, task)
	}

	wp.mux.Unlock()
}

func (wp *WorkerPool) maybeQueueIdle(w *workerT) (func(), bool) {
	wp.mux.Lock()
	defer wp.mux.Unlock()

	if wp.maxWorkers <= 0 {
		wp.numWorkers--
		return nil, false
	}

	nt := len(wp.qTasks)

	if nt-wp.taskIdx == 0 {
		// No tasks; queue as idle
		w.stamp = time.Now().UnixNano()
		wp.qIdle = append(wp.qIdle, w)
		return nil, true
	}

	// Grab new tasks from front of task queue
	task := wp.qTasks[wp.taskIdx]
	wp.taskIdx++
	return task, true
}

func newWorker() *workerT {
	return &workerT{
		ch: make(chan func(), 1),
	}
}

func (w *workerT) submit(task func()) {
	w.ch <- task
}

func (w *workerT) run(wp *WorkerPool) {
	for task := range w.ch {
		task()

		for {
			task, ok := wp.maybeQueueIdle(w)
			if !ok {
				return
			}
			if task == nil {
				break
			}
			task()
		}
	}
}
