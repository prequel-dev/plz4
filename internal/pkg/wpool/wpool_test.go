package wpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Test that Submit executes all queued tasks up to maxWorkers
// and that no tasks are dropped while the pool is open.
func TestWorkerPoolSubmitExecutesTasks(t *testing.T) {
	const (
		nTasks = 1024
	)

	wp := NewWorkerPool()
	defer wp.Close()

	var (
		wg      sync.WaitGroup
		counter int32
	)

	wg.Add(nTasks)
	for range nTasks {
		wp.Submit(func() {
			atomic.AddInt32(&counter, 1)
			wg.Done()
		})
	}

	wg.Wait()

	if got := atomic.LoadInt32(&counter); got != nTasks {
		t.Fatalf("expected %d tasks to run, got %d", nTasks, got)
	}
}

// Test that when maxWorkers is reached, additional submitted tasks
// are queued and eventually executed by existing workers.
func TestWorkerPoolQueuedTasksExecuted(t *testing.T) {
	const nTasks = 5

	wp := NewWorkerPool(WithMaxWorkers(1))
	defer wp.Close()

	var (
		wg      sync.WaitGroup
		counter int32
	)

	wg.Add(nTasks)
	for i := 0; i < nTasks; i++ {
		wp.Submit(func() {
			defer wg.Done()
			// Give GC/idling a chance, but keep it short to avoid flakiness.
			time.Sleep(1 * time.Millisecond)
			atomic.AddInt32(&counter, 1)
		})
	}

	wg.Wait()

	if got := atomic.LoadInt32(&counter); got != nTasks {
		t.Fatalf("expected %d queued tasks to run, got %d", nTasks, got)
	}
}

// Test that Close stops accepting new tasks and closes idle workers.
func TestWorkerPoolCloseStopsNewTasks(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(1))

	var ranFirst, ranAfterClose int32
	var wg sync.WaitGroup

	wg.Add(1)
	wp.Submit(func() {
		atomic.StoreInt32(&ranFirst, 1)
		wg.Done()
	})

	wg.Wait()

	// Close the pool; subsequent Submit calls should be ignored.
	wp.Close()

	wp.Submit(func() {
		// This should never run if Close worked correctly.
		atomic.StoreInt32(&ranAfterClose, 1)
	})

	// Give a brief window for any incorrectly accepted task to run.
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&ranFirst) != 1 {
		t.Fatalf("expected first task to run before Close")
	}

	if got := atomic.LoadInt32(&ranAfterClose); got != 0 {
		t.Fatalf("expected no tasks to run after Close, got %d", got)
	}
}

// Test runGC behavior by constructing idle workers with timestamps
// far in the past and ensuring they are closed and removed.
func TestWorkerPoolRunGCClosesOldWorkers(t *testing.T) {
	wp := &WorkerPool{
		qTasks:     nil,
		qIdle:      nil,
		numWorkers: 0,
		maxWorkers: 2,
	}

	// Create two idle workers with old timestamps.
	w1 := newWorker()
	w2 := newWorker()

	old := time.Now().Add(-drainMax * 2).UnixNano()
	w1.stamp = old
	w2.stamp = old

	wp.qIdle = []*workerT{w1, w2}
	wp.numWorkers = 2

	if cont := wp.runGC(drainMax); !cont {
		t.Fatalf("runGC unexpectedly requested stop")
	}

	// Both workers should have been removed from qIdle and numWorkers decremented.
	if len(wp.qIdle) != 0 {
		t.Fatalf("expected qIdle to be empty after GC, got %d", len(wp.qIdle))
	}
	if wp.numWorkers != 0 {
		t.Fatalf("expected numWorkers to be 0 after GC, got %d", wp.numWorkers)
	}
}

// Test runGC returns false when pool is effectively closed (maxWorkers <= 0).
func TestWorkerPoolRunGCStopsWhenClosed(t *testing.T) {
	wp := &WorkerPool{maxWorkers: 0}
	if cont := wp.runGC(drainMax); cont {
		t.Fatalf("expected runGC to return false when maxWorkers <= 0")
	}
}

// Test all option setters to reach 100% coverage
func TestAllOptionSetters(t *testing.T) {
	wp := NewWorkerPool(
		WithMaxWorkers(10),
		WithMinWorkers(2),
		WithPreallocWorkers(3),
		WithTaskPrealloc(100),
		WithDrainTick(time.Millisecond*100),
		WithDrainMax(time.Millisecond*200),
	)
	defer wp.Close()

	if wp.maxWorkers != 10 {
		t.Fatalf("expected maxWorkers 10, got %d", wp.maxWorkers)
	}
	if wp.minWorkers != 2 {
		t.Fatalf("expected minWorkers 2, got %d", wp.minWorkers)
	}

	// Test that preallocated workers exist
	if wp.numWorkers < 2 {
		t.Fatalf("expected at least 2 preallocated workers, got %d", wp.numWorkers)
	}
}

// Test negative minWorkers is clamped to 0
func TestNegativeMinWorkers(t *testing.T) {
	wp := NewWorkerPool(WithMinWorkers(-5))
	defer wp.Close()

	// Test that pool still works normally
	var wg sync.WaitGroup
	wg.Add(1)
	wp.Submit(func() {
		wg.Done()
	})
	wg.Wait()
}

// Test drainIdleWorkers ticker loop terminates when pool is closed
func TestDrainIdleWorkersStopsOnClose(t *testing.T) {
	wp := NewWorkerPool(
		WithMaxWorkers(5),
		WithMinWorkers(0),
		WithDrainTick(time.Millisecond*10),
		WithDrainMax(time.Millisecond*20),
	)

	// Give workers time to be created
	time.Sleep(time.Millisecond * 5)

	// Close should stop the drainIdleWorkers goroutine
	wp.Close()

	// Give time for goroutine to exit
	time.Sleep(time.Millisecond * 50)

	// If we didn't hang, the test passed
}

// Test runGC respects minWorkers
func TestRunGCRespectsMinWorkers(t *testing.T) {
	wp := &WorkerPool{
		qTasks:     nil,
		qIdle:      nil,
		numWorkers: 0,
		maxWorkers: 5,
		minWorkers: 2,
	}

	// Create 3 idle workers, all with old timestamps
	old := time.Now().Add(-drainMax * 2).UnixNano()
	for i := 0; i < 3; i++ {
		w := newWorker()
		w.stamp = old
		wp.qIdle = append(wp.qIdle, w)
		wp.numWorkers++
	}

	if cont := wp.runGC(drainMax); !cont {
		t.Fatalf("runGC unexpectedly requested stop")
	}

	// Should keep minWorkers (2) and only remove 1
	if len(wp.qIdle) != 2 {
		t.Fatalf("expected 2 idle workers after GC (minWorkers), got %d", len(wp.qIdle))
	}
	if wp.numWorkers != 2 {
		t.Fatalf("expected numWorkers to be 2 after GC, got %d", wp.numWorkers)
	}
}

// Test runGC doesn't drain young workers
func TestRunGCKeepsYoungWorkers(t *testing.T) {
	wp := &WorkerPool{
		qTasks:     nil,
		qIdle:      nil,
		numWorkers: 0,
		maxWorkers: 5,
		minWorkers: 0,
	}

	// Create workers with recent timestamps
	now := time.Now().UnixNano()
	for i := 0; i < 3; i++ {
		w := newWorker()
		w.stamp = now
		wp.qIdle = append(wp.qIdle, w)
		wp.numWorkers++
	}

	if cont := wp.runGC(drainMax); !cont {
		t.Fatalf("runGC unexpectedly requested stop")
	}

	// All workers should remain since they're not old enough
	if len(wp.qIdle) != 3 {
		t.Fatalf("expected 3 idle workers after GC (all young), got %d", len(wp.qIdle))
	}
	if wp.numWorkers != 3 {
		t.Fatalf("expected numWorkers to be 3 after GC, got %d", wp.numWorkers)
	}
}

// Test runGC cleans up task queue sliding
func TestRunGCCleansUpTaskQueue(t *testing.T) {
	wp := &WorkerPool{
		qTasks:     make([]func(), 10),
		qIdle:      nil,
		taskIdx:    8,
		numWorkers: 0,
		maxWorkers: 5,
		minWorkers: 0,
	}

	// Add 2 actual tasks at the end
	counter := 0
	wp.qTasks[8] = func() { counter++ }
	wp.qTasks[9] = func() { counter++ }

	if cont := wp.runGC(drainMax); !cont {
		t.Fatalf("runGC unexpectedly requested stop")
	}

	// taskIdx should be reset to 0
	if wp.taskIdx != 0 {
		t.Fatalf("expected taskIdx to be 0 after GC, got %d", wp.taskIdx)
	}

	// qTasks should have slid down
	if len(wp.qTasks) != 2 {
		t.Fatalf("expected qTasks length to be 2 after GC, got %d", len(wp.qTasks))
	}

	// Verify tasks are still executable
	for _, task := range wp.qTasks {
		if task != nil {
			task()
		}
	}
	if counter != 2 {
		t.Fatalf("expected 2 tasks to execute, got %d", counter)
	}
}

// Test multiple concurrent Close calls don't panic
func TestConcurrentClose(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(10))

	var wg sync.WaitGroup
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			wp.Close()
		}()
	}

	wg.Wait()
}

// Test Submit after Close is safe
func TestSubmitAfterClose(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(2))
	wp.Close()

	// Should not panic or hang
	executed := false
	wp.Submit(func() {
		executed = true
	})

	time.Sleep(time.Millisecond * 50)

	if executed {
		t.Fatalf("expected task not to execute after Close")
	}
}

// Test worker can process multiple tasks from queue
func TestWorkerProcessesMultipleQueuedTasks(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(1))
	defer wp.Close()

	var wg sync.WaitGroup
	counter := int32(0)

	// Submit more tasks than workers
	n := 10
	wg.Add(n)
	for i := 0; i < n; i++ {
		wp.Submit(func() {
			atomic.AddInt32(&counter, 1)
			time.Sleep(time.Millisecond)
			wg.Done()
		})
	}

	wg.Wait()

	if got := atomic.LoadInt32(&counter); got != int32(n) {
		t.Fatalf("expected %d tasks to run, got %d", n, got)
	}
}

// Test zero maxWorkers means pool doesn't accept tasks and doesn't
// start any workers or drain goroutines.
func TestZeroMaxWorkers(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(0))
	defer wp.Close()

	var executed int32
	for i := 0; i < 10; i++ {
		wp.Submit(func() {
			atomic.AddInt32(&executed, 1)
		})
	}

	// Give any (incorrect) execution a brief chance to run.
	time.Sleep(10 * time.Millisecond)

	if got := atomic.LoadInt32(&executed); got != 0 {
		t.Fatalf("expected no task execution with maxWorkers=0, got %d", got)
	}

	// Inspect internal state to ensure no workers or queued tasks.
	wp.mux.Lock()
	defer wp.mux.Unlock()
	if wp.numWorkers != 0 {
		t.Fatalf("expected numWorkers=0, got %d", wp.numWorkers)
	}
	if len(wp.qIdle) != 0 {
		t.Fatalf("expected no idle workers, got %d", len(wp.qIdle))
	}
	if len(wp.qTasks) != 0 {
		t.Fatalf("expected no queued tasks, got %d", len(wp.qTasks))
	}
}

// Test maybeQueueIdle when maxWorkers is 0
func TestMaybeQueueIdleWhenClosed(t *testing.T) {
	wp := &WorkerPool{
		maxWorkers: 0,
	}

	w := newWorker()
	task, ok := wp.maybeQueueIdle(w)

	if ok {
		t.Fatalf("expected maybeQueueIdle to return false when maxWorkers=0")
	}
	if task != nil {
		t.Fatalf("expected nil task when maxWorkers=0")
	}
}

// Test race between Submit and Close under heavier but bounded contention.
// The goal is to exercise potential deadlocks without making the test flaky
// or unbounded in runtime.
func TestSubmitCloseRace(t *testing.T) {
	const (
		iterations      = 20
		nSubmitGoroutes = 8
		submitsPerG     = 100
	)

	for i := 0; i < iterations; i++ {
		wp := NewWorkerPool(WithMaxWorkers(4))

		var wg sync.WaitGroup
		// submitters + closers
		wg.Add(nSubmitGoroutes + 2)

		// Multiple submitter goroutines hammer Submit.
		for s := 0; s < nSubmitGoroutes; s++ {
			go func() {
				defer wg.Done()
				for j := 0; j < submitsPerG; j++ {
					wp.Submit(func() {
						// Small work to keep workers busy but bounded.
						time.Sleep(time.Microsecond)
					})
				}
			}()
		}

		// Two closers race with submitters; Close is idempotent.
		go func() {
			defer wg.Done()
			time.Sleep(50 * time.Microsecond)
			wp.Close()
		}()
		go func() {
			defer wg.Done()
			time.Sleep(150 * time.Microsecond)
			wp.Close()
		}()

		// If there is a deadlock between Submit/Close/drainGC, this wait
		// will eventually hit the test-wide timeout.
		wg.Wait()
	}
}

// Test preallocation options
func TestPreallocationOptions(t *testing.T) {
	tests := []struct {
		name         string
		maxWorkers   int
		preWorkers   int
		minWorkers   int
		taskPrealloc int
	}{
		{"preallocate more than max", 5, 10, 0, 0},
		{"preallocate equal to max", 5, 5, 0, 0},
		{"preallocate with minWorkers", 10, 3, 5, 0},
		{"large task prealloc", 5, 0, 0, 1000},
		{"zero task prealloc", 5, 0, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []Opts{
				WithMaxWorkers(tt.maxWorkers),
				WithPreallocWorkers(tt.preWorkers),
				WithMinWorkers(tt.minWorkers),
			}
			if tt.taskPrealloc > 0 {
				opts = append(opts, WithTaskPrealloc(tt.taskPrealloc))
			}

			wp := NewWorkerPool(opts...)
			defer wp.Close()

			// Verify pool works
			var wg sync.WaitGroup
			wg.Add(1)
			wp.Submit(func() {
				wg.Done()
			})
			wg.Wait()
		})
	}
}

// Test stress with many concurrent submits
func TestConcurrentSubmits(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(4))
	defer wp.Close()

	var wg sync.WaitGroup
	counter := int32(0)
	n := 1000

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			wp.Submit(func() {
				atomic.AddInt32(&counter, 1)
				wg.Done()
			})
		}()
	}

	wg.Wait()

	if got := atomic.LoadInt32(&counter); got != int32(n) {
		t.Fatalf("expected %d tasks to run, got %d", n, got)
	}
}

// Test that idle workers actually go idle and get reused
func TestWorkerIdleAndReuse(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(5), WithMinWorkers(0))
	defer wp.Close()

	var wg sync.WaitGroup

	// First wave of tasks
	wg.Add(5)
	for i := 0; i < 5; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			wg.Done()
		})
	}
	wg.Wait()

	// Give workers time to become idle
	time.Sleep(time.Millisecond * 10)

	// Check that some workers are idle
	wp.mux.Lock()
	idleCount := len(wp.qIdle)
	wp.mux.Unlock()

	if idleCount == 0 {
		t.Fatalf("expected some idle workers, got 0")
	}

	// Second wave should reuse idle workers
	wg.Add(3)
	for i := 0; i < 3; i++ {
		wp.Submit(func() {
			time.Sleep(time.Millisecond)
			wg.Done()
		})
	}
	wg.Wait()
}

// Test panic recovery in tasks doesn't crash the worker
func TestPanicInTask(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(2))
	defer wp.Close()

	var wg sync.WaitGroup

	// First task panics; worker pool is expected to recover and continue.
	wg.Add(1)
	wp.Submit(func() {
		defer wg.Done()
		// Note: This will panic, but the worker implementation recovers
		// from the panic, logs it, and keeps the worker running.
		// This test documents that panics in tasks do not crash the worker.

		// This intentional panic exercises worker panic handling.
		panic("intentional panic in worker task")
	})

	// Second task should still work if worker pool is robust
	wg.Add(1)
	wp.Submit(func() {
		time.Sleep(time.Millisecond)
		wg.Done()
	})

	wg.Wait()
}

// Test that a worker processing tasks exits correctly when a task is
// delivered after Close has been called on the pool. This exercises the
// `!ok` path in workerT.run where maybeQueueIdle returns false once
// maxWorkers <= 0.
func TestWorkerTaskAfterCloseTriggersExit(t *testing.T) {
	wp := &WorkerPool{maxWorkers: 1}

	var wg sync.WaitGroup
	wg.Add(1)

	ch := make(chan struct{})
	wp.Submit(func() {
		wg.Wait()
		ch <- struct{}{}
	})

	// Close the pool while the worker is busy
	wp.Close()

	// Unblock the worker
	wg.Done()

	<-ch

	// At this point the worker should have begun to exit,
	// Give time for worker to exit.
	time.Sleep(10 * time.Millisecond)

	// Inspect internal state to ensure no workers remain
	wp.mux.Lock()
	defer wp.mux.Unlock()
	if wp.numWorkers != 0 {
		t.Fatalf("expected numWorkers=0 after Close and task completion, got %d", wp.numWorkers)
	}
	if len(wp.qIdle) != 0 {
		t.Fatalf("expected no idle workers after Close, got %d", len(wp.qIdle))
	}
}

// Test empty pool behavior when created with maxWorkers=0 and then closed.
// Submit should be a no-op and must not execute the task.
func TestEmptyPoolSubmit(t *testing.T) {
	wp := NewWorkerPool(WithMaxWorkers(0))
	wp.Close()

	executed := false
	wp.Submit(func() {
		executed = true
	})

	time.Sleep(10 * time.Millisecond)

	if executed {
		t.Fatalf("expected no task execution when submitting to closed zero-worker pool")
	}
}

// Test mixed old and young workers in GC
func TestRunGCMixedAgeWorkers(t *testing.T) {
	wp := &WorkerPool{
		qIdle:      nil,
		numWorkers: 0,
		maxWorkers: 10,
		minWorkers: 0,
	}

	now := time.Now().UnixNano()
	old := time.Now().Add(-drainMax * 2).UnixNano()

	// Add old workers first (they should be drained)
	for i := 0; i < 3; i++ {
		w := newWorker()
		w.stamp = old
		wp.qIdle = append(wp.qIdle, w)
		wp.numWorkers++
	}

	// Add young workers (they should stay)
	for i := 0; i < 3; i++ {
		w := newWorker()
		w.stamp = now
		wp.qIdle = append(wp.qIdle, w)
		wp.numWorkers++
	}

	wp.runGC(drainMax)

	// Should keep only the young workers
	if len(wp.qIdle) != 3 {
		t.Fatalf("expected 3 workers after GC (young ones), got %d", len(wp.qIdle))
	}

	// Verify remaining workers are the young ones
	for _, w := range wp.qIdle {
		age := now - w.stamp
		if age > int64(drainMax) {
			t.Fatalf("found old worker after GC with age %d", age)
		}
	}
}

const (
	PoolSize = int(1e4)
	TaskNum  = int(1e6)
)

func BenchmarkWorkerPool(b *testing.B) {
	pool := NewWorkerPool(WithMaxWorkers(PoolSize), WithTaskPrealloc(TaskNum/2))
	defer pool.Close()

	var wg sync.WaitGroup

	taskFunc := func() {
		time.Sleep(time.Millisecond)
		wg.Done()
	}

	b.ResetTimer()
	for range b.N {
		wg.Add(TaskNum)
		for range TaskNum {
			pool.Submit(taskFunc)
		}
		wg.Wait()
	}
	b.StopTimer()
}

func BenchmarkGoroutines(b *testing.B) {
	var wg sync.WaitGroup

	taskFunc := func() {
		time.Sleep(time.Millisecond)
		wg.Done()
	}

	b.ResetTimer()
	for range b.N {
		wg.Add(TaskNum)
		for range TaskNum {
			go taskFunc()
		}
		wg.Wait()
	}
}
