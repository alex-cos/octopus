package octopus_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alex-cos/octopus"
	"github.com/stretchr/testify/assert"
)

// helper: create a manager with short timeouts for testing.
func testManager(t *testing.T) *octopus.Octopus {
	t.Helper()
	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(5*time.Second),
		octopus.WithLogger(slog.Default()),
	)
	return mgr
}

// simpleWorker blocks until Done is closed, then returns nil.
func simpleWorker(lc octopus.Lifecycle) error {
	<-lc.Done()
	return nil
}

// --- TestStartStop -----------------------------------------------------------

func TestStartStop(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	stopped := make(chan struct{})
	worker := func(lc octopus.Lifecycle) error {
		<-lc.Done()
		close(stopped)
		return nil
	}

	err := mgr.Start(context.Background(), "w1", worker)
	assert.NoError(t, err, "Start failed: %v", err)

	if !mgr.IsRunning("w1") {
		t.Fatal("expected w1 to be running")
	}

	err = mgr.Stop("w1")
	assert.NoError(t, err, "Stop failed: %v", err)

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker to stop")
	}

	if mgr.IsRunning("w1") {
		t.Fatal("expected w1 to be stopped")
	}
}

// --- TestStartDuplicate ------------------------------------------------------

func TestStartDuplicate(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "dup", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)
	defer func() {
		mgr.Stop("dup") //nolint:errcheck
	}()

	err = mgr.Start(context.Background(), "dup", simpleWorker)
	if !errors.Is(err, octopus.ErrWorkerAlreadyRunning) {
		t.Fatalf("expected ErrWorkerAlreadyRunning, got: %v", err)
	}
}

// --- TestStopNotFound --------------------------------------------------------

func TestStopNotFound(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Stop("nonexistent")
	if !errors.Is(err, octopus.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got: %v", err)
	}
}

// --- TestHotSwap -------------------------------------------------------------

func TestHotSwap(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	var generation atomic.Int32
	genReady := make(chan int32, 1)

	workerGen := func(gen int32) octopus.WorkerFunc {
		return func(lc octopus.Lifecycle) error {
			generation.Store(gen)
			select {
			case genReady <- gen:
			default:
			}
			<-lc.Done()
			return nil
		}
	}

	// Start generation 1.
	err := mgr.Start(context.Background(), "svc", workerGen(1))
	assert.NoError(t, err, "Start failed: %v", err)
	select {
	case g := <-genReady:
		if g != 1 {
			t.Fatalf("expected generation 1, got %d", g)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for generation 1")
	}

	// Stop, then start generation 2.
	err = mgr.Stop("svc")
	assert.NoError(t, err, "Stop failed: %v", err)

	err = mgr.Start(context.Background(), "svc", workerGen(2))
	assert.NoError(t, err, "Start failed: %v", err)
	select {
	case g := <-genReady:
		if g != 2 {
			t.Fatalf("expected generation 2, got %d", g)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for generation 2")
	}

	err = mgr.Stop("svc")
	assert.NoError(t, err, "Stop failed: %v", err)
}

// --- TestShutdownAll ---------------------------------------------------------

func TestShutdownAll(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	const n = 5
	for i := range n {
		id := fmt.Sprintf("worker-%d", i)
		if err := mgr.Start(context.Background(), id, simpleWorker); err != nil {
			t.Fatalf("Start %s failed: %v", id, err)
		}
	}

	if a := mgr.Alive(); a != n {
		t.Fatalf("expected %d alive, got %d", n, a)
	}

	if err := mgr.ShutdownAll(); err != nil {
		t.Fatalf("ShutdownAll failed: %v", err)
	}

	mgr.Wait()

	if a := mgr.Alive(); a != 0 {
		t.Fatalf("expected 0 alive after shutdown, got %d", a)
	}
}

// --- TestShutdownPreventsStart -----------------------------------------------

func TestShutdownPreventsStart(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.ShutdownAll()
	assert.NoError(t, err)

	mgr.Wait()

	err = mgr.Start(context.Background(), "late", simpleWorker)
	if !errors.Is(err, octopus.ErrManagerShutdown) {
		t.Fatalf("expected ErrManagerShutdown, got: %v", err)
	}
}

// --- TestWorkerPanic ---------------------------------------------------------

func TestWorkerPanic(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	panicWorker := func(lc octopus.Lifecycle) error {
		panic("boom!")
	}

	if err := mgr.Start(context.Background(), "panicker", panicWorker); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the panicked worker to be cleaned up from the registry.
	assert.Eventually(t, func() bool {
		return !mgr.IsRunning("panicker")
	}, 2*time.Second, 10*time.Millisecond, "panicked worker should have been removed from registry")

	// Should be able to re-start with the same ID after panic.
	if err := mgr.Start(context.Background(), "panicker", simpleWorker); err != nil {
		t.Fatalf("Re-start after panic failed: %v", err)
	}
	err := mgr.Stop("panicker")
	assert.NoError(t, err, "Stop failed: %v", err)
}

// --- TestWorkerTimeout -------------------------------------------------------

func TestWorkerTimeout(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	// A worker that ignores the stop signal.
	stubbornWorker := func(lc octopus.Lifecycle) error {
		// Intentionally ignore Done and block forever.
		select {}
	}

	err := mgr.Start(
		context.Background(),
		"stubborn",
		stubbornWorker,
		octopus.WithWorkerTimeout(200*time.Millisecond),
	)
	assert.NoError(t, err, "Start failed: %v", err)

	start := time.Now()
	err = mgr.Stop("stubborn")
	elapsed := time.Since(start)

	if !errors.Is(err, octopus.ErrStopTimeout) {
		t.Fatalf("expected ErrStopTimeout, got: %v", err)
	}

	// Should have taken roughly the timeout duration.
	if elapsed < 150*time.Millisecond || elapsed > 1*time.Second {
		t.Fatalf("unexpected elapsed time: %v", elapsed)
	}
}

// --- TestGlobalTimeout -------------------------------------------------------

func TestGlobalTimeout(t *testing.T) {
	t.Parallel()

	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(300*time.Millisecond),
		octopus.WithLogger(slog.Default()),
	)

	// Workers that ignore stop signal.
	stubbornWorker := func(lc octopus.Lifecycle) error {
		select {}
	}

	for i := range 3 {
		id := fmt.Sprintf("stubborn-%d", i)
		err := mgr.Start(context.Background(), id, stubbornWorker, octopus.WithWorkerTimeout(10*time.Second))
		assert.NoError(t, err, "Start failed: %v", err)
	}

	start := time.Now()
	err := mgr.ShutdownAll()
	elapsed := time.Since(start)

	if !errors.Is(err, octopus.ErrShutdownTimeout) {
		t.Fatalf("expected ErrShutdownTimeout, got: %v", err)
	}

	if elapsed < 250*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("unexpected elapsed time: %v", elapsed)
	}
}

// --- TestConcurrentStartStop -------------------------------------------------

func TestConcurrentStartStop(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("concurrent-%d", n)

			err := mgr.Start(context.Background(), id, simpleWorker)
			if err != nil {
				return // may get ErrManagerShutdown or duplicate, that's ok
			}

			// Small jitter.
			time.Sleep(time.Duration(n%10) * time.Millisecond)

			err = mgr.Stop(id)
			assert.NoError(t, err, "Stop failed: %v", err)
		}(i)
	}

	wg.Wait()

	// Clean shutdown should work after all concurrent operations.
	if err := mgr.ShutdownAll(); err != nil {
		t.Fatalf("ShutdownAll after concurrent ops failed: %v", err)
	}
	mgr.Wait()
}

// --- TestWait ----------------------------------------------------------------

func TestWait(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "w1", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)
	err = mgr.Start(context.Background(), "w2", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	done := make(chan struct{})
	go func() {
		mgr.Wait()
		close(done)
	}()

	// Wait should block.
	select {
	case <-done:
		t.Fatal("Wait should block until ShutdownAll is called")
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	err = mgr.ShutdownAll()
	assert.NoError(t, err, "ShutdownAll failed: %v", err)

	select {
	case <-done:
		// expected — Wait unblocked after shutdown.
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not unblock after ShutdownAll")
	}
}

// --- TestWorkerSelfExit ------------------------------------------------------

func TestWorkerSelfExit(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	// A worker that exits on its own after a short delay.
	selfExitWorker := func(lc octopus.Lifecycle) error {
		time.Sleep(50 * time.Millisecond)
		return errors.New("done on my own")
	}

	err := mgr.Start(context.Background(), "self-exit", selfExitWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	// Wait for the worker to exit and be cleaned up from the registry.
	assert.Eventually(t, func() bool {
		return !mgr.IsRunning("self-exit")
	}, 2*time.Second, 10*time.Millisecond, "self-exited worker should have been removed from registry")

	// Should be able to restart with the same ID.
	if err := mgr.Start(context.Background(), "self-exit", simpleWorker); err != nil {
		t.Fatalf("Re-start after self-exit failed: %v", err)
	}
	err = mgr.Stop("self-exit")
	assert.NoError(t, err, "Stop failed: %v", err)
}

// --- TestWorkersListSorted ---------------------------------------------------

func TestWorkersListSorted(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	ids := []string{"charlie", "alpha", "bravo"}
	for _, id := range ids {
		err := mgr.Start(context.Background(), id, simpleWorker)
		assert.NoError(t, err, "Start failed: %v", err)
	}

	workers := mgr.Workers()
	expected := []string{"alpha", "bravo", "charlie"}
	if len(workers) != len(expected) {
		t.Fatalf("expected %d workers, got %d", len(expected), len(workers))
	}
	for i, w := range workers {
		if w != expected[i] {
			t.Fatalf("expected workers[%d]=%q, got %q", i, expected[i], w)
		}
	}

	err := mgr.ShutdownAll()
	assert.NoError(t, err, "ShutdownAll failed: %v", err)
	mgr.Wait()
}

// --- TestLifecycleIsStopping -------------------------------------------------

func TestLifecycleIsStopping(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	stoppingDetected := make(chan bool, 1)
	workerDone := make(chan struct{})

	worker := func(lc octopus.Lifecycle) error {
		defer close(workerDone)
		<-lc.Done()
		stoppingDetected <- lc.IsStopping()
		return nil
	}

	err := mgr.Start(context.Background(), "detector", worker)
	assert.NoError(t, err, "Start failed: %v", err)
	err = mgr.Stop("detector")
	assert.NoError(t, err, "Stop failed: %v", err)

	select {
	case wasStopping := <-stoppingDetected:
		if !wasStopping {
			t.Fatal("expected IsStopping() to be true during graceful stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stopping detection")
	}

	<-workerDone
}

// --- TestShutdownAllIdempotent -----------------------------------------------

func TestShutdownAllIdempotent(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "w1", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	// Call ShutdownAll multiple times — should not panic or error.
	err1 := mgr.ShutdownAll()
	err2 := mgr.ShutdownAll()
	err3 := mgr.ShutdownAll()

	if err1 != nil {
		t.Fatalf("first ShutdownAll failed: %v", err1)
	}
	// Subsequent calls return nil (the once.Do already ran).
	if err2 != nil {
		t.Fatalf("second ShutdownAll should return nil, got: %v", err2)
	}
	if err3 != nil {
		t.Fatalf("third ShutdownAll should return nil, got: %v", err3)
	}

	mgr.Wait()
}

// --- TestWaitFor -------------------------------------------------------------

func TestWaitFor(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	// WaitFor on non-existent worker.
	err := mgr.WaitFor("ghost")
	if !errors.Is(err, octopus.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got: %v", err)
	}

	// WaitFor on a running worker — should block until it exits.
	errCh := make(chan error, 1)
	err = mgr.Start(context.Background(), "tracked", func(lc octopus.Lifecycle) error {
		<-lc.Done()
		return errors.New("tracked error")
	})
	assert.NoError(t, err, "Start failed: %v", err)

	go func() {
		errCh <- mgr.WaitFor("tracked")
	}()

	// Give WaitFor time to start waiting.
	time.Sleep(50 * time.Millisecond)

	err = mgr.Stop("tracked")
	assert.NoError(t, err, "Stop failed: %v", err)

	select {
	case err = <-errCh:
		if err == nil {
			t.Fatal("expected error from WaitFor, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitFor did not unblock after worker exit")
	}
}

// --- TestWaitForNotFoundAfterExit --------------------------------------------

func TestWaitForNotFoundAfterExit(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "brief", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)
	err = mgr.Stop("brief")
	assert.NoError(t, err, "Stop failed: %v", err)

	// After the worker has exited and been removed, WaitFor should return ErrWorkerNotFound.
	err = mgr.WaitFor("brief")
	if !errors.Is(err, octopus.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound after worker exit, got: %v", err)
	}
}

// --- TestWorkerInfo ---------------------------------------------------------

func TestWorkerInfoRunning(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "running", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	info, err := mgr.WorkerInfo("running")
	assert.NoError(t, err, "WorkerInfo failed: %v", err)

	if info.ID != "running" {
		t.Fatalf("expected ID 'running', got %q", info.ID)
	}
	if info.StartedAt.IsZero() {
		t.Fatal("expected StartedAt to be set")
	}
	if !info.StoppedAt.IsZero() {
		t.Fatal("expected StoppedAt to be zero while running")
	}
	if info.Err != nil {
		t.Fatalf("expected Err to be nil while running, got %v", info.Err)
	}

	err = mgr.Stop("running")
	assert.NoError(t, err)
}

func TestWorkerInfoStopped(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	err := mgr.Start(context.Background(), "stopped", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	info, err := mgr.WorkerInfo("stopped")
	assert.NoError(t, err, "WorkerInfo failed: %v", err)

	if info.ID != "stopped" {
		t.Fatalf("expected ID 'stopped', got %q", info.ID)
	}
	if info.StartedAt.IsZero() {
		t.Fatal("expected StartedAt to be set")
	}
	if !info.StoppedAt.IsZero() {
		t.Fatal("expected StoppedAt to be zero while running")
	}

	// Now stop it - after stop, it's removed from registry.
	err = mgr.Stop("stopped")
	assert.NoError(t, err, "Stop failed: %v", err)

	// WorkerInfo now returns ErrWorkerNotFound as it's been cleaned up.
	_, err = mgr.WorkerInfo("stopped")
	if !errors.Is(err, octopus.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound after stop, got: %v", err)
	}
}

func TestWorkerInfoNotFound(t *testing.T) {
	t.Parallel()

	mgr := testManager(t)

	_, err := mgr.WorkerInfo("ghost")
	if !errors.Is(err, octopus.ErrWorkerNotFound) {
		t.Fatalf("expected ErrWorkerNotFound, got: %v", err)
	}
}

// --- TestHooks ---------------------------------------------------------

func TestOnStartHook(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calledWith := ""

	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(5*time.Second),
		octopus.WithOnStart(func(id string) {
			mu.Lock()
			calledWith = id
			mu.Unlock()
		}))

	err := mgr.Start(context.Background(), "hooked", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	// Give the hook time to fire.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if calledWith != "hooked" {
		t.Fatalf("expected onStart hook to be called with 'hooked', got %q", calledWith)
	}
	mu.Unlock()

	err = mgr.Stop("hooked")
	assert.NoError(t, err)
}

func TestOnStopHook(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var receivedID string
	var receivedErr error

	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(5*time.Second),
		octopus.WithOnStop(func(id string, err error) {
			mu.Lock()
			receivedID = id
			receivedErr = err
			mu.Unlock()
		}))

	err := mgr.Start(context.Background(), "hooked", simpleWorker)
	assert.NoError(t, err, "Start failed: %v", err)

	err = mgr.Stop("hooked")
	assert.NoError(t, err, "Stop failed: %v", err)

	mu.Lock()
	if receivedID != "hooked" {
		t.Fatalf("expected onStop hook to be called with 'hooked', got %q", receivedID)
	}
	if receivedErr != nil {
		t.Fatalf("expected onStop hook err to be nil, got %v", receivedErr)
	}
	mu.Unlock()
}

func TestOnStopHookWithError(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var receivedErr error

	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(5*time.Second),
		octopus.WithOnStop(func(_ string, err error) {
			mu.Lock()
			receivedErr = err
			mu.Unlock()
		}))

	err := mgr.Start(context.Background(), "error-worker", func(lc octopus.Lifecycle) error {
		<-lc.Done()
		return errors.New("worker error")
	})
	assert.NoError(t, err, "Start failed: %v", err)

	err = mgr.Stop("error-worker")
	assert.NoError(t, err, "Stop failed: %v", err)

	mu.Lock()
	if receivedErr == nil {
		t.Fatal("expected onStop hook to receive error")
	}
	if receivedErr.Error() != "worker error" {
		t.Fatalf("expected 'worker error', got %v", receivedErr)
	}
	mu.Unlock()
}
