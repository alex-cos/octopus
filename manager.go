package octopus

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Octopus is a thread-safe goroutine lifecycle manager. It allows
// starting and stopping named worker goroutines at runtime (hot start/stop),
// while also supporting a complete ShutdownAll to terminate everything.
// All public methods are safe for concurrent use.
type Octopus struct {
	mu        sync.RWMutex
	workers   map[string]*workerEntry
	config    managerConfig
	shutdown  atomic.Bool
	wg        sync.WaitGroup
	logger    *slog.Logger
	shutdownC chan struct{} // closed when ShutdownAll completes
	once      sync.Once     // ensures ShutdownAll runs only once
}

// NewOctopus creates a new Octopus. The provided context serves as the
// parent context for all worker goroutines. If the parent context is cancelled,
// all workers will receive a cancellation signal.
func NewOctopus(opts ...ManagerOption) *Octopus {
	cfg := defaultManagerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	return &Octopus{
		workers:   make(map[string]*workerEntry),
		config:    cfg,
		logger:    cfg.logger.With("component", "octopus"),
		shutdownC: make(chan struct{}),
	}
}

// Start registers and launches a new worker goroutine with the given ID.
// If a worker with the same ID is already running, ErrWorkerAlreadyRunning
// is returned. If the manager has been shut down, ErrManagerShutdown is returned.
func (m *Octopus) Start(ctx context.Context, id string, fn WorkerFunc, opts ...WorkerOption) error {
	if m.shutdown.Load() {
		return fmt.Errorf("%w: cannot start %q", ErrManagerShutdown, id)
	}

	wcfg := defaultWorkerConfig()
	for _, opt := range opts {
		opt(&wcfg)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check shutdown under lock to prevent race with ShutdownAll.
	if m.shutdown.Load() {
		return fmt.Errorf("%w: cannot start %q", ErrManagerShutdown, id)
	}

	if _, exists := m.workers[id]; exists {
		return fmt.Errorf("%w: %q", ErrWorkerAlreadyRunning, id)
	}

	workerCtx, workerCancel := context.WithCancel(ctx) // nolint: gosec
	workerLogger := m.logger.With("worker", id)
	lc := newWorkerLifecycle(workerCtx, workerLogger)

	entry := &workerEntry{
		id:        id,
		fn:        fn,
		cancel:    workerCancel,
		lifecycle: lc,
		config:    wcfg,
		done:      make(chan struct{}),
		startedAt: time.Now(),
	}

	m.workers[id] = entry
	m.wg.Add(1)

	m.logger.Info("worker starting", "worker", id)

	if m.config.onStart != nil {
		m.config.onStart(id)
	}

	go m.runWorker(entry)

	return nil
}

// Stop gracefully stops a running worker identified by its ID. It signals the
// worker to stop and waits up to the worker's configured timeout for it to
// finish. If the worker does not exist, ErrWorkerNotFound is returned. If the
// timeout is exceeded, ErrStopTimeout is returned (the worker's context is
// still cancelled but the goroutine may linger).
func (m *Octopus) Stop(id string) error {
	m.mu.RLock()
	entry, exists := m.workers[id]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("%w: %q", ErrWorkerNotFound, id)
	}

	m.logger.Info("worker stopping", "worker", id)

	// Mark as stopping so the worker can distinguish graceful stop from
	// parent context cancellation.
	entry.lifecycle.markStopping()
	entry.cancel()

	// Wait for the worker goroutine to finish, with timeout.
	select {
	case <-entry.done:
		m.logger.Info("worker stopped", "worker", id)
		return nil
	case <-time.After(entry.config.timeout):
		m.logger.Warn("worker stop timeout exceeded",
			"worker", id,
			"timeout", entry.config.timeout,
		)
		// Remove from registry even on timeout — the goroutine is orphaned.
		m.mu.Lock()
		delete(m.workers, id)
		m.mu.Unlock()
		return fmt.Errorf("%w: %q after %v", ErrStopTimeout, id, entry.config.timeout)
	}
}

// ShutdownAll initiates a graceful shutdown of all running workers. It signals
// every worker to stop and waits for them all to finish, respecting individual
// worker timeouts with the global timeout as a safety net.
//
// ShutdownAll can only be called once. Subsequent calls return immediately
// with nil. After ShutdownAll is called, no new workers can be started.
func (m *Octopus) ShutdownAll() error {
	var shutdownErr error

	m.once.Do(func() {
		m.shutdown.Store(true)
		m.logger.Info("shutdown initiated")

		// Snapshot the current worker IDs under read lock.
		m.mu.RLock()
		ids := make([]string, 0, len(m.workers))
		for id := range m.workers {
			ids = append(ids, id)
		}
		m.mu.RUnlock()

		if len(ids) == 0 {
			m.logger.Info("shutdown complete (no workers)")
			close(m.shutdownC)
			return
		}

		m.logger.Info("shutting down workers", "count", len(ids))

		// Signal all workers to stop in parallel.
		m.mu.RLock()
		for _, id := range ids {
			if entry, ok := m.workers[id]; ok {
				entry.lifecycle.markStopping()
				entry.cancel()
			}
		}
		m.mu.RUnlock()

		// Wait for all workers to finish, with global timeout as safety net.
		allDone := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(allDone)
		}()

		select {
		case <-allDone:
			m.logger.Info("shutdown complete", "workers_stopped", len(ids))
		case <-time.After(m.config.globalTimeout):
			m.logger.Error("shutdown timeout exceeded, forcing cancellation",
				"timeout", m.config.globalTimeout,
			)
			shutdownErr = fmt.Errorf("%w: after %v", ErrShutdownTimeout, m.config.globalTimeout)
		}

		// Clear the registry regardless of outcome so Workers()/IsRunning()
		// don't report phantom workers.
		m.mu.Lock()
		clear(m.workers)
		m.mu.Unlock()

		close(m.shutdownC)
	})

	return shutdownErr
}

// Wait blocks until all workers have terminated. Typically called after
// ShutdownAll to ensure everything is cleaned up before the process exits.
func (m *Octopus) Wait() {
	<-m.shutdownC
}

// IsRunning reports whether a worker with the given ID is currently running.
func (m *Octopus) IsRunning(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.workers[id]
	return exists
}

// Workers returns a sorted list of all currently running worker IDs.
func (m *Octopus) Workers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Alive returns the number of currently running workers.
func (m *Octopus) Alive() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.workers)
}

// WorkerInfo returns the state information for a worker.
// It provides access to timestamps and exit error.
// Returns ErrWorkerNotFound if the worker has never existed.
func (m *Octopus) WorkerInfo(id string) (WorkerInfo, error) {
	m.mu.RLock()
	entry, exists := m.workers[id]
	m.mu.RUnlock()

	if exists {
		return entry.getInfo(), nil
	}

	return WorkerInfo{}, fmt.Errorf("%w: %q", ErrWorkerNotFound, id)
}

// WaitFor blocks until the worker with the given ID exits, then returns its
// exit error (if any). It returns ErrWorkerNotFound if the worker is not
// currently running. Useful for monitoring specific workers or synchronizing
// on their completion.
func (m *Octopus) WaitFor(id string) error {
	m.mu.RLock()
	entry, exists := m.workers[id]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("%w: %q", ErrWorkerNotFound, id)
	}

	<-entry.done
	return entry.getErr()
}

// runWorker is the internal goroutine wrapper that executes the worker function
// with panic recovery and automatic cleanup.
func (m *Octopus) runWorker(e *workerEntry) {
	defer m.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			m.logger.Error("worker panicked",
				"worker", e.id,
				"panic", r,
				"stack", string(stack),
			)
			e.setErr(fmt.Errorf("%w: %v", ErrWorkerPanicked, r))
		}

		// Always close done to unblock anyone waiting on this worker.
		close(e.done)
		e.setStoppedAt(time.Now())

		err := e.getErr()

		// Remove from registry.
		m.mu.Lock()
		delete(m.workers, e.id)
		m.mu.Unlock()

		// Call onStop hook after cleanup.
		if m.config.onStop != nil {
			m.config.onStop(e.id, err)
		}
	}()

	err := e.fn(e.lifecycle)
	if err != nil {
		e.setErr(err)
		m.logger.Error("worker exited with error", "worker", e.id, "error", err)
	} else {
		m.logger.Debug("worker exited cleanly", "worker", e.id)
	}
}
