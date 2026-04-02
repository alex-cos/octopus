package octopus

import (
	"context"
	"sync"
	"time"
)

// WorkerFunc is the function signature for worker goroutines managed by
// Octopus. The function receives a Lifecycle that provides shutdown
// signals and a logger. It should return nil on clean exit, or an error
// if something went wrong.
type WorkerFunc func(lc Lifecycle) error

// WorkerInfo represents the state of a worker at a point in time.
// It can be retrieved via Octopus.WorkerInfo to observe the lifecycle
// of a running worker.
type WorkerInfo struct {
	ID        string
	StartedAt time.Time
	StoppedAt time.Time
	Err      error
}

// workerEntry is the internal representation of a running worker inside
// the manager's registry.
type workerEntry struct {
	id        string
	fn        WorkerFunc
	cancel    context.CancelFunc
	lifecycle *workerLifecycle
	config    workerConfig
	done      chan struct{} // closed when the goroutine exits
	mu        sync.Mutex    // protects err, stoppedAt
	err       error         // error returned by the worker (or panic)
	startedAt time.Time
	stoppedAt time.Time
}

func (w *workerEntry) setErr(err error) {
	w.mu.Lock()
	w.err = err
	w.mu.Unlock()
}

func (w *workerEntry) getErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *workerEntry) setStoppedAt(t time.Time) {
	w.mu.Lock()
	w.stoppedAt = t
	w.mu.Unlock()
}

func (w *workerEntry) getInfo() WorkerInfo {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WorkerInfo{
		ID:        w.id,
		StartedAt: w.startedAt,
		StoppedAt: w.stoppedAt,
		Err:      w.err,
	}
}