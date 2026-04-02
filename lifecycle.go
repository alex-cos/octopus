package octopus

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// Lifecycle is the interface passed to every worker function. It provides
// the worker with signals about its lifecycle and a pre-configured logger.
type Lifecycle interface {
	// Done returns a channel that is closed when the worker should stop.
	// Workers should select on this channel to detect shutdown requests.
	Done() <-chan struct{}

	// Err returns the reason for the lifecycle ending. It returns nil if
	// Done is not yet closed.
	Err() error

	// IsStopping reports whether a stop has been requested for this worker.
	IsStopping() bool

	// Logger returns a *slog.Logger pre-configured with the worker's ID.
	Logger() *slog.Logger
}

// workerLifecycle implements Lifecycle for an individual worker.
type workerLifecycle struct {
	ctx      context.Context // nolint: containedctx
	stopping atomic.Bool
	logger   *slog.Logger
}

// newWorkerLifecycle creates a new workerLifecycle bound to the given context.
func newWorkerLifecycle(ctx context.Context, logger *slog.Logger) *workerLifecycle {
	return &workerLifecycle{
		ctx:    ctx,
		logger: logger,
	}
}

func (lc *workerLifecycle) Done() <-chan struct{} {
	return lc.ctx.Done()
}

func (lc *workerLifecycle) Err() error {
	return lc.ctx.Err()
}

func (lc *workerLifecycle) IsStopping() bool {
	return lc.stopping.Load()
}

func (lc *workerLifecycle) Logger() *slog.Logger {
	return lc.logger
}

// markStopping sets the stopping flag to true. This is called by the manager
// before cancelling the worker's context, so the worker can distinguish
// between a graceful stop and a parent context cancellation.
func (lc *workerLifecycle) markStopping() {
	lc.stopping.Store(true)
}
