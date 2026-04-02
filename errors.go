package octopus

import "errors"

var (
	// ErrWorkerAlreadyRunning is returned when Start is called with an ID
	// that is already registered and running.
	ErrWorkerAlreadyRunning = errors.New("worker already running")

	// ErrWorkerNotFound is returned when Stop or a query targets a worker ID
	// that does not exist in the manager's registry.
	ErrWorkerNotFound = errors.New("worker not found")

	// ErrManagerShutdown is returned when Start is called after ShutdownAll
	// has been initiated. No new workers can be registered after shutdown.
	ErrManagerShutdown = errors.New("manager is shut down")

	// ErrWorkerPanicked indicates that a worker goroutine panicked.
	// The original panic value is wrapped within this error.
	ErrWorkerPanicked = errors.New("worker panicked")

	// ErrShutdownTimeout is returned by ShutdownAll when the global timeout
	// expires before all workers have terminated gracefully.
	ErrShutdownTimeout = errors.New("shutdown timeout exceeded")

	// ErrStopTimeout is returned by Stop when the individual worker timeout
	// expires before the worker has terminated gracefully.
	ErrStopTimeout = errors.New("stop timeout exceeded for worker")
)
