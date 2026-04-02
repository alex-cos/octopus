package octopus

import (
	"log/slog"
	"time"
)

const (
	// DefaultGlobalTimeout is the default timeout for ShutdownAll.
	DefaultGlobalTimeout = 30 * time.Second

	// DefaultWorkerTimeout is the default timeout for stopping an individual worker.
	DefaultWorkerTimeout = 10 * time.Second
)

// managerConfig holds the configuration for a Octopus.
type managerConfig struct {
	globalTimeout time.Duration
	logger       *slog.Logger
	onStart      func(id string)
	onStop       func(id string, err error)
}

// defaultManagerConfig returns a managerConfig with sensible defaults.
func defaultManagerConfig() managerConfig {
	return managerConfig{
		globalTimeout: DefaultGlobalTimeout,
		logger:        slog.Default(),
	}
}

// ManagerOption configures a Octopus.
type ManagerOption func(*managerConfig)

// WithGlobalTimeout sets the maximum duration ShutdownAll will wait for all
// workers to terminate before forcing cancellation. Panics if d <= 0.
func WithGlobalTimeout(d time.Duration) ManagerOption {
	return func(c *managerConfig) {
		if d <= 0 {
			panic("global timeout must be positive")
		}
		c.globalTimeout = d
	}
}

// WithLogger sets the slog.Logger used by the manager and all workers.
// If not set, slog.Default() is used.
func WithLogger(l *slog.Logger) ManagerOption {
	return func(c *managerConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithOnStart sets a callback that is invoked when a worker starts.
// The callback receives the worker ID. Useful for metrics (e.g., Prometheus counters).
func WithOnStart(fn func(id string)) ManagerOption {
	return func(c *managerConfig) {
		c.onStart = fn
	}
}

// WithOnStop sets a callback that is invoked when a worker stops.
// The callback receives the worker ID and its exit error (nil if clean exit).
// Useful for metrics (e.g., Prometheus counters, OpenTelemetry spans).
func WithOnStop(fn func(id string, err error)) ManagerOption {
	return func(c *managerConfig) {
		c.onStop = fn
	}
}

// workerConfig holds the configuration for an individual worker.
type workerConfig struct {
	timeout time.Duration
}

// defaultWorkerConfig returns a workerConfig with sensible defaults.
func defaultWorkerConfig() workerConfig {
	return workerConfig{
		timeout: DefaultWorkerTimeout,
	}
}

// WorkerOption configures an individual worker.
type WorkerOption func(*workerConfig)

// WithWorkerTimeout sets the maximum duration Stop will wait for this specific
// worker to terminate before considering it timed out. Panics if d <= 0.
func WithWorkerTimeout(d time.Duration) WorkerOption {
	return func(c *workerConfig) {
		if d <= 0 {
			panic("worker timeout must be positive")
		}
		c.timeout = d
	}
}
