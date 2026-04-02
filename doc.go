// Package octopus provides a thread-safe goroutine lifecycle manager for Go
// applications. Inspired by the Tomb package, octopus adds support for hot
// starting and stopping of individual goroutines at runtime, while maintaining
// the ability to perform a full graceful shutdown.
//
// # Key Features
//
//   - Named worker goroutines with unique identifiers
//   - Hot start/stop: dynamically add or remove workers at runtime
//   - ShutdownAll: gracefully stop every running worker at once
//   - Per-worker and global shutdown timeouts with fallback
//   - Automatic panic recovery with stack trace logging
//   - Thread-safe: all methods can be called from any goroutine
//   - Integration with context.Context and log/slog
//   - Observable: hooks for start/stop events
//
// # Worker Responsibilities
//
// Workers must respect the Lifecycle's Done channel. If a worker ignores
// lc.Done() and exceeds its timeout, it becomes orphaned (leaks).
// To avoid leaks:
//
//	worker := func(lc octopus.Lifecycle) error {
//	    for {
//	        select {
//	        case <-lc.Done():  // REQUIRED
//	            return nil
//	        case <-work:
//	            // do work
//	        }
//	    }
//	}
//
// Go does not support forced termination of goroutines.
// If a worker blocks on a syscall or infinite loop without
// respecting the context, it cannot be stopped gracefully.
//
// # Thread Safety
//
// All public methods (Start, Stop, ShutdownAll, etc.) are safe for
// concurrent use by multiple goroutines.
//
// # Quick Start
//
//	mgr := octopus.NewManager(ctx,
//	    octopus.WithGlobalTimeout(30*time.Second),
//	    octopus.WithLogger(slog.Default()),
//	)
//
//	// Start a worker
//	mgr.Start("poller", func(lc octopus.Lifecycle) error {
//	    for {
//	        select {
//	        case <-lc.Done():
//	            return nil
//	        case <-time.After(time.Second):
//	            lc.Logger().Info("polling...")
//	        }
//	    }
//	}, octopus.WithWorkerTimeout(5*time.Second))
//
//	// Hot-stop a single worker
//	mgr.Stop("poller")
//
//	// Shut everything down
//	mgr.ShutdownAll()
//	mgr.Wait()
package octopus
