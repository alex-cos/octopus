# Octopus

[![Go Version](https://img.shields.io/badge/Go-1.25%2B-blue)](https://go.dev/)
[![Test Status](https://github.com/alex-cos/octopus/actions/workflows/test.yml/badge.svg)](https://github.com/alex-cos/octopus/actions/workflows/test.yml)
[![Lint Status](https://github.com/alex-cos/octopus/actions/workflows/lint.yml/badge.svg)](https://github.com/alex-cos/octopus/actions/workflows/lint.yml)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/alex-cos/octopus)](https://goreportcard.com/report/github.com/alex-cos/octopus)

A thread-safe goroutine lifecycle manager for Go applications. Inspired by the [Tomb](https://github.com/go-tomb/tomb) package, octopus adds support for hot starting and stopping of individual goroutines at runtime, while maintaining the ability to perform a full graceful shutdown.

![octopus](/octopus.png)

## Features

- **Named worker goroutines** with unique identifiers
- **Hot start/stop**: dynamically add or remove workers at runtime
- **ShutdownAll**: gracefully stop every running worker at once
- **Per-worker and global shutdown timeouts** with fallback
- **Automatic panic recovery** with stack trace logging
- **Thread-safe**: all methods can be called from any goroutine
- **Integration** with `context.Context` and `log/slog`
- **Observability** with hooks for start/stop events

## Installation

```bash
go get github.com/alex-cos/octopus
```

## Requirements

- Go 1.25.8 or later

## Quick Start

```go
package main

import (
  "context"
  "log/slog"
  "time"

  "github.com/alex-cos/octopus"
)

func main() {
  ctx := context.Background()

  mgr := octopus.NewOctopus(
    octopus.WithGlobalTimeout(30*time.Second),
    octopus.WithLogger(slog.Default()),
  )

  // Start a worker
  mgr.Start(ctx, "poller", func(lc octopus.Lifecycle) error {
    for {
      select {
      case <-lc.Done():
        return nil
      case <-time.After(time.Second):
        lc.Logger().Info("polling...")
      }
    }
  }, octopus.WithWorkerTimeout(5*time.Second))

  // Hot-stop a single worker
  mgr.Stop("poller")

  // Shut everything down
  mgr.ShutdownAll()
  mgr.Wait()
}
```

## Usage

### Creating a Manager

```go
mgr := octopus.NewOctopus()
```

With options:

```go
mgr := octopus.NewOctopus(
    octopus.WithGlobalTimeout(30*time.Second),
    octopus.WithLogger(myLogger),
)
```

### Starting Workers

```go
err := mgr.Start(ctx, "worker-id", func(lc octopus.Lifecycle) error {
    for {
        select {
        case <-lc.Done():
            return nil
        default:
            // do work
        }
    }
}, octopus.WithWorkerTimeout(10*time.Second))
```

### Stopping a Single Worker

```go
err := mgr.Stop("worker-id")
```

### Graceful Shutdown of All Workers

```go
err := mgr.ShutdownAll()
mgr.Wait()
```

### Advanced Usage: HTTP Servers

Octopus works particularly well with HTTP servers that need graceful shutdown.
The `Serve` helper function wraps an `http.Server` to properly handle lifecycle events:

```go
// Serve returns a WorkerFunc for graceful HTTP server shutdown.
func Serve(s *http.Server) octopus.WorkerFunc {
    return func(lc octopus.Lifecycle) error {
        errCh := make(chan error, 1)
        go func() { errCh <- s.ListenAndServe() }()

        select {
        case err := <-errCh:
            if err == http.ErrServerClosed {
                return nil
            }
            return err
        case <-lc.Done():
            ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
            defer cancel()
            return s.Shutdown(ctx)
        }
    }
}

srv := &http.Server{
    Addr:              ":8080",
    Handler:           router,
    ReadHeaderTimeout: 10 * time.Second,
}

mgr := octopus.NewOctopus(
    octopus.WithGlobalTimeout(20*time.Second),
    octopus.WithLogger(slog.Default()),
)

if err := mgr.Start(context.Background(), "webserver", Serve(srv), octopus.WithWorkerTimeout(15*time.Second)); err != nil {
    panic(err)
}

// Later, during shutdown:
mgr.ShutdownAll()
mgr.Wait()
```

### Querying Worker State

```go
// Check if a specific worker is running
running := mgr.IsRunning("worker-id")

// Get list of all running worker IDs (sorted)
ids := mgr.Workers()

// Get the count of running workers
count := mgr.Alive()
```

## API Reference

### Manager Methods

| Method | Description |
| --- | --- |
| `Start(ctx, id, fn, opts...)` | Start a named worker goroutine |
| `Stop(id)` | Gracefully stop a single worker |
| `ShutdownAll()` | Gracefully stop all workers |
| `Wait()` | Block until all workers have terminated |
| `IsRunning(id)` | Check if a worker is running |
| `Workers()` | Get sorted list of running worker IDs |
| `Alive()` | Get count of running workers |

### Manager Options

| Option | Description | Default |
| --- | --- | --- |
| `WithGlobalTimeout(d)` | Max duration for `ShutdownAll` | 30s |
| `WithLogger(l)` | Logger for manager and workers | `slog.Default()` |

### Worker Options

| Option | Description | Default |
| --- | --- | --- |
| `WithWorkerTimeout(d)` | Max duration for `Stop` | 10s |

### Lifecycle Interface

| Method | Description |
| --- | --- |
| `Done()` | Channel closed when worker should stop |
| `Err()` | Reason for lifecycle ending (nil if not closed) |
| `IsStopping()` | Whether a stop has been requested |
| `Logger()` | Pre-configured `*slog.Logger` with worker ID |

### Errors

| Error | Description |
| --- | --- |
| `ErrWorkerAlreadyRunning` | Worker ID is already in use |
| `ErrWorkerNotFound` | No worker with the given ID exists |
| `ErrWorkerNotRunning` | Worker has already stopped or finished |
| `ErrManagerShutdown` | Manager has been shut down, no new workers allowed |
| `ErrWorkerPanicked` | A worker goroutine panicked |
| `ErrShutdownTimeout` | Global shutdown timeout exceeded |
| `ErrStopTimeout` | Individual worker stop timeout exceeded |
