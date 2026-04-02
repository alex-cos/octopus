package octopus_test

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alex-cos/octopus"
)

func Example() {
	// Create a manager with a 10s global shutdown timeout.
	mgr := octopus.NewOctopus(
		octopus.WithGlobalTimeout(10*time.Second),
		octopus.WithLogger(slog.Default()),
	)

	// Start a poller worker.
	err := mgr.Start(context.Background(), "poller", func(lc octopus.Lifecycle) error {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-lc.Done():
				lc.Logger().Info("poller shutting down")
				return nil
			case <-ticker.C:
				lc.Logger().Info("polling...")
			}
		}
	}, octopus.WithWorkerTimeout(3*time.Second))
	if err != nil {
		panic(err)
	}

	// Start a cache warmer worker.
	err = mgr.Start(context.Background(), "cache-warmer", func(lc octopus.Lifecycle) error {
		for {
			select {
			case <-lc.Done():
				return nil
			case <-time.After(1 * time.Second):
				lc.Logger().Info("warming cache...")
			}
		}
	}, octopus.WithWorkerTimeout(2*time.Second))
	if err != nil {
		panic(err)
	}

	// Let them run a bit.
	time.Sleep(1200 * time.Millisecond)

	// Get info about the cache-warmer worker.
	info, err := mgr.WorkerInfo("cache-warmer")
	if err != nil {
		panic(err)
	}
	fmt.Printf("cache-warmer started at %v\n", info.StartedAt)

	// Hot-stop the cache warmer (poller keeps running).
	err = mgr.Stop("cache-warmer")
	if err != nil {
		panic(err)
	}
	fmt.Println("cache-warmer stopped, poller still running:", mgr.IsRunning("poller"))

	// Restart cache-warmer with a different implementation.
	err = mgr.Start(context.Background(), "cache-warmer", func(lc octopus.Lifecycle) error {
		<-lc.Done()
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("cache-warmer restarted, workers:", mgr.Workers())

	// Graceful shutdown of everything.
	err = mgr.ShutdownAll()
	if err != nil {
		panic(err)
	}
	mgr.Wait()
	fmt.Println("all workers shut down")
}
