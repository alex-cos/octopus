package octopus_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/alex-cos/octopus"
)

func TestMain(m *testing.M) {
	// Suppress slog output during benchmarks.
	m.Run()
}

func quietBenchmarkManager() *octopus.Octopus {
	return octopus.NewOctopus(
		octopus.WithGlobalTimeout(30*time.Second),
		octopus.WithLogger(slog.New(slog.DiscardHandler)),
	)
}

func simpleBenchmarkWorker(lc octopus.Lifecycle) error {
	<-lc.Done()
	return nil
}

func BenchmarkStartStop(b *testing.B) {
	mgr := quietBenchmarkManager()
	ctx := context.Background()

	b.ResetTimer()
	for i := range b.N {
		id := fmt.Sprintf("w-%d", i)
		if err := mgr.Start(ctx, id, simpleBenchmarkWorker); err != nil {
			b.Fatal(err)
		}
		if err := mgr.Stop(id); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := mgr.ShutdownAll(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkConcurrentStartStop(b *testing.B) {
	mgr := quietBenchmarkManager()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := 0
		for pb.Next() {
			id := fmt.Sprintf("w-%d", n)
			n++
			_ = mgr.Start(ctx, id, simpleBenchmarkWorker)
			_ = mgr.Stop(id)
		}
	})
	b.StopTimer()
	if err := mgr.ShutdownAll(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkShutdownAll(b *testing.B) {
	ctx := context.Background()

	for range b.N {
		mgr := quietBenchmarkManager()
		for j := range 100 {
			id := fmt.Sprintf("w-%d", j)
			_ = mgr.Start(ctx, id, simpleBenchmarkWorker)
		}
		_ = mgr.ShutdownAll()
		mgr.Wait()
	}
}
