package oceanbasetelemetrystore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestProjectionMaintenanceRetryStopsAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var attempts int
	retryProjectionMaintenance(ctx, time.Millisecond, time.Hour, func(ctx context.Context) (bool, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("each migration attempt must have a deadline")
		}
		attempts++
		if attempts == 1 {
			return false, errors.New("temporary DDL failure")
		}
		return true, nil
	})
	if attempts != 2 {
		t.Fatalf("got %d attempts", attempts)
	}
}

func TestProjectionMaintenanceRetryCancelsActiveAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan struct{})
	var attempts atomic.Int32
	go func() {
		defer close(done)
		retryProjectionMaintenance(ctx, time.Millisecond, time.Hour, func(attemptCtx context.Context) (bool, error) {
			attempts.Add(1)
			close(started)
			<-attemptCtx.Done()
			return false, attemptCtx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("retry did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown left migration retry active")
	}
	if attempts.Load() != 1 {
		t.Fatal("shutdown ran another migration")
	}
}
