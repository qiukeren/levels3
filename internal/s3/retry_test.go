package s3

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetrySuccess(t *testing.T) {
	callCount := 0
	err := WithRetry(context.Background(), 3, 10*time.Millisecond, func() error {
		callCount++
		if callCount < 2 {
			return &RetryableError{Err: errors.New("temporary error")}
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestRetryExhausted(t *testing.T) {
	callCount := 0
	err := WithRetry(context.Background(), 3, 10*time.Millisecond, func() error {
		callCount++
		return &RetryableError{Err: errors.New("temporary error")}
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestNonRetryableError(t *testing.T) {
	callCount := 0
	err := WithRetry(context.Background(), 3, 10*time.Millisecond, func() error {
		callCount++
		return errors.New("permanent error")
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != 1 {
		t.Errorf("expected 1 call (no retry for permanent error), got %d", callCount)
	}
}

func TestRetryContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	callCount := 0
	err := WithRetry(ctx, 3, 10*time.Millisecond, func() error {
		callCount++
		return &RetryableError{Err: errors.New("temporary error")}
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if callCount != 0 {
		t.Errorf("expected 0 calls (context already cancelled), got %d", callCount)
	}
}
