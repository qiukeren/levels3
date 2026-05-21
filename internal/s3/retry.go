package s3

import (
	"context"
	"math/rand"
	"time"
)

// WithRetry executes fn up to maxAttempts times with exponential backoff + jitter.
// It only retries errors wrapped in RetryableError; non-retryable errors are
// returned immediately. If ctx is cancelled, the function returns ctx.Err().
func WithRetry(ctx context.Context, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}

		err = fn()
		if err == nil {
			return nil
		}

		if !IsRetryable(err) {
			return err
		}

		if attempt < maxAttempts-1 {
			delay := calcBackoff(baseDelay, attempt)
			if err = sleepCtx(ctx, delay); err != nil {
				return err
			}
		}
	}

	return &RetryableError{Err: ErrRetryExhausted}
}

// calcBackoff returns an exponential backoff duration with full jitter.
// delay = baseDelay * 2^attempt, then a random value between 0 and delay is chosen.
// This prevents thundering herd in distributed scenarios.
func calcBackoff(baseDelay time.Duration, attempt int) time.Duration {
	expDelay := baseDelay * time.Duration(1<<uint(attempt))
	if expDelay <= 0 {
		expDelay = baseDelay
	}
	return time.Duration(rand.Int63n(int64(expDelay)))
}

// sleepCtx sleeps for the given duration or until ctx is cancelled.
func sleepCtx(ctx context.Context, delay time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}
