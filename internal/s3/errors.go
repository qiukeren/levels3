package s3

import "errors"

var (
	ErrLockNotFound      = errors.New("lock file not found")
	ErrLockHeld          = errors.New("storage is locked by another process")
	ErrLockAcquireFailed = errors.New("failed to acquire lock")
	ErrFileTooLarge      = errors.New("file exceeds maximum allowed size")
	ErrInvalidFile       = errors.New("invalid file descriptor")
	ErrClosed            = errors.New("storage is closed")
	ErrRetryExhausted    = errors.New("max retry attempts exhausted")
)

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return "retryable: " + e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}
