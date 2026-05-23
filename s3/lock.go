package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// S3Lock implements a distributed lock using S3 as the coordination backend.
//
// Lock Strategy:
//   - A lock file is stored at {Path}/LOCK in S3, containing "{pid}-{timestamp}-{random}".
//   - On Lock(): check remote lock liveness; if stale (lease expired), overwrite it.
//   - A background goroutine renews the lease periodically to prevent expiry.
//   - On Unlock(): stop renewal, delete the remote lock, clean up local file.
//
// This is a best-effort distributed lock. In the event of a network partition,
// the lock may be held by multiple processes until the lease expires.
type S3Lock struct {
	clientAPI  S3ClientAPI
	opt        OpenOption
	lockID     string
	leaseErrCh chan error // receives lease renewal errors for the caller to handle
	stopCh     chan struct{}
}

// NewS3Lock creates a new S3Lock backed by the given S3ClientAPI.
func NewS3Lock(clientAPI S3ClientAPI, opt OpenOption) *S3Lock {
	opt.ApplyDefaults()
	return &S3Lock{
		clientAPI:  clientAPI,
		opt:        opt,
		leaseErrCh: make(chan error, 1),
		stopCh:     make(chan struct{}),
	}
}

// Lock attempts to acquire the distributed lock. It blocks until the lock is
// acquired or the context is cancelled.
//
// The lock is considered valid for LeaseDuration; the background renewal
// goroutine refreshes it every LeaseRenewal interval.
func (l *S3Lock) Lock(ctx context.Context) error {
	l.lockID = fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), randomInt())

	localLockPath := filepath.Join(l.opt.LocalPath(), "LOCK")
	_, localErr := os.ReadFile(localLockPath)

	remoteExists, err := l.clientAPI.Exists(ctx, "LOCK")
	if err != nil {
		return fmt.Errorf("failed to check remote lock: %w", err)
	}

	if remoteExists {
		remoteData, err := l.clientAPI.GetBytes(ctx, "LOCK")
		if err != nil {
			return fmt.Errorf("failed to read remote lock: %w", err)
		}

		if !l.isLockHolder(string(remoteData), localLockPath, localErr) {
			if !l.isLockStale(string(remoteData)) {
				return ErrLockHeld
			}
		}
	}

	if err := l.acquireLock(ctx, localLockPath); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	l.startLeaseRenewal(ctx)

	return nil
}

// Unlock releases the distributed lock. It stops the lease renewal goroutine,
// deletes the remote lock file, and removes the local lock file.
func (l *S3Lock) Unlock(ctx context.Context) error {
	close(l.stopCh)

	if err := l.releaseRemoteLock(ctx); err != nil {
		return fmt.Errorf("failed to release remote lock: %w", err)
	}

	localLockPath := filepath.Join(l.opt.LocalPath(), "LOCK")
	os.Remove(localLockPath)

	return nil
}

// isLockHolder checks whether the local lock file matches the remote lock data.
// Returns true only if both files exist and their content matches.
func (l *S3Lock) isLockHolder(remoteData string, localPath string, localErr error) bool {
	if localErr != nil {
		return false
	}

	localData, err := os.ReadFile(localPath)
	if err != nil {
		return false
	}

	return string(localData) == remoteData
}

// isLockStale determines whether a lock can be considered stale (expired).
// A lock is stale if:
//   - The lock data cannot be parsed (corrupted).
//   - The lease timestamp has exceeded LeaseDuration.
//   - The owning process is no longer running.
func (l *S3Lock) isLockStale(lockData string) bool {
	var pid int
	var timestamp int64
	var rnd int
	_, err := fmt.Sscanf(lockData, "%d-%d-%d", &pid, &timestamp, &rnd)
	if err != nil {
		return true
	}

	if time.Since(time.Unix(0, timestamp)) > l.opt.LeaseDuration {
		return true
	}

	if pid != os.Getpid() && !isProcessRunning(pid) {
		return true
	}

	return false
}

// acquireLock writes the lock to S3 and persists it locally.
// Local file is written first to guarantee crash recovery can identify the holder.
func (l *S3Lock) acquireLock(ctx context.Context, localPath string) error {
	lockData := []byte(l.lockID)

	// Write local lock first for crash recovery.
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create local lock directory: %w", err)
	}
	if err := os.WriteFile(localPath, lockData, 0644); err != nil {
		return fmt.Errorf("failed to write local lock file: %w", err)
	}

	// Then write remote lock. If this fails, the local lock is cleaned up.
	if err := l.clientAPI.PutBytes(ctx, "LOCK", lockData); err != nil {
		os.Remove(localPath)
		return fmt.Errorf("failed to put remote lock: %w", err)
	}

	return nil
}

// releaseRemoteLock deletes the lock file from S3.
func (l *S3Lock) releaseRemoteLock(ctx context.Context) error {
	return l.clientAPI.Remove(ctx, "LOCK")
}

// startLeaseRenewal begins a background goroutine that periodically rewrites
// the remote lock file to prevent lease expiry.
func (l *S3Lock) startLeaseRenewal(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(l.opt.LeaseRenewal)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := l.renewLease(ctx); err != nil {
					// Lease renewal failed persistently. This is a critical error:
					// the process may lose the lock without knowing it.
					// Send the error on a non-blocking channel for the caller to handle.
					select {
					case l.leaseErrCh <- fmt.Errorf("lease renewal failed: %w", err):
					default:
					}
					return
				}
			case <-l.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// LeaseErr returns a channel that receives lease renewal errors.
// Callers should monitor this channel and react (e.g. by stopping writes)
// if a lease renewal failure is detected.
func (l *S3Lock) LeaseErr() <-chan error {
	return l.leaseErrCh
}

// renewLease rewrites the remote lock to extend the lease.
func (l *S3Lock) renewLease(ctx context.Context) error {
	return l.clientAPI.PutBytes(ctx, "LOCK", []byte(l.lockID))
}

func randomInt() int {
	n, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		return int(time.Now().UnixNano() % 1000000)
	}
	return int(n.Int64())
}

// isProcessRunning checks whether a process with the given PID is still running.
//
// WARNING: On Unix, os.FindProcess always succeeds and Signal(nil) returns nil
// for any valid PID, even if it belongs to a different (recycled) process.
// This means a stale lock from a dead process could be interpreted as still held
// if another process reuses the same PID.
//
// This is a secondary check; the primary defense against stale locks is the
// lease timestamp expiration in isLockStale(). The PID check serves only as
// an optimization to quickly identify locks held by definitely-dead processes.
func isProcessRunning(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = process.Signal(os.Signal(nil))
	return err == nil
}
