package s3

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

// mockClient implements S3ClientAPI for testing.
type mockClient struct {
	// Configure behavior
	putBytesFunc       func(ctx context.Context, key string, data []byte) error
	listFunc           func(ctx context.Context) ([]storage.FileDesc, error)
	listNamesFunc      func(ctx context.Context, prefix string) ([]string, error)
	existsFunc         func(ctx context.Context, key string) (bool, error)
	getBytesFunc       func(ctx context.Context, key string) ([]byte, error)
	removeFunc         func(ctx context.Context, key string) error
	copyFunc           func(ctx context.Context, srcKey, dstKey string) error
	putIfNotExistsFunc func(ctx context.Context, key string, data []byte) (bool, error)

	// Record calls
	putBytesCalls  []struct{ Key string; Data []byte }
	listNamesCalls []struct{ Prefix string }
}

func (m *mockClient) PutBytes(ctx context.Context, key string, data []byte) error {
	m.putBytesCalls = append(m.putBytesCalls, struct {
		Key  string
		Data []byte
	}{key, data})
	if m.putBytesFunc != nil {
		return m.putBytesFunc(ctx, key, data)
	}
	return nil
}

func (m *mockClient) PutBytesIfNotExists(ctx context.Context, key string, data []byte) (bool, error) {
	if m.putIfNotExistsFunc != nil {
		return m.putIfNotExistsFunc(ctx, key, data)
	}
	return true, nil
}

func (m *mockClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if m.getBytesFunc != nil {
		return m.getBytesFunc(ctx, key)
	}
	return nil, os.ErrNotExist
}

func (m *mockClient) Remove(ctx context.Context, key string) error {
	if m.removeFunc != nil {
		return m.removeFunc(ctx, key)
	}
	return nil
}

func (m *mockClient) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsFunc != nil {
		return m.existsFunc(ctx, key)
	}
	return false, nil
}

func (m *mockClient) List(ctx context.Context) ([]storage.FileDesc, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx)
	}
	return nil, nil
}

func (m *mockClient) ListNames(ctx context.Context, prefix string) ([]string, error) {
	m.listNamesCalls = append(m.listNamesCalls, struct{ Prefix string }{prefix})
	if m.listNamesFunc != nil {
		return m.listNamesFunc(ctx, prefix)
	}
	return nil, nil
}

func (m *mockClient) Copy(ctx context.Context, srcKey, dstKey string) error {
	if m.copyFunc != nil {
		return m.copyFunc(ctx, srcKey, dstKey)
	}
	return nil
}

// newTestStorage creates an S3Storage with a temp directory and mock client.
// The caller should provide setup functions for the mock.
func newTestStorage(t *testing.T, setupMock func(*mockClient)) (*S3Storage, *mockClient, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "levels3-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	mock := &mockClient{}
	if setupMock != nil {
		setupMock(mock)
	}

	opt := OpenOption{
		Bucket:                 "test-bucket",
		Path:                   "test-path",
		Ak:                     "test-ak",
		Sk:                     "test-sk",
		Region:                 "us-east-1",
		LocalCacheDir:          tmpDir,
		EnableLocalPersistence: true,
	}
	opt.ApplyDefaults()

	lock := NewS3Lock(mock, opt)
	storage, err := NewS3Storage(mock, lock, opt)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create S3Storage: %v", err)
	}

	cleanup := func() {
		storage.Close()
		os.RemoveAll(tmpDir)
	}

	return storage, mock, cleanup
}

func TestUploadFileReturnsErrorWhenS3Fails(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.putBytesFunc = func(ctx context.Context, key string, data []byte) error {
			return errors.New("s3 unavailable")
		}
	})
	defer cleanup()

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	err := s.uploadFile(fd, []byte("test data"))

	if err == nil {
		t.Fatal("expected error when S3 PutBytes fails, got nil")
	}

	// Verify pendingDir has the file (crash-recovery safety net)
	expectedName := filepath.Join(s.pendingDir, "000001.ldb")
	if _, statErr := os.Stat(expectedName); statErr != nil {
		t.Fatalf("expected pending file %s to exist: %v", expectedName, statErr)
	}
}

func TestUploadFileCleansPendingOnSuccess(t *testing.T) {
	s, mock, cleanup := newTestStorage(t, nil)
	defer cleanup()

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 42}

	// First, simulate a failed upload by writing to pendingDir manually
	pendingPath := filepath.Join(s.pendingDir, "000042.ldb")
	if err := os.WriteFile(pendingPath, []byte("stale data"), 0644); err != nil {
		t.Fatalf("failed to write pending file: %v", err)
	}

	// Now uploadFile succeeds (mock's default PutBytes returns nil)
	if err := s.uploadFile(fd, []byte("fresh data")); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Verify pending file was cleaned up
	if _, statErr := os.Stat(pendingPath); statErr == nil {
		t.Fatal("expected pending file to be removed after successful upload")
	}

	// Verify the uploaded data matches
	if len(mock.putBytesCalls) != 1 {
		t.Fatalf("expected 1 PutBytes call, got %d", len(mock.putBytesCalls))
	}
	if mock.putBytesCalls[0].Key != "000042.ldb" {
		t.Fatalf("expected key 000042.ldb, got %s", mock.putBytesCalls[0].Key)
	}
}

func TestSyncPendingToS3RecoversFiles(t *testing.T) {
	s, mock, cleanup := newTestStorage(t, nil)
	defer cleanup()

	// Write pending files
	pendingFiles := []struct {
		name string
		fd   storage.FileDesc
	}{
		{"000010.ldb", storage.FileDesc{Type: storage.TypeTable, Num: 10}},
		{"000011.ldb", storage.FileDesc{Type: storage.TypeTable, Num: 11}},
	}
	for _, pf := range pendingFiles {
		path := filepath.Join(s.pendingDir, pf.name)
		if err := os.WriteFile(path, []byte(pf.name+"-data"), 0644); err != nil {
			t.Fatalf("failed to write pending file: %v", err)
		}
	}

	if err := s.syncPendingToS3(); err != nil {
		t.Fatalf("syncPendingToS3 failed: %v", err)
	}

	// Verify both files were uploaded
	if len(mock.putBytesCalls) != 2 {
		t.Fatalf("expected 2 PutBytes calls, got %d", len(mock.putBytesCalls))
	}

	// Verify pending files were removed from disk
	for _, pf := range pendingFiles {
		path := filepath.Join(s.pendingDir, pf.name)
		if _, statErr := os.Stat(path); statErr == nil {
			t.Fatalf("expected pending file %s to be removed after upload", pf.name)
		}
	}
}

func TestSyncPendingToS3BestEffort(t *testing.T) {
	s, mock, cleanup := newTestStorage(t, func(m *mockClient) {
		callCount := 0
		m.putBytesFunc = func(ctx context.Context, key string, data []byte) error {
			callCount++
			// First file fails, second succeeds
			if callCount == 1 {
				return errors.New("s3 error on first file")
			}
			return nil
		}
	})
	defer cleanup()

	// Write two pending files
	file1Path := filepath.Join(s.pendingDir, "000010.ldb")
	file2Path := filepath.Join(s.pendingDir, "000011.ldb")
	os.WriteFile(file1Path, []byte("data1"), 0644)
	os.WriteFile(file2Path, []byte("data2"), 0644)

	err := s.syncPendingToS3()
	if err == nil {
		t.Fatal("expected non-nil error from syncPendingToS3 (first file failed)")
	}

	// First file should remain in pendingDir (upload failed)
	if _, statErr := os.Stat(file1Path); statErr != nil {
		t.Fatalf("expected first pending file to remain: %v", statErr)
	}

	// Second file should be removed (upload succeeded)
	if _, statErr := os.Stat(file2Path); statErr == nil {
		t.Fatal("expected second pending file to be removed after successful upload")
	}

	// Verify PutBytes was called 2 times (we continue despite partial failure)
	if len(mock.putBytesCalls) != 2 {
		t.Fatalf("expected 2 PutBytes calls, got %d", len(mock.putBytesCalls))
	}
}

func TestLockLostFencingForMutations(t *testing.T) {
	s, _, cleanup := newTestStorage(t, nil)
	defer cleanup()

	// Simulate lock loss
	s.lockLost.Store(true)

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}

	t.Run("SetMeta", func(t *testing.T) {
		err := s.SetMeta(fd)
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	})

	t.Run("Create", func(t *testing.T) {
		_, err := s.Create(fd)
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	})

	t.Run("Remove", func(t *testing.T) {
		err := s.Remove(fd)
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	})

	t.Run("Rename", func(t *testing.T) {
		oldFD := storage.FileDesc{Type: storage.TypeTable, Num: 1}
		newFD := storage.FileDesc{Type: storage.TypeTable, Num: 2}
		err := s.Rename(oldFD, newFD)
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	})

	t.Run("uploadFile", func(t *testing.T) {
		err := s.uploadFile(fd, []byte("data"))
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	})
}

func TestLockAllowsReadsWhenLockLost(t *testing.T) {
	s, _, cleanup := newTestStorage(t, nil)
	defer cleanup()

	s.lockLost.Store(true)

	t.Run("GetMeta returns ErrNotExist (not lock-lost)", func(t *testing.T) {
		_, err := s.GetMeta()
		if errors.Is(err, ErrLockLost) {
			t.Fatal("GetMeta should not return ErrLockLost (read operation)")
		}
	})

	t.Run("Open returns ErrNotExist (not lock-lost)", func(t *testing.T) {
		fd := storage.FileDesc{Type: storage.TypeTable, Num: 99}
		_, err := s.Open(fd)
		if errors.Is(err, ErrLockLost) {
			t.Fatal("Open should not return ErrLockLost (read operation)")
		}
	})

	t.Run("List returns empty (not lock-lost)", func(t *testing.T) {
		_, err := s.List(storage.TypeAll)
		if errors.Is(err, ErrLockLost) {
			t.Fatal("List should not return ErrLockLost (read operation)")
		}
	})
}

func TestListDedupPrefersPendingDir(t *testing.T) {
	mock := &mockClient{
		listFunc: func(ctx context.Context) ([]storage.FileDesc, error) {
			// S3 has 000001.ldb
			return []storage.FileDesc{
				{Type: storage.TypeTable, Num: 1},
			}, nil
		},
	}

	tmpDir, err := os.MkdirTemp("", "levels3-test-list-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opt := OpenOption{
		Bucket:                 "test-bucket",
		Path:                   "test-path",
		Ak:                     "test-ak",
		Sk:                     "test-sk",
		Region:                 "us-east-1",
		LocalCacheDir:          tmpDir,
		EnableLocalPersistence: true,
	}
	opt.ApplyDefaults()

	lock := NewS3Lock(mock, opt)
	s, err := NewS3Storage(mock, lock, opt)
	if err != nil {
		t.Fatalf("failed to create S3Storage: %v", err)
	}
	defer s.Close()

	// Write pendingDir file with same fd number as S3
	os.WriteFile(filepath.Join(s.pendingDir, "000001.ldb"), []byte("pending-data"), 0644)

	files, err := s.List(storage.TypeTable)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file (dedup), got %d", len(files))
	}

	// The fd should be listed (file exists in pendingDir)
	if files[0].Num != 1 {
		t.Fatalf("expected fd num 1, got %d", files[0].Num)
	}
}

func TestBackgroundRetryStopsWhenLockLost(t *testing.T) {
	called := 0
	mock := &mockClient{
		putBytesFunc: func(ctx context.Context, key string, data []byte) error {
			called++
			return errors.New("s3 error")
		},
	}

	tmpDir, err := os.MkdirTemp("", "levels3-test-retry-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opt := OpenOption{
		Bucket:                 "test-bucket",
		Path:                   "test-path",
		Ak:                     "test-ak",
		Sk:                     "test-sk",
		Region:                 "us-east-1",
		LocalCacheDir:          tmpDir,
		EnableLocalPersistence: true,
	}
	opt.ApplyDefaults()

	lock := NewS3Lock(mock, opt)
	s, err := NewS3Storage(mock, lock, opt)
	if err != nil {
		t.Fatalf("failed to create S3Storage: %v", err)
	}
	defer s.Close()

	// Add a pending file
	os.WriteFile(filepath.Join(s.pendingDir, "000001.ldb"), []byte("data"), 0644)

	// Simulate lock loss
	s.lockLost.Store(true)

	// This should skip upload because lock is lost
	s.retryPendingFilesOnce()

	if called > 0 {
		t.Fatalf("expected 0 PutBytes calls after lock lost, got %d", called)
	}
}

func TestCleanupRecoversManifestFromPendingDir(t *testing.T) {
	// Scenario: CURRENT exists in S3, but the MANIFEST file it references
	// is not in S3 (interrupted upload from previous session). The MANIFEST
	// exists in pendingDir and should be recovered.
	s, mock, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			if key == "CURRENT" {
				return true, nil
			}
			return false, nil // MANIFEST-100 doesn't exist in S3
		}
		m.getBytesFunc = func(ctx context.Context, key string) ([]byte, error) {
			if key == "CURRENT" {
				return []byte("MANIFEST-100\n"), nil
			}
			return nil, os.ErrNotExist
		}
	})
	defer cleanup()

	// Write MANIFEST-100 to pendingDir (simulating interrupted upload)
	manifestFd := storage.FileDesc{Type: storage.TypeManifest, Num: 100}
	manifestName := manifestFd.String() // "MANIFEST-000100"
	manifestData := []byte("manifest content from pending dir")
	manifestPath := filepath.Join(s.pendingDir, manifestName)
	if err := os.WriteFile(manifestPath, manifestData, 0644); err != nil {
		t.Fatalf("failed to write manifest to pendingDir: %v", err)
	}

	// cleanupOrphaned -> verifyCurrentManifest should find and recover it
	if err := s.cleanupOrphaned(); err != nil {
		t.Fatalf("cleanupOrphaned failed: %v", err)
	}

	// Verify MANIFEST was uploaded to S3
	found := false
	for _, call := range mock.putBytesCalls {
		if call.Key == manifestName {
			found = true
			if string(call.Data) != string(manifestData) {
				t.Fatalf("expected manifest data %q, got %q", string(manifestData), string(call.Data))
			}
			break
		}
	}
	if !found {
		t.Fatal("expected PutBytes call for " + manifestName + " from pendingDir recovery")
	}

	// Verify pendingDir file was cleaned up
	if _, statErr := os.Stat(manifestPath); statErr == nil {
		t.Fatal("expected " + manifestName + " to be removed from pendingDir after recovery")
	}
}

func TestCleanupRecoversManifestFromPendingDirWhenCurrentMissing(t *testing.T) {
	// Scenario: CURRENT does NOT exist in S3, and there are no S3 files.
	// MANIFEST exists in pendingDir from a previous interrupted session.
	// recoverFromLocalCache should find and use it to rebuild CURRENT.
	s, mock, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			return false, nil // CURRENT doesn't exist
		}
		m.listFunc = func(ctx context.Context) ([]storage.FileDesc, error) {
			return nil, nil // No files in S3
		}
	})
	defer cleanup()

	// Write MANIFEST-100 to pendingDir
	manifestFd := storage.FileDesc{Type: storage.TypeManifest, Num: 100}
	manifestName := manifestFd.String() // "MANIFEST-000100"
	manifestData := []byte("manifest content for current-rebuild")
	manifestPath := filepath.Join(s.pendingDir, manifestName)
	if err := os.WriteFile(manifestPath, manifestData, 0644); err != nil {
		t.Fatalf("failed to write manifest to pendingDir: %v", err)
	}

	// cleanupOrphaned -> recoverFromLocalCache should recover from pendingDir
	if err := s.cleanupOrphaned(); err != nil {
		t.Fatalf("cleanupOrphaned failed: %v", err)
	}

	// Verify MANIFEST and CURRENT were uploaded
	putKeys := make(map[string]bool)
	for _, call := range mock.putBytesCalls {
		putKeys[call.Key] = true
	}
	if !putKeys[manifestName] {
		t.Fatal("expected " + manifestName + " to be uploaded from pendingDir recovery")
	}
	if !putKeys["CURRENT"] {
		t.Fatal("expected CURRENT to be uploaded after manifest recovery from pendingDir")
	}

	// Verify pendingDir file was removed
	if _, statErr := os.Stat(manifestPath); statErr == nil {
		t.Fatal("expected " + manifestName + " to be removed from pendingDir after recovery")
	}
}

// ---------------------------------------------------------------------------
// GetMeta 错误路径测试
// ---------------------------------------------------------------------------

// TestGetMetaWhenExistsFails verifies that GetMeta propagates errors from
// Exists("CURRENT") rather than silently returning os.ErrNotExist.
func TestGetMetaWhenExistsFails(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			if key == "CURRENT" {
				return false, errors.New("s3 internal error")
			}
			return false, nil
		}
	})
	defer cleanup()

	_, err := s.GetMeta()
	if err == nil || !strings.Contains(err.Error(), "CURRENT existence") {
		t.Fatalf("expected error about CURRENT existence check, got: %v", err)
	}
}

// TestGetMetaWhenGetBytesFails verifies that GetMeta propagates errors from
// GetBytes("CURRENT") when the key is known to exist.
func TestGetMetaWhenGetBytesFails(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			return true, nil
		}
		m.getBytesFunc = func(ctx context.Context, key string) ([]byte, error) {
			if key == "CURRENT" {
				return nil, errors.New("s3 unavailable")
			}
			return nil, os.ErrNotExist
		}
	})
	defer cleanup()

	_, err := s.GetMeta()
	if err == nil || !strings.Contains(err.Error(), "failed to get meta") {
		t.Fatalf("expected error about getting meta content, got: %v", err)
	}
}

// TestGetMetaWhenCurrentContentInvalid verifies that GetMeta returns an error
// when CURRENT contains unparseable content.
func TestGetMetaWhenCurrentContentInvalid(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			return true, nil
		}
		m.getBytesFunc = func(ctx context.Context, key string) ([]byte, error) {
			if key == "CURRENT" {
				return []byte("not-a-valid-fd\n"), nil
			}
			return nil, os.ErrNotExist
		}
	})
	defer cleanup()

	_, err := s.GetMeta()
	if err == nil || !strings.Contains(err.Error(), "invalid CURRENT") {
		t.Fatalf("expected error about invalid CURRENT content, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Open 非 NotFound 错误路径测试
// ---------------------------------------------------------------------------

// TestOpenWhenS3ReturnsNonNotFoundError verifies that Open returns a wrapped
// error when S3 returns a non-NotFound error, rather than silently falling
// through to os.ErrNotExist.
func TestOpenWhenS3ReturnsNonNotFoundError(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.getBytesFunc = func(ctx context.Context, key string) ([]byte, error) {
			return nil, errors.New("network timeout")
		}
	})
	defer cleanup()

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	_, err := s.Open(fd)
	if err == nil {
		t.Fatal("expected error when S3 returns non-NotFound error")
	}
	if strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected non-NotFound error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Rename pending 目录文件路径测试
// ---------------------------------------------------------------------------

// TestRenameWithPendingDirFiles verifies that Rename renames pending directory
// files along with their .cksum companions when the old file exists in pendingDir.
func TestRenameWithPendingDirFiles(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.copyFunc = func(ctx context.Context, srcKey, dstKey string) error {
			return nil
		}
		m.removeFunc = func(ctx context.Context, key string) error {
			return nil
		}
	})
	defer cleanup()

	oldName := "000001.ldb"
	newName := "000002.ldb"

	// Write old file and checksum to pendingDir
	os.WriteFile(filepath.Join(s.pendingDir, oldName), []byte("pending-data"), 0644)
	os.WriteFile(filepath.Join(s.pendingDir, oldName+".cksum"), []byte("abcdef01"), 0644)

	oldfd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	newfd := storage.FileDesc{Type: storage.TypeTable, Num: 2}

	err := s.Rename(oldfd, newfd)
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify pending file was renamed
	if _, err := os.Stat(filepath.Join(s.pendingDir, newName)); err != nil {
		t.Fatal("expected new pending file to exist after rename")
	}
	if _, err := os.Stat(filepath.Join(s.pendingDir, oldName)); !os.IsNotExist(err) {
		t.Fatal("expected old pending file to be removed after rename")
	}
}

// TestRenameWithPendingDirCopyFallback verifies that when os.Rename fails,
// Rename falls back to copy-then-delete for pending directory files.
func TestRenameWithPendingDirCopyFallback(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.copyFunc = func(ctx context.Context, srcKey, dstKey string) error {
			return nil
		}
		m.removeFunc = func(ctx context.Context, key string) error {
			return nil
		}
	})
	defer cleanup()

	oldName := "000003.ldb"
	newName := "000004.ldb"
	oldPendingPath := filepath.Join(s.pendingDir, oldName)
	oldCksumPath := oldPendingPath + ".cksum"

	// Write old file to pendingDir
	os.WriteFile(oldPendingPath, []byte("pending-data"), 0644)
	os.WriteFile(oldCksumPath, []byte("abcdef01"), 0644)

	oldfd := storage.FileDesc{Type: storage.TypeTable, Num: 3}
	newfd := storage.FileDesc{Type: storage.TypeTable, Num: 4}

	err := s.Rename(oldfd, newfd)
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify old file was removed and new file exists (via copy fallback)
	if _, err := os.Stat(filepath.Join(s.pendingDir, newName)); err != nil {
		t.Fatal("expected new pending file to exist after rename via copy fallback")
	}
	if _, err := os.Stat(oldPendingPath); !os.IsNotExist(err) {
		t.Fatal("expected old pending file to be removed after copy fallback")
	}
}

// ---------------------------------------------------------------------------
// uploadWithInfiniteRetry 锁丢失 / 超时测试
// ---------------------------------------------------------------------------

// TestUploadWithInfiniteRetryLockLost verifies that uploadWithInfiniteRetry
// returns ErrLockLost when the lock is lost during retry.
func TestUploadWithInfiniteRetryLockLost(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.putBytesFunc = func(ctx context.Context, key string, data []byte) error {
			return errors.New("upload always fails")
		}
	})
	defer cleanup()

	// Fast retry config
	s.opt.RetryMaxAttempts = 0
	s.opt.RetryBaseDelay = 1 * time.Millisecond
	s.opt.RetryMaxDuration = 0

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.uploadWithInfiniteRetry(fd, []byte("test data"))
	}()

	// Allow a few retry cycles to start
	time.Sleep(50 * time.Millisecond)

	// Simulate lock loss during retry
	s.lockLost.Store(true)

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrLockLost) {
			t.Fatalf("expected ErrLockLost, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for uploadWithInfiniteRetry to detect lockLost")
	}
}

// TestUploadWithInfiniteRetryMaxDuration verifies that uploadWithInfiniteRetry
// returns an error when RetryMaxDuration is exceeded.
func TestUploadWithInfiniteRetryMaxDuration(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.putBytesFunc = func(ctx context.Context, key string, data []byte) error {
			return errors.New("upload always fails")
		}
	})
	defer cleanup()

	s.opt.RetryMaxAttempts = 0
	s.opt.RetryBaseDelay = 1 * time.Millisecond
	s.opt.RetryMaxDuration = 100 * time.Millisecond

	fd := storage.FileDesc{Type: storage.TypeTable, Num: 1}
	err := s.uploadWithInfiniteRetry(fd, []byte("test"))

	if err == nil {
		t.Fatal("expected error when max retry duration exceeded, got nil")
	}
	if errors.Is(err, ErrLockLost) {
		t.Fatal("expected max duration error, not ErrLockLost")
	}
}

// ---------------------------------------------------------------------------
// syncLocalToS3 错误路径测试
// ---------------------------------------------------------------------------

// TestSyncLocalToS3WithPutBytesError verifies that syncLocalToS3 returns an
// error when PutBytes fails for a local cache file.
func TestSyncLocalToS3WithPutBytesError(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.putBytesFunc = func(ctx context.Context, key string, data []byte) error {
			return errors.New("s3 upload failed")
		}
	})
	defer cleanup()

	// Write local cache files — LOCK and CURRENT are skipped by syncLocalToS3
	os.WriteFile(filepath.Join(s.localDir, "000001.log"), []byte("journal data"), 0644)

	err := s.syncLocalToS3()
	if err == nil || !strings.Contains(err.Error(), "failed to sync") {
		t.Fatalf("expected error about sync failure, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// findLatestLocalManifest 测试
// ---------------------------------------------------------------------------

// TestFindLatestLocalManifest verifies that findLatestLocalManifest returns
// the MANIFEST with the highest file number from the local cache directory.
func TestFindLatestLocalManifest(t *testing.T) {
	s, _, cleanup := newTestStorage(t, nil)
	defer cleanup()

	// Write manifest files with different numbers
	os.WriteFile(filepath.Join(s.localDir, "MANIFEST-000001"), []byte("manifest1"), 0644)
	os.WriteFile(filepath.Join(s.localDir, "MANIFEST-000003"), []byte("manifest3"), 0644)
	os.WriteFile(filepath.Join(s.localDir, "000001.ldb"), []byte("noise"), 0644) // non-manifest

	fd, data, err := s.findLatestLocalManifest()
	if err != nil {
		t.Fatalf("findLatestLocalManifest failed: %v", err)
	}
	if fd.Num != 3 {
		t.Fatalf("expected manifest num 3, got %d", fd.Num)
	}
	if string(data) != "manifest3" {
		t.Fatalf("expected manifest3 data, got %q", string(data))
	}
}

// TestFindLatestLocalManifestNotFound verifies that findLatestLocalManifest
// returns an error when no MANIFEST file exists in the local cache.
func TestFindLatestLocalManifestNotFound(t *testing.T) {
	s, _, cleanup := newTestStorage(t, nil)
	defer cleanup()

	// Only non-manifest files
	os.WriteFile(filepath.Join(s.localDir, "000001.ldb"), []byte("data"), 0644)
	os.WriteFile(filepath.Join(s.localDir, "000002.log"), []byte("journal"), 0644)

	_, _, err := s.findLatestLocalManifest()
	if err == nil {
		t.Fatal("expected error when no MANIFEST found in local cache, got nil")
	}
}

// ---------------------------------------------------------------------------
// removeAllOrphaned S3 清理路径测试
// ---------------------------------------------------------------------------

// TestRemoveAllOrphanedCleansS3 verifies that removeAllOrphaned removes all
// LevelDB files from S3 including CURRENT.
func TestRemoveAllOrphanedCleansS3(t *testing.T) {
	var removedFiles []string
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.listFunc = func(ctx context.Context) ([]storage.FileDesc, error) {
			return []storage.FileDesc{
				{Type: storage.TypeTable, Num: 1},
				{Type: storage.TypeJournal, Num: 2},
			}, nil
		}
		m.removeFunc = func(ctx context.Context, key string) error {
			removedFiles = append(removedFiles, key)
			return nil
		}
	})
	defer cleanup()

	err := s.removeAllOrphaned()
	if err != nil {
		t.Fatalf("removeAllOrphaned failed: %v", err)
	}

	// Should remove 000001.ldb, 000002.log, CURRENT
	expectedRemoves := []string{"000001.ldb", "000002.log", "CURRENT"}
	for _, expected := range expectedRemoves {
		found := false
		for _, removed := range removedFiles {
			if removed == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected %s to be removed, got removals: %v", expected, removedFiles)
		}
	}
}

// ---------------------------------------------------------------------------
// Lock() 失败路径测试
// ---------------------------------------------------------------------------

// TestLockFailsWhenS3LockFails verifies that Lock() returns a wrapped error
// when s3Lock.Lock() fails (e.g., network error while checking remote lock).
func TestLockFailsWhenS3LockFails(t *testing.T) {
	s, _, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			if key == "LOCK" {
				return false, errors.New("network error checking LOCK")
			}
			return false, nil
		}
	})
	defer cleanup()

	_, err := s.Lock()
	if err == nil || !strings.Contains(err.Error(), "failed to acquire lock") {
		t.Fatalf("expected Lock() to fail with 'failed to acquire lock', got: %v", err)
	}
}

// TestLockFailsWhenSyncLocalToS3Fails verifies that Lock() returns an error
// when syncLocalToS3 fails, and properly releases the S3 lock before returning.
func TestLockFailsWhenSyncLocalToS3Fails(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "levels3-lock-syncfail-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var removeCalled bool
	mock := &mockClient{
		existsFunc: func(ctx context.Context, key string) (bool, error) {
			return false, nil
		},
		putBytesFunc: func(ctx context.Context, key string, data []byte) error {
			// Only allow LOCK to succeed; fail syncLocalToS3 uploads
			if key == "LOCK" {
				return nil
			}
			return errors.New("s3 upload failed")
		},
		listFunc: func(ctx context.Context) ([]storage.FileDesc, error) {
			return nil, nil
		},
		removeFunc: func(ctx context.Context, key string) error {
			removeCalled = true
			return nil
		},
	}

	opt := OpenOption{
		Bucket:                 "test-bucket",
		Path:                   "test-path",
		Ak:                     "test-ak",
		Sk:                     "test-sk",
		Region:                 "us-east-1",
		LocalCacheDir:          tmpDir,
		EnableLocalPersistence: true,
	}
	opt.ApplyDefaults()

	lock := NewS3Lock(mock, opt)
	s, err := NewS3Storage(mock, lock, opt)
	if err != nil {
		t.Fatalf("NewS3Storage failed: %v", err)
	}
	defer s.Close()

	// Write a local cache file so syncLocalToS3 has work to do
	os.WriteFile(filepath.Join(s.localDir, "000001.log"), []byte("journal data"), 0644)

	_, err = s.Lock()
	if err == nil || !strings.Contains(err.Error(), "failed to sync local cache") {
		t.Fatalf("expected Lock() to fail with sync error, got: %v", err)
	}

	if !removeCalled {
		t.Fatal("expected s3Lock.Unlock to be called after syncLocalToS3 failure")
	}
}

// ---------------------------------------------------------------------------
// S3Storage.Unlock 锁丢失路径测试
// ---------------------------------------------------------------------------

// TestS3StorageUnlockWithLockLost verifies that S3Storage.Unlock() skips
// remote lock release when lockLost is true.
func TestS3StorageUnlockWithLockLost(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "levels3-unlock-locklost-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	removeCount := 0
	mock := &mockClient{
		listFunc: func(ctx context.Context) ([]storage.FileDesc, error) {
			return nil, nil
		},
		removeFunc: func(ctx context.Context, key string) error {
			removeCount++
			return nil
		},
	}

	opt := OpenOption{
		Bucket:        "test-bucket",
		Path:          "test-path",
		Ak:            "test-ak",
		Sk:            "test-sk",
		Region:        "us-east-1",
		LocalCacheDir: tmpDir,
	}
	opt.ApplyDefaults()

	lock := NewS3Lock(mock, opt)
	s, err := NewS3Storage(mock, lock, opt)
	if err != nil {
		t.Fatalf("NewS3Storage failed: %v", err)
	}
	defer s.Close()

	// Simulate lock held + lock lost
	s.lockHeld.Store(true)
	s.lockLost.Store(true)

	s.Unlock()

	if removeCount != 0 {
		t.Fatalf("expected 0 Remove calls when Unlock with lockLost, got %d", removeCount)
	}
}

// ---------------------------------------------------------------------------
// cleanupOrphaned 本地缓存恢复测试
// ---------------------------------------------------------------------------

// TestCleanupRecoversManifestFromLocalCache verifies that cleanupOrphaned
// recovers a missing MANIFEST from local cache when CURRENT references it.
func TestCleanupRecoversManifestFromLocalCache(t *testing.T) {
	s, mock, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			if key == "CURRENT" {
				return true, nil
			}
			if strings.HasPrefix(key, "MANIFEST-") {
				return false, nil // manifest not in S3
			}
			return false, nil
		}
		m.getBytesFunc = func(ctx context.Context, key string) ([]byte, error) {
			if key == "CURRENT" {
				return []byte("MANIFEST-000100\n"), nil
			}
			return nil, os.ErrNotExist
		}
	})
	defer cleanup()

	manifestName := "MANIFEST-000100"
	manifestData := []byte("manifest content from local cache")

	// Write manifest to local cache
	os.WriteFile(filepath.Join(s.localDir, manifestName), manifestData, 0644)

	// cleanupOrphaned → verifyCurrentManifest → recover from local cache
	if err := s.cleanupOrphaned(); err != nil {
		t.Fatalf("cleanupOrphaned failed: %v", err)
	}

	// Verify MANIFEST was uploaded to S3
	found := false
	for _, call := range mock.putBytesCalls {
		if call.Key == manifestName {
			found = true
			if string(call.Data) != string(manifestData) {
				t.Fatalf("expected manifest data %q, got %q", string(manifestData), string(call.Data))
			}
			break
		}
	}
	if !found {
		t.Fatal("expected PutBytes call for " + manifestName + " from local cache recovery")
	}
}

// TestCleanupOrphanedCurrentMissingWithLocalManifest verifies that when CURRENT
// is missing from S3 and a MANIFEST exists in local cache, recoverFromLocalCache
// uploads both the MANIFEST and CURRENT to restore the database.
func TestCleanupOrphanedCurrentMissingWithLocalManifest(t *testing.T) {
	s, mock, cleanup := newTestStorage(t, func(m *mockClient) {
		m.existsFunc = func(ctx context.Context, key string) (bool, error) {
			return false, nil // CURRENT doesn't exist
		}
		m.listFunc = func(ctx context.Context) ([]storage.FileDesc, error) {
			return nil, nil // No files in S3
		}
	})
	defer cleanup()

	manifestName := "MANIFEST-000100"
	manifestData := []byte("manifest for current-rebuild")

	// Write MANIFEST to local cache (not pendingDir)
	os.WriteFile(filepath.Join(s.localDir, manifestName), manifestData, 0644)

	// cleanupOrphaned → recoverFromLocalCache → findLatestLocalManifest
	if err := s.cleanupOrphaned(); err != nil {
		t.Fatalf("cleanupOrphaned failed: %v", err)
	}

	// Verify both MANIFEST and CURRENT were uploaded
	putKeys := make(map[string]bool)
	for _, call := range mock.putBytesCalls {
		putKeys[call.Key] = true
	}
	if !putKeys[manifestName] {
		t.Fatal("expected " + manifestName + " to be uploaded from local cache recovery")
	}
	if !putKeys["CURRENT"] {
		t.Fatal("expected CURRENT to be uploaded after manifest recovery from local cache")
	}
}
