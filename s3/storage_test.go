package s3

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

// mockClient implements S3ClientAPI for testing.
type mockClient struct {
	// Configure behavior
	putBytesFunc       func(ctx context.Context, key string, data []byte) error
	listFunc           func(ctx context.Context) ([]storage.FileDesc, error)
	existsFunc         func(ctx context.Context, key string) (bool, error)
	getBytesFunc       func(ctx context.Context, key string) ([]byte, error)
	removeFunc         func(ctx context.Context, key string) error
	copyFunc           func(ctx context.Context, srcKey, dstKey string) error
	putIfNotExistsFunc func(ctx context.Context, key string, data []byte) (bool, error)

	// Record calls
	putBytesCalls []struct {
		Key  string
		Data []byte
	}
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
