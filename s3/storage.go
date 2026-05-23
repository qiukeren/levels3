package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type memFile struct {
	fd   storage.FileDesc
	data []byte
}

type memReader struct {
	*bytes.Reader
	closed bool
}

func (r *memReader) Close() error {
	if r.closed {
		return storage.ErrClosed
	}
	r.closed = true
	return nil
}

type memWriter struct {
	fd        storage.FileDesc
	buf       *bytes.Buffer
	closed    bool
	syncFn    func() error
	localPath string // Path to local cache file for eager persistence
	logFn     func(string)
}

func (w *memWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, storage.ErrClosed
	}
	n, err := w.buf.Write(p)
	if err != nil {
		return n, err
	}
	// Eagerly persist metadata files (MANIFEST, journal) to local cache on every Write().
	// This ensures the local cache always has the latest content even if the process
	// crashes before Sync()/Close(). Without this, goleveldb's newManifest() creates
	// a new manifest, updates CURRENT, deletes the old manifest, but never Sync()s
	// the new manifest writer — leaving the new manifest only in memory buffer.
	if w.localPath != "" {
		if werr := atomicWriteFile(w.localPath, w.buf.Bytes(), 0644); werr != nil {
			w.logFn(fmt.Sprintf("eager local cache write failed: %v", werr))
		}
	}
	return n, nil
}

func (w *memWriter) Sync() error {
	if w.closed {
		return storage.ErrClosed
	}
	if w.syncFn != nil {
		return w.syncFn()
	}
	return nil
}

func (w *memWriter) Close() error {
	if w.closed {
		return storage.ErrClosed
	}
	w.closed = true
	if w.syncFn != nil {
		return w.syncFn()
	}
	return nil
}

type storageLocker struct {
	s *S3Storage
}

func (l *storageLocker) Unlock() {
	// If the lock was already lost (lease expired, taken by another process),
	// do NOT attempt to release the remote lock — it no longer belongs to us.
	// Releasing it would delete the other process's lock, causing split-brain.
	if l.s.lockLost.Load() {
		l.s.Log("storageLocker.Unlock: lock already lost, skipping remote lock release")
		return
	}

	if err := l.s.s3Lock.Unlock(l.s.ctx); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), l.s.opt.RequestTimeout)
		defer cancel()
		if err2 := l.s.s3Lock.Unlock(ctx); err2 != nil {
			l.s.Log(fmt.Sprintf("failed to unlock: %v", err2))
		}
	}
}

type S3Storage struct {
	client      S3ClientAPI
	s3Lock      *S3Lock
	opt         OpenOption
	ctx         context.Context
	cancel      context.CancelFunc
	cache       *lru.Cache[string, *memFile]
	mu          sync.Mutex
	closed      bool
	localDir    string
	pendingDir  string        // 本地目录，用于存储上传失败的文件，确保数据不丢失
	retryStopCh chan struct{} // 后台重试停止信号
	retryWg     sync.WaitGroup
	lockLost    atomic.Bool // 锁丢失标记，设置后禁止写入
	lockHeld    atomic.Bool // 是否持有分布式锁，后台重试前检查
}

func NewS3Storage(client S3ClientAPI, s3Lock *S3Lock, opt OpenOption) (*S3Storage, error) {
	opt.ApplyDefaults()
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	cache, err := lru.New[string, *memFile](opt.MaxCacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	localDir := opt.LocalPath()
	if localDir != "" {
		if err := os.MkdirAll(localDir, 0755); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create local cache dir: %w", err)
		}
	}

	// 初始化 pending upload 目录，用于上传失败时本地持久化
	pendingDir := ""
	if opt.EnableLocalPersistence {
		pendingDir = opt.LocalPersistencePath()
		if err := os.MkdirAll(pendingDir, 0755); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create pending upload dir: %w", err)
		}
	}

	ss := &S3Storage{
		client:      client,
		s3Lock:      s3Lock,
		opt:         opt,
		ctx:         ctx,
		cancel:      cancel,
		cache:       cache,
		localDir:    localDir,
		pendingDir:  pendingDir,
		retryStopCh: make(chan struct{}),
	}

	// 启动后台重试 goroutine，无限重试直到成功
	if pendingDir != "" {
		ss.retryWg.Add(1)
		go ss.backgroundRetryLoop()
	}

	return ss, nil
}

func (s *S3Storage) Lock() (storage.Locker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, storage.ErrClosed
	}

	if err := s.s3Lock.Lock(s.ctx); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// 锁刚获取立即启动租约监控，确保在后继 recovery 操作期间
	// 如果 lease renewal 失败能立即检测到并禁止写入，防止 split-brain
	s.startLeaseMonitoring()

	// 优先恢复上一个崩溃 session 中未上传的 pending 文件
	if err := s.syncPendingToS3(); err != nil {
		s.Log(fmt.Sprintf("syncPendingToS3 had errors (continuing): %v", err))
	}

	if err := s.syncLocalToS3(); err != nil {
		if unlockErr := s.s3Lock.Unlock(s.ctx); unlockErr != nil {
			s.Log(fmt.Sprintf("failed to unlock after sync error: %v", unlockErr))
		}
		return nil, fmt.Errorf("failed to sync local cache to S3: %w", err)
	}

	// Clean up orphaned files from interrupted sessions.
	// This handles the case where goleveldb's newManifest() created a new manifest,
	// updated CURRENT, deleted the old manifest, but crashed before the new manifest
	// was synced to S3. Without cleanup, subsequent opens fail with "file missing".
	if err := s.cleanupOrphaned(); err != nil {
		s.Log(fmt.Sprintf("failed to cleanup orphaned files: %v", err))
	}

	s.lockHeld.Store(true)

	return &storageLocker{s: s}, nil
}

func (s *S3Storage) Unlock() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lockHeld.Store(false)

	// 如果锁已丢失（租约过期、被其他进程抢占），不释放远程锁
	// 释放远程锁会删除对方的锁，导致 split-brain
	if s.lockLost.Load() {
		s.Log("S3Storage.Unlock: lock already lost, skipping remote lock release")
		return
	}

	if err := s.s3Lock.Unlock(s.ctx); err != nil {
		// 如果第一次解锁失败（网络问题），s3Lock.Unlock 已标记 stopCh 关闭
		// 重试解锁是安全的（s3Lock.Unlock 现在是幂等的）
		ctx, cancel := context.WithTimeout(context.Background(), s.opt.RequestTimeout)
		defer cancel()
		if err2 := s.s3Lock.Unlock(ctx); err2 != nil {
			s.Log(fmt.Sprintf("failed to unlock: %v", err2))
		}
	}
}

func (s *S3Storage) SetMeta(fd storage.FileDesc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}
	if s.lockLost.Load() {
		return ErrLockLost
	}

	key := "CURRENT"
	content := fd.String() + "\n"

	if err := s.client.PutBytes(s.ctx, key, []byte(content)); err != nil {
		return fmt.Errorf("failed to set meta: %w", err)
	}

	return nil
}

func (s *S3Storage) GetMeta() (storage.FileDesc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.FileDesc{}, storage.ErrClosed
	}

	// Check existence first, then read
	exists, err := s.client.Exists(s.ctx, "CURRENT")
	if err != nil {
		return storage.FileDesc{}, fmt.Errorf("failed to check CURRENT existence: %w", err)
	}
	if !exists {
		return storage.FileDesc{}, os.ErrNotExist
	}

	data, err := s.client.GetBytes(s.ctx, "CURRENT")
	if err != nil {
		return storage.FileDesc{}, fmt.Errorf("failed to get meta: %w", err)
	}

	line := strings.TrimSpace(string(data))
	fd, ok := parseFileDesc(line)
	if !ok {
		return storage.FileDesc{}, fmt.Errorf("invalid CURRENT file content: %s", line)
	}

	exists, errExists := s.client.Exists(s.ctx, fd.String())
	if errExists != nil {
		return storage.FileDesc{}, fmt.Errorf("failed to check meta file existence: %w", errExists)
	}
	if !exists {
		return storage.FileDesc{}, os.ErrNotExist
	}

	return fd, nil
}

func parseFileDesc(s string) (storage.FileDesc, bool) {
	var fd storage.FileDesc
	_, err := fmt.Sscanf(s, "%d.%s", &fd.Num, &s)
	if err == nil {
		switch s {
		case "log":
			fd.Type = storage.TypeJournal
		case "ldb", "sst":
			fd.Type = storage.TypeTable
		case "tmp":
			fd.Type = storage.TypeTemp
		default:
			return fd, false
		}
		return fd, true
	}

	n, _ := fmt.Sscanf(s, "MANIFEST-%d", &fd.Num)
	if n == 1 {
		fd.Type = storage.TypeManifest
		return fd, true
	}

	return fd, false
}

func (s *S3Storage) List(ft storage.FileType) ([]storage.FileDesc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, storage.ErrClosed
	}

	allFiles, err := s.client.List(s.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// 同时收集 pending 目录中的文件
	var pendingFiles []storage.FileDesc
	if s.pendingDir != "" {
		entries, readErr := os.ReadDir(s.pendingDir)
		if readErr == nil {
			for _, entry := range entries {
				if !entry.IsDir() {
					if fd, ok := parseFileDesc(entry.Name()); ok {
						pendingFiles = append(pendingFiles, fd)
					}
				}
			}
		}
	}

	// 合并两个列表，去重时优先使用 pendingDir 版本（pendingDir 中的数据可能是更新版本）
	seen := make(map[string]bool)
	var filtered []storage.FileDesc

	// 先加 pendingDir 文件（优先级更高）
	for _, fd := range pendingFiles {
		key := fd.String()
		seen[key] = true
		if fd.Type&ft != 0 {
			filtered = append(filtered, fd)
		}
	}

	// 再加 S3 中没有的文件
	for _, fd := range allFiles {
		key := fd.String()
		if !seen[key] && fd.Type&ft != 0 {
			filtered = append(filtered, fd)
		}
	}

	return filtered, nil
}

func (s *S3Storage) Open(fd storage.FileDesc) (storage.Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, storage.ErrClosed
	}

	name := fd.String()
	cacheKey := s.makeCacheKey(fd)

	if file, ok := s.cache.Get(cacheKey); ok {
		reader := bytes.NewReader(file.data)
		return &memReader{Reader: reader}, nil
	}

	// 1. 先尝试从 S3 读取
	data, err := s.client.GetBytes(s.ctx, name)
	if err == nil {
		if len(data) <= int(s.opt.MaxFileSize) {
			memFile := &memFile{fd: fd, data: data}
			s.cache.Add(cacheKey, memFile)
		}
		reader := bytes.NewReader(data)
		return &memReader{Reader: reader}, nil
	}

	// 2. S3 没找到，尝试从 pending 目录读取
	if s.pendingDir != "" {
		localPath := filepath.Join(s.pendingDir, name)
		if localData, readErr := os.ReadFile(localPath); readErr == nil {
			s.Log(fmt.Sprintf("Open: 从 pending 目录读取文件: %s", name))
			// 不需要加入 cache，因为后台正在上传，等上传成功后再处理
			reader := bytes.NewReader(localData)
			return &memReader{Reader: reader}, nil
		}
	}

	// 3. 都没找到，返回错误
	if errors.Is(err, os.ErrNotExist) || isNotFoundError(err) {
		return nil, os.ErrNotExist
	}
	return nil, fmt.Errorf("failed to open file %s: %w", name, err)
}

func (s *S3Storage) Create(fd storage.FileDesc) (storage.Writer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, storage.ErrClosed
	}
	if s.lockLost.Load() {
		return nil, ErrLockLost
	}

	name := fd.String()
	buf := &bytes.Buffer{}

	writer := &memWriter{
		fd:  fd,
		buf: buf,
		syncFn: func() error {
			return s.uploadFile(fd, buf.Bytes())
		},
		logFn: s.Log,
	}

	cacheKey := s.makeCacheKey(fd)
	s.cache.Add(cacheKey, &memFile{fd: fd, data: nil})

	if s.shouldCacheLocally(fd) {
		localPath := filepath.Join(s.localDir, name)
		if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create local dir: %w", err)
		}
		writer.localPath = localPath
		writer.syncFn = func() error {
			if err := atomicWriteFile(localPath, buf.Bytes(), 0644); err != nil {
				return fmt.Errorf("failed to write local cache: %w", err)
			}
			return s.uploadFile(fd, buf.Bytes())
		}
	}

	return writer, nil
}

func (s *S3Storage) uploadFile(fd storage.FileDesc, data []byte) error {
	name := fd.String()
	if !isMetadataKey(name) && int64(len(data)) > s.opt.MaxFileSize {
		return ErrFileTooLarge
	}
	if s.lockLost.Load() {
		return ErrLockLost
	}

	s.Log(fmt.Sprintf("uploadFile start: %s (%d bytes)", name, len(data)))
	if err := s.client.PutBytes(s.ctx, name, data); err != nil {
		s.Log(fmt.Sprintf("uploadFile FAIL: %s (%v)", name, err))

		// 上传失败时尝试本地持久化作为 crash-recovery safety net
		if s.pendingDir != "" {
			if persistErr := s.persistToLocal(fd, data); persistErr != nil {
				s.Log(fmt.Sprintf("uploadFile: 本地持久化也失败: %s (%v)", name, persistErr))
			} else {
				s.Log(fmt.Sprintf("uploadFile: S3上传失败但已本地持久化，数据可在重启后恢复: %s", name))
			}
		}

		// 返回真实 S3 错误，让 LevelDB 知道写入未提交
		return fmt.Errorf("failed to upload file %s: %w", name, err)
	}

	// 上传成功，清理 pending 目录中可能存在的旧文件
	if s.pendingDir != "" {
		pendingPath := filepath.Join(s.pendingDir, name)
		if rmErr := os.Remove(pendingPath); rmErr == nil {
			s.Log(fmt.Sprintf("uploadFile: 清理 pending 目录中的旧文件: %s", name))
		}
	}

	cacheKey := s.makeCacheKey(fd)
	s.cache.Add(cacheKey, &memFile{fd: fd, data: data})

	s.Log(fmt.Sprintf("uploadFile OK: %s (%d bytes)", name, len(data)))
	return nil
}

// persistToLocal 将数据写入本地 pending 目录，确保上传失败时数据不丢失
func (s *S3Storage) persistToLocal(fd storage.FileDesc, data []byte) error {
	if s.pendingDir == "" {
		return errors.New("pending directory not configured")
	}

	name := fd.String()
	localPath := filepath.Join(s.pendingDir, name)

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// 原子写入本地文件（先写临时文件再重命名，防止崩溃产生截断文件）
	if err := atomicWriteFile(localPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write local file: %w", err)
	}

	s.Log(fmt.Sprintf("persistToLocal OK: %s (%d bytes) -> %s", name, len(data), localPath))
	return nil
}

// RetryPendingUploads 重试 pending 目录中积压的上传任务
// 成功上传的文件会从 pending 目录删除
// 返回成功和失败的数量
func (s *S3Storage) RetryPendingUploads() (successCount, failCount int, err error) {
	if s.pendingDir == "" || s.opt.LocalCacheDir == "" {
		return 0, 0, nil
	}

	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to read pending dir: %w", err)
	}

	s.Log(fmt.Sprintf("RetryPendingUploads: 发现 %d 个待上传文件", len(entries)))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		localPath := filepath.Join(s.pendingDir, name)

		data, readErr := os.ReadFile(localPath)
		if readErr != nil {
			s.Log(fmt.Sprintf("RetryPendingUploads: 读取本地文件失败: %s (%v)", name, readErr))
			failCount++
			continue
		}

		fd, ok := parseFileDesc(name)
		if !ok {
			s.Log(fmt.Sprintf("RetryPendingUploads: 无法解析文件名: %s", name))
			failCount++
			continue
		}

		s.Log(fmt.Sprintf("RetryPendingUploads: 重试上传: %s (%d bytes)", name, len(data)))
		uploadErr := s.client.PutBytes(s.ctx, fd.String(), data)
		if uploadErr != nil {
			s.Log(fmt.Sprintf("RetryPendingUploads: 上传失败: %s (%v)", name, uploadErr))
			failCount++
			continue
		}

		// 上传成功，删除本地文件
		if rmErr := os.Remove(localPath); rmErr != nil {
			s.Log(fmt.Sprintf("RetryPendingUploads: 删除本地文件失败: %s (%v)", name, rmErr))
		}
		successCount++
		s.Log(fmt.Sprintf("RetryPendingUploads: 上传成功并删除本地文件: %s", name))
	}

	s.Log(fmt.Sprintf("RetryPendingUploads: 完成，成功=%d, 失败=%d", successCount, failCount))
	return successCount, failCount, nil
}

// HasPendingUploads 检查是否有待上传的文件
func (s *S3Storage) HasPendingUploads() bool {
	if s.pendingDir == "" {
		return false
	}

	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			return true
		}
	}
	return false
}

func (s *S3Storage) Remove(fd storage.FileDesc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}
	if s.lockLost.Load() {
		return ErrLockLost
	}

	name := fd.String()
	cacheKey := s.makeCacheKey(fd)

	s.cache.Remove(cacheKey)

	if s.shouldCacheLocally(fd) {
		localPath := filepath.Join(s.localDir, name)
		os.Remove(localPath)
	}

	// 同时删除 pending 目录中的文件，避免后台继续上传已删除的文件
	if s.pendingDir != "" {
		pendingPath := filepath.Join(s.pendingDir, name)
		rmErr := os.Remove(pendingPath)
		if rmErr == nil {
			s.Log(fmt.Sprintf("Remove: 同时删除 pending 目录中的文件: %s", name))
		}
	}

	if err := s.client.Remove(s.ctx, name); err != nil {
		return fmt.Errorf("failed to remove file %s: %w", name, err)
	}

	return nil
}

func (s *S3Storage) Rename(oldfd, newfd storage.FileDesc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrClosed
	}
	if s.lockLost.Load() {
		return ErrLockLost
	}

	oldName := oldfd.String()
	newName := newfd.String()

	s.Log(fmt.Sprintf("Rename start: %s -> %s", oldName, newName))
	if err := s.client.Copy(s.ctx, oldName, newName); err != nil {
		s.Log(fmt.Sprintf("Rename FAIL (Copy): %s -> %s (%v)", oldName, newName, err))
		return fmt.Errorf("failed to copy %s to %s: %w", oldName, newName, err)
	}

	oldData, err := s.client.GetBytes(s.ctx, oldName)
	if err == nil {
		oldCacheKey := s.makeCacheKey(oldfd)
		newCacheKey := s.makeCacheKey(newfd)
		s.cache.Remove(oldCacheKey)
		s.cache.Add(newCacheKey, &memFile{fd: newfd, data: oldData})
	} else {
		s.Log(fmt.Sprintf("Rename: cache update skipped (GetBytes err: %v)", err))
	}

	// 处理 pending 目录中的文件：如果旧文件在 pending 目录中，需要重命名为新文件名
	if s.pendingDir != "" {
		oldPendingPath := filepath.Join(s.pendingDir, oldName)
		newPendingPath := filepath.Join(s.pendingDir, newName)

		// 检查旧文件是否存在
		if _, statErr := os.Stat(oldPendingPath); statErr == nil {
			// 旧文件存在，重命名为新文件名
			if renameErr := os.Rename(oldPendingPath, newPendingPath); renameErr == nil {
				s.Log(fmt.Sprintf("Rename: 同时重命名 pending 目录中的文件: %s -> %s", oldName, newName))
			} else {
				s.Log(fmt.Sprintf("Rename: 重命名 pending 目录文件失败: %s (%v)", oldName, renameErr))
				// 如果重命名失败，尝试读取后写入新文件再删除旧文件
				if data, readErr := os.ReadFile(oldPendingPath); readErr == nil {
					if writeErr := os.WriteFile(newPendingPath, data, 0644); writeErr == nil {
						os.Remove(oldPendingPath)
						s.Log(fmt.Sprintf("Rename: 通过读写方式重命名 pending 目录文件成功: %s -> %s", oldName, newName))
					}
				}
			}
		}
	}

	if err := s.client.Remove(s.ctx, oldName); err != nil {
		s.Log(fmt.Sprintf("Rename WARN (Remove source): %s (%v), retrying with timeout", oldName, err))
		// 网络超时可能导致 Remove 失败，重试一次防止新旧文件同时在 S3 中
		retryCtx, retryCancel := context.WithTimeout(context.Background(), s.opt.RequestTimeout)
		defer retryCancel()
		if retryErr := s.client.Remove(retryCtx, oldName); retryErr != nil {
			s.Log(fmt.Sprintf("Rename FAIL (Remove source retry): %s (%v)", oldName, retryErr))
			return fmt.Errorf("failed to delete source %s after copy (retried): %w", oldName, retryErr)
		}
		s.Log(fmt.Sprintf("Rename: Remove retry succeeded: %s", oldName))
	}

	if s.shouldCacheLocally(oldfd) {
		oldLocal := filepath.Join(s.localDir, oldName)
		newLocal := filepath.Join(s.localDir, newName)
		if data, err := os.ReadFile(oldLocal); err == nil {
			os.WriteFile(newLocal, data, 0644)
			os.Remove(oldLocal)
		}
	}

	s.Log(fmt.Sprintf("Rename OK: %s -> %s", oldName, newName))
	return nil
}

// backgroundRetryLoop 后台 goroutine，持续监控并重试 pending 目录中的文件
// 直到成功或收到停止信号
func (s *S3Storage) backgroundRetryLoop() {
	defer s.retryWg.Done()

	flushInterval := s.opt.RetryFlushInterval
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	s.Log(fmt.Sprintf("backgroundRetryLoop: 启动后台重试循环，间隔=%v", flushInterval))

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.retryStopCh:
			s.Log("backgroundRetryLoop: 收到停止信号，正在等待当前重试完成...")
			// 再做一次最终重试，确保所有文件都被尝试
			s.retryPendingFilesOnce()
			s.Log("backgroundRetryLoop: 已停止")
			return
		case <-ticker.C:
			s.retryPendingFilesOnce()
		}
	}
}

// retryPendingFilesOnce 单次尝试重试所有 pending 文件
func (s *S3Storage) retryPendingFilesOnce() {
	if s.pendingDir == "" {
		return
	}
	if s.lockLost.Load() {
		s.Log("retryPendingFilesOnce: lock lost, skipping retry")
		return
	}
	if !s.lockHeld.Load() {
		// 未持有分布式锁时不应该上传文件到 S3，防止与其他进程产生 split-brain
		// 当 Lock() 被调用后会设置 lockHeld = true，后台重试才能安全上传
		s.Log("retryPendingFilesOnce: lock not held, skipping retry")
		return
	}

	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		if !os.IsNotExist(err) {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 读取pending目录失败: %v", err))
		}
		return
	}

	if len(entries) == 0 {
		return
	}

	s.Log(fmt.Sprintf("retryPendingFilesOnce: 发现 %d 个待上传文件", len(entries)))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		localPath := filepath.Join(s.pendingDir, name)

		data, readErr := os.ReadFile(localPath)
		if readErr != nil {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 读取本地文件失败: %s (%v)", name, readErr))
			continue
		}

		fd, ok := parseFileDesc(name)
		if !ok {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 无法解析文件名: %s", name))
			continue
		}

		s.Log(fmt.Sprintf("retryPendingFilesOnce: 尝试上传: %s (%d bytes)", name, len(data)))

		// 带重试的 PutBytes
		uploadErr := s.uploadWithInfiniteRetry(fd, data)
		if uploadErr != nil {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 上传失败（已达最大重试时间）: %s (%v)", name, uploadErr))
			continue
		}

		// 上传成功，删除本地文件
		if rmErr := os.Remove(localPath); rmErr != nil {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 删除本地文件失败: %s (%v)", name, rmErr))
		} else {
			s.Log(fmt.Sprintf("retryPendingFilesOnce: 上传成功并删除本地文件: %s", name))
		}
	}
}

// uploadWithInfiniteRetry 无限重试直到成功
// 如果配置了 RetryMaxDuration，则在超过该时间后返回错误
func (s *S3Storage) uploadWithInfiniteRetry(fd storage.FileDesc, data []byte) error {
	name := fd.String()
	maxDuration := s.opt.RetryMaxDuration
	baseDelay := s.opt.RetryBaseDelay
	retryMaxAttempts := s.opt.RetryMaxAttempts

	startTime := time.Now()
	attempt := 0

	for {
		select {
		case <-s.retryStopCh:
			return errors.New("收到停止信号")
		default:
		}

		if s.lockLost.Load() {
			return ErrLockLost
		}

		// 检查是否超过最大重试时间
		if maxDuration > 0 && time.Since(startTime) > maxDuration {
			return fmt.Errorf("超过最大重试持续时间 %v", maxDuration)
		}

		attempt++
		err := s.client.PutBytes(s.ctx, name, data)
		if err == nil {
			s.Log(fmt.Sprintf("uploadWithInfiniteRetry: 第%d次尝试成功: %s", attempt, name))
			return nil
		}

		s.Log(fmt.Sprintf("uploadWithInfiniteRetry: 第%d次尝试失败: %s (%v)", attempt, name, err))

		// 使用指数退避，但限制最大延迟为 30 秒
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		// 如果配置了最大重试次数，超过后按最大延迟继续重试
		if retryMaxAttempts > 0 && attempt >= retryMaxAttempts {
			delay = 30 * time.Second
		}

		s.Log(fmt.Sprintf("uploadWithInfiniteRetry: 等待 %v 后重试: %s", delay, name))

		select {
		case <-s.retryStopCh:
			return errors.New("收到停止信号")
		case <-time.After(delay):
		}
	}
}

func (s *S3Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	s.cache.Purge()

	// 先取消全局 context，中断正在进行的 S3 请求（如 PutBytes）
	// 再等待后台 goroutine 退出，避免在 PutBytes 超时期间阻塞 120 秒
	s.cancel()
	close(s.retryStopCh)
	s.retryWg.Wait()

	return nil
}

func (s *S3Storage) Log(str string) {
	if s.opt.Logger != nil {
		s.opt.Logger.Printf("[S3Storage] %s\n", str)
		return
	}
	fmt.Printf("[S3Storage] %s\n", str)
}

// startLeaseMonitoring starts a background goroutine that watches the
// S3Lock.LeaseErr() channel. If the lease is lost (network partition,
// lock expiry), it sets the lockLost flag, causing all subsequent
// mutating operations to return ErrLockLost. This provides fencing:
// once the lease is lost, this process stops writing, preventing
// split-brain corruption.
func (s *S3Storage) startLeaseMonitoring() {
	go func() {
		select {
		case err, ok := <-s.s3Lock.LeaseErr():
			if !ok {
				return // channel closed, lock already released
			}
			s.Log(fmt.Sprintf("CRITICAL: lost distributed lock: %v. Disabling writes.", err))
			s.lockLost.Store(true)
		case <-s.ctx.Done():
			return
		}
	}()
}

func (s *S3Storage) syncLocalToS3() error {
	if s.localDir == "" {
		return nil
	}

	entries, err := os.ReadDir(s.localDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read local cache dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if name == "LOCK" || name == "CURRENT" {
			continue
		}

		localPath := filepath.Join(s.localDir, name)

		data, err := os.ReadFile(localPath)
		if err != nil {
			s.Log(fmt.Sprintf("failed to read %s during sync: %v", name, err))
			continue
		}

		s.Log(fmt.Sprintf("syncLocalToS3 uploading: %s (%d bytes)", name, len(data)))
		if err := s.client.PutBytes(s.ctx, name, data); err != nil {
			s.Log(fmt.Sprintf("syncLocalToS3 FAIL: %s (%v)", name, err))
			return fmt.Errorf("failed to sync %s to S3: %w", name, err)
		}
		s.Log(fmt.Sprintf("syncLocalToS3 OK: %s", name))
	}

	return nil
}

// syncPendingToS3 attempts to upload all files from pendingDir to S3.
// This is called during Lock() to recover data that was persisted locally
// when S3 uploads failed in a previous (crashed) session.
//
// Design: best-effort. If some files cannot be uploaded (e.g., S3 is still
// unreachable), LevelDB can still open -- pending files stay in pendingDir
// for the next attempt via backgroundRetryLoop.
func (s *S3Storage) syncPendingToS3() error {
	if s.pendingDir == "" {
		return nil
	}

	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read pending dir: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	s.Log(fmt.Sprintf("syncPendingToS3: found %d pending files to recover", len(entries)))
	var firstErr error

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		localPath := filepath.Join(s.pendingDir, name)

		data, readErr := os.ReadFile(localPath)
		if readErr != nil {
			s.Log(fmt.Sprintf("syncPendingToS3: failed to read %s: %v", name, readErr))
			continue
		}

		fd, ok := parseFileDesc(name)
		if !ok {
			s.Log(fmt.Sprintf("syncPendingToS3: unrecognized filename %s, removing", name))
			os.Remove(localPath)
			continue
		}

		s.Log(fmt.Sprintf("syncPendingToS3: recovering %s (%d bytes)", name, len(data)))
		if err := s.client.PutBytes(s.ctx, name, data); err != nil {
			s.Log(fmt.Sprintf("syncPendingToS3: FAILED to upload %s: %v (keeping in pendingDir)", name, err))
			if firstErr == nil {
				firstErr = err
			}
			continue // best-effort: keep trying other files
		}

		// Upload successful -- remove from pendingDir
		if rmErr := os.Remove(localPath); rmErr != nil {
			s.Log(fmt.Sprintf("syncPendingToS3: uploaded %s but failed to remove local copy: %v", name, rmErr))
		} else {
			s.Log(fmt.Sprintf("syncPendingToS3: successfully recovered %s", name))
		}

		// Update in-memory cache
		cacheKey := s.makeCacheKey(fd)
		if len(data) <= int(s.opt.MaxFileSize) {
			s.cache.Add(cacheKey, &memFile{fd: fd, data: data})
		}
	}

	return firstErr
}

// cleanupOrphaned checks for inconsistent state in S3 and tries to recover.
// This handles the case where a previous session was interrupted (e.g., by Ctrl+C)
// and goleveldb's newManifest() had already:
//   - Created a NEW manifest file (but not synced it to S3)
//   - Updated CURRENT to point to the new manifest
//   - Deleted the OLD manifest from S3
//
// The recovery strategy:
//  1. CURRENT exists but MANIFEST missing → recover from local cache (safe,
//     because SSTables were uploaded before manifest rotation)
//  2. CURRENT missing → clean up (unsafe to recover: local MANIFEST may
//     reference SSTables that were never uploaded to S3)
func (s *S3Storage) cleanupOrphaned() error {
	currentExists, err := s.client.Exists(s.ctx, "CURRENT")
	if err != nil {
		return fmt.Errorf("failed to check CURRENT: %w", err)
	}

	if currentExists {
		return s.verifyCurrentManifest()
	}

	// CURRENT doesn't exist — try to recover from local cache first
	return s.recoverFromLocalCache()
}

// verifyCurrentManifest checks that the MANIFEST referenced by CURRENT exists in S3.
// If not, it recovers from local cache. This is safe because CURRENT was updated
// during manifest rotation AFTER all SSTables were already committed to S3.
// Only the MANIFEST file itself was lost due to interrupted upload.
func (s *S3Storage) verifyCurrentManifest() error {
	data, err := s.client.GetBytes(s.ctx, "CURRENT")
	if err != nil {
		return fmt.Errorf("failed to read CURRENT: %w", err)
	}
	line := strings.TrimSpace(string(data))
	fd, ok := parseFileDesc(line)
	if !ok {
		s.Log(fmt.Sprintf("cleanup: invalid CURRENT content %q, removing all orphaned files", line))
		return s.removeAllOrphaned()
	}

	manifestExists, err := s.client.Exists(s.ctx, fd.String())
	if err != nil {
		return fmt.Errorf("failed to check manifest %s: %w", fd.String(), err)
	}
	if manifestExists {
		return nil // All consistent
	}

	// CURRENT → MANIFEST-N but MANIFEST-N not in S3 (interrupted manifest rotation).
	// Try to recover from local cache. This is safe because all SSTables were
	// uploaded to S3 before the old manifest was rotated out.
	s.Log(fmt.Sprintf("cleanup: CURRENT points to %s but not found in S3, trying local cache recovery", fd.String()))
	if s.localDir != "" {
		localPath := filepath.Join(s.localDir, fd.String())
		if data, err := os.ReadFile(localPath); err == nil && len(data) > 0 {
			s.Log(fmt.Sprintf("cleanup: found %s in local cache (%d bytes), uploading to S3", fd.String(), len(data)))
			if err := s.client.PutBytes(s.ctx, fd.String(), data); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to upload recovered manifest: %v", err))
			} else {
				s.Log(fmt.Sprintf("cleanup: successfully recovered %s from local cache", fd.String()))
				return nil // Recovery successful
			}
		} else {
			s.Log(fmt.Sprintf("cleanup: %s not found in local cache either: %v", fd.String(), err))
		}
	}

	// Check pendingDir for the manifest before giving up
	if s.pendingDir != "" {
		pendingPath := filepath.Join(s.pendingDir, fd.String())
		if pData, pErr := os.ReadFile(pendingPath); pErr == nil && len(pData) > 0 {
			s.Log(fmt.Sprintf("cleanup: found %s in pendingDir (%d bytes), uploading to S3", fd.String(), len(pData)))
			if err := s.client.PutBytes(s.ctx, fd.String(), pData); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to upload recovered manifest from pendingDir: %v", err))
			} else {
				s.Log(fmt.Sprintf("cleanup: successfully recovered %s from pendingDir", fd.String()))
				os.Remove(pendingPath)
				return nil
			}
		}
	}

	// Could not recover from local cache or pendingDir
	s.Log(fmt.Sprintf("cleanup: removing all orphaned files (manifest %s unrecoverable)", fd.String()))
	return s.removeAllOrphaned()
}

// recoverFromLocalCache tries to rebuild CURRENT from MANIFEST files in local cache
// when CURRENT is missing from S3. This handles the case where a previous session
// was interrupted before SetMeta was called, or S3's CURRENT was lost.
//
// If recovery succeeds (MANIFEST + CURRENT uploaded to S3), LevelDB's Open() will
// try to open. If some SSTables are missing, Open() returns ErrCorrupted — the caller
// should then use leveldb.Recover() to scan files and rebuild from scratch.
func (s *S3Storage) recoverFromLocalCache() error {
	allFiles, err := s.client.List(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	if len(allFiles) == 0 && s.localDir == "" {
		return nil
	}

	s.Log(fmt.Sprintf("cleanup: CURRENT not found (S3 has %d files), trying local cache recovery", len(allFiles)))

	// Try to find the latest MANIFEST in local cache
	if s.localDir != "" {
		manifestFd, manifestData, err := s.findLatestLocalManifest()
		if err == nil && manifestFd != nil && len(manifestData) > 0 {
			s.Log(fmt.Sprintf("cleanup: recovering manifest %s from local cache (%d bytes)", manifestFd.String(), len(manifestData)))
			if err := s.client.PutBytes(s.ctx, manifestFd.String(), manifestData); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to upload recovered manifest: %v", err))
				return s.removeAllOrphaned()
			}

			currentContent := manifestFd.String() + "\n"
			if err := s.client.PutBytes(s.ctx, "CURRENT", []byte(currentContent)); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to write recovered CURRENT: %v", err))
				return s.removeAllOrphaned()
			}

			s.Log(fmt.Sprintf("cleanup: successfully recovered database from local cache (CURRENT → %s)", manifestFd.String()))
			return nil
		}
	}

	// Try to find MANIFEST in pendingDir before giving up
	if s.pendingDir != "" {
		manifestFd, manifestData, pErr := s.findLatestPendingManifest()
		if pErr == nil && manifestFd != nil && len(manifestData) > 0 {
			s.Log(fmt.Sprintf("cleanup: recovering manifest %s from pendingDir (%d bytes)", manifestFd.String(), len(manifestData)))
			if err := s.client.PutBytes(s.ctx, manifestFd.String(), manifestData); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to upload recovered manifest from pendingDir: %v", err))
				return s.removeAllOrphaned()
			}

			currentContent := manifestFd.String() + "\n"
			if err := s.client.PutBytes(s.ctx, "CURRENT", []byte(currentContent)); err != nil {
				s.Log(fmt.Sprintf("cleanup: failed to write recovered CURRENT: %v", err))
				return s.removeAllOrphaned()
			}

			s.Log(fmt.Sprintf("cleanup: successfully recovered database from pendingDir (CURRENT &rarr; %s)", manifestFd.String()))
			os.Remove(filepath.Join(s.pendingDir, manifestFd.String()))
			return nil
		}
	}

	// Could not recover — clean up to allow fresh start
	if len(allFiles) > 0 {
		s.Log(fmt.Sprintf("cleanup: removing %d orphaned files (no recoverable manifest)", len(allFiles)))
		return s.removeAllOrphaned()
	}

	return nil
}

// findLatestLocalManifest scans the local cache directory for MANIFEST files
// and returns the one with the highest file number.
func (s *S3Storage) findLatestLocalManifest() (fd *storage.FileDesc, data []byte, err error) {
	entries, err := os.ReadDir(s.localDir)
	if err != nil {
		return nil, nil, err
	}

	var bestNum int64
	var bestName string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var num int
		if n, _ := fmt.Sscanf(name, "MANIFEST-%d", &num); n == 1 {
			if int64(num) > bestNum {
				bestNum = int64(num)
				bestName = name
			}
		}
	}

	if bestName == "" {
		return nil, nil, fmt.Errorf("no MANIFEST file found in local cache")
	}

	fileData, err := os.ReadFile(filepath.Join(s.localDir, bestName))
	if err != nil {
		return nil, nil, err
	}

	return &storage.FileDesc{Type: storage.TypeManifest, Num: bestNum}, fileData, nil
}

// findLatestPendingManifest scans the pendingDir for MANIFEST files and returns
// the one with the highest file number. This is used during crash recovery to
// find manifest files that were persisted locally but not yet uploaded to S3.
func (s *S3Storage) findLatestPendingManifest() (fd *storage.FileDesc, data []byte, err error) {
	if s.pendingDir == "" {
		return nil, nil, os.ErrNotExist
	}
	entries, err := os.ReadDir(s.pendingDir)
	if err != nil {
		return nil, nil, err
	}
	var bestNum int64
	var bestName string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		var num int
		if n, _ := fmt.Sscanf(name, "MANIFEST-%d", &num); n == 1 {
			if int64(num) > bestNum {
				bestNum = int64(num)
				bestName = name
			}
		}
	}
	if bestName == "" {
		return nil, nil, fmt.Errorf("no MANIFEST file found in pending dir")
	}
	fileData, err := os.ReadFile(filepath.Join(s.pendingDir, bestName))
	if err != nil {
		return nil, nil, err
	}
	return &storage.FileDesc{Type: storage.TypeManifest, Num: bestNum}, fileData, nil
}

// removeAllOrphaned removes all LevelDB files from S3 and purges local cache,
// so that LevelDB can start fresh on the next Open().
// This is a last resort when auto-recovery from local cache fails.
//
// IMPORTANT: This function must NEVER be called when the lock is not held.
// It unconditionally deletes all S3 data for this path.
func (s *S3Storage) removeAllOrphaned() error {
	if s.lockLost.Load() {
		s.Log("removeAllOrphaned: lock lost, skipping S3 cleanup to avoid data loss")
		return ErrLockLost
	}

	// Purge in-memory cache
	s.cache.Purge()

	// Clean up local cache directory
	if s.localDir != "" {
		if err := os.RemoveAll(s.localDir); err != nil {
			s.Log(fmt.Sprintf("cleanup: failed to remove local cache: %v", err))
		}
		// Recreate empty directory for future use
		if err := os.MkdirAll(s.localDir, 0755); err != nil {
			s.Log(fmt.Sprintf("cleanup: failed to recreate local cache dir: %v", err))
		}
	}

	// Remove all LevelDB files from S3
	allFiles, err := s.client.List(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to list files during cleanup: %w", err)
	}

	for _, fd := range allFiles {
		name := fd.String()
		s.Log(fmt.Sprintf("cleanup: removing %s", name))
		if err := s.client.Remove(s.ctx, name); err != nil {
			s.Log(fmt.Sprintf("cleanup: failed to remove %s: %v", name, err))
		}
	}

	// Remove CURRENT from S3 if present
	if err := s.client.Remove(s.ctx, "CURRENT"); err != nil {
		// Ignore error if CURRENT doesn't exist
	}

	s.Log("cleanup: all orphaned files removed")
	return nil
}

func (s *S3Storage) makeCacheKey(fd storage.FileDesc) string {
	return fd.String()
}

// atomicWriteFile writes data to a temp file in the same directory, then renames
// it atomically. This prevents partial/corrupted files if the process crashes
// mid-write, ensuring the file is either the old complete version or the new one.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

func (s *S3Storage) shouldCacheLocally(fd storage.FileDesc) bool {
	if s.localDir == "" {
		return false
	}
	return fd.Type == storage.TypeJournal || fd.Type == storage.TypeManifest
}
