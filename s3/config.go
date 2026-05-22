package s3

import (
	"errors"
	"log"
	"path/filepath"
	"time"
)

const (
	DefaultMaxCacheSize   = 500
	DefaultMaxFileSize    = 64 * 1024 * 1024 // 64MB
	DefaultRetryAttempts  = 5                // 增加重试次数
	DefaultRetryBaseDelay = 500 * time.Millisecond // 增加基础延迟，更适合网络抖动
	DefaultRequestTimeout = 120 * time.Second // 增加单次请求超时
	DefaultLeaseDuration  = 60 * time.Second // 增加锁的 Lease 时间
	DefaultLeaseRenewal   = 20 * time.Second // 增加续约间隔
	// RetryMaxDuration 无限重试的最大持续时间，0 表示无限重试直到成功
	DefaultRetryMaxDuration = 0
	// RetryFlushInterval 后台重试的检查间隔
	DefaultRetryFlushInterval = 5 * time.Second
)

type OpenOption struct {
	Bucket           string
	Path             string
	Ak               string
	Sk               string
	Region           string
	Endpoint         string
	LocalCacheDir    string
	MaxCacheSize     int
	MaxFileSize      int64
	RetryMaxAttempts int
	RetryBaseDelay   time.Duration
	RequestTimeout   time.Duration
	LeaseDuration    time.Duration
	LeaseRenewal     time.Duration
	Logger           *log.Logger
	// EnableLocalPersistence 上传失败时是否持久化到本地（确保数据不丢失）
	EnableLocalPersistence bool
	// LocalPersistenceDir 上传失败时的本地备份目录（为空则使用 LocalCacheDir）
	LocalPersistenceDir string
	// RetryMaxDuration 无限重试的最大持续时间，0 表示无限重试直到成功
	RetryMaxDuration time.Duration
	// RetryFlushInterval 后台重试的检查间隔
	RetryFlushInterval time.Duration
}

func (opt *OpenOption) ApplyDefaults() {
	if opt.MaxCacheSize <= 0 {
		opt.MaxCacheSize = DefaultMaxCacheSize
	}
	if opt.MaxFileSize <= 0 {
		opt.MaxFileSize = DefaultMaxFileSize
	}
	if opt.RetryMaxAttempts <= 0 {
		opt.RetryMaxAttempts = DefaultRetryAttempts
	}
	if opt.RetryBaseDelay <= 0 {
		opt.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if opt.RequestTimeout <= 0 {
		opt.RequestTimeout = DefaultRequestTimeout
	}
	if opt.LeaseDuration <= 0 {
		opt.LeaseDuration = DefaultLeaseDuration
	}
	if opt.LeaseRenewal <= 0 {
		opt.LeaseRenewal = DefaultLeaseRenewal
	}
	// 默认启用本地持久化以确保数据不丢失
	if !opt.EnableLocalPersistence {
		opt.EnableLocalPersistence = true
	}
	// 默认无限重试直到成功
	if opt.RetryMaxDuration <= 0 {
		opt.RetryMaxDuration = DefaultRetryMaxDuration
	}
	if opt.RetryFlushInterval <= 0 {
		opt.RetryFlushInterval = DefaultRetryFlushInterval
	}
}

func (opt *OpenOption) Validate() error {
	if opt.Bucket == "" {
		return errors.New("Bucket is required")
	}
	if opt.Path == "" {
		return errors.New("Path is required")
	}
	if opt.Ak == "" {
		return errors.New("Ak is required")
	}
	if opt.Sk == "" {
		return errors.New("Sk is required")
	}
	if opt.Region == "" {
		return errors.New("Region is required")
	}
	if opt.LocalCacheDir == "" {
		return errors.New("LocalCacheDir is required")
	}
	return nil
}

func (opt *OpenOption) LocalPath() string {
	return filepath.Join(opt.LocalCacheDir, opt.Path)
}

// LocalPersistencePath 返回上传失败时的本地备份路径
func (opt *OpenOption) LocalPersistencePath() string {
	if opt.LocalPersistenceDir != "" {
		return filepath.Join(opt.LocalPersistenceDir, opt.Path, "pending_uploads")
	}
	return filepath.Join(opt.LocalCacheDir, opt.Path, "pending_uploads")
}
