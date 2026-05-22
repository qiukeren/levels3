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
	DefaultRetryAttempts  = 3
	DefaultRetryBaseDelay = 100 * time.Millisecond
	DefaultRequestTimeout = 30 * time.Second
	DefaultLeaseDuration  = 30 * time.Second
	DefaultLeaseRenewal   = 10 * time.Second
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
