// Package s3 provides an S3-backed storage backend for LevelDB.
//
// Architecture
//
// The package is organized into seven files with clear separation of concerns:
//
//	interface.go  - S3ClientAPI defines the contract for all S3 operations
//	client.go     - S3Client implements S3ClientAPI using AWS SDK v2
//	lock.go       - S3Lock implements distributed locking via S3
//	storage.go    - S3Storage implements leveldb/storage.Storage
//	retry.go      - WithRetry provides context-aware exponential backoff with jitter
//	errors.go     - Package-level sentinel errors and RetryableError type
//	config.go     - OpenOption configuration model and defaults
//
// Quick Start
//
//	opt := s3.OpenOption{
//	    Bucket:        "my-bucket",
//	    Path:          "mydb/data",
//	    Ak:            "access-key",
//	    Sk:            "secret-key",
//	    Region:        "us-east-1",
//	    LocalCacheDir: "/tmp/levels3-cache",
//	}
//
//	client, _ := s3.NewS3Client(opt)
//	lock := s3.NewS3Lock(client, opt)
//	st, _ := s3.NewS3Storage(client, lock, opt)
//	db, _ := leveldb.Open(st, nil)
//	defer db.Close()
//
// Design Principles
//
//   - Interface-based: S3ClientAPI decouples storage/lock from the AWS SDK.
//   - Context-first: All S3 operations accept context.Context for cancellation.
//   - Retry-safe: Exponential backoff with jitter prevents thundering herd.
//   - Memory-aware: Configurable MaxFileSize and LRU cache prevent OOM.
//   - Leased locking: S3-based distributed lock with automatic lease renewal.
package s3
