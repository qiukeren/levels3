# levels3

S3-backed storage implementation for LevelDB, production-ready with distributed locking, retry logic, and memory safety.

## Features

- **S3 Storage Backend**: Store LevelDB data on S3-compatible object storage
- **Distributed Locking**: S3-based distributed lock with lease mechanism
- **Retry Logic**: Exponential backoff retry for transient failures
- **Memory Safe**: Configurable cache size and file size limits
- **Production Ready**: Comprehensive error handling and timeout control
- **AWS SDK v2**: Using the latest AWS SDK for Go

## Installation

```bash
go get levels3
```

## Usage

```go
package main

import (
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"levels3/internal/s3"
)

func main() {
	opt := s3.OpenOption{
		Bucket:         "my-bucket",
		Path:           "mydb/data",
		Ak:             "your-access-key",
		Sk:             "your-secret-key",
		Region:         "us-east-1",
		LocalCacheDir:  "/tmp/levels3-cache",
		RequestTimeout: 30 * time.Second,
	}

	client, err := s3.NewS3Client(opt)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}

	s3Lock := s3.NewS3Lock(client, opt)

	st, err := s3.NewS3Storage(client, s3Lock, opt)
	if err != nil {
		log.Fatalf("Failed to create S3 storage: %v", err)
	}
	defer st.Close()

	db, err := leveldb.Open(st, nil)
	if err != nil {
		log.Fatalf("Failed to open leveldb: %v", err)
	}
	defer db.Close()

	// Use the database
	db.Put([]byte("key"), []byte("value"), nil)
	value, _ := db.Get([]byte("key"), nil)
}
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| Bucket | S3 bucket name | Required |
| Path | Prefix path in bucket | Required |
| Ak | AWS Access Key | Required |
| Sk | AWS Secret Key | Required |
| Region | AWS region | Required |
| LocalCacheDir | Local cache directory | Required |
| Endpoint | Custom S3-compatible endpoint (e.g. Tencent Cloud COS, MinIO) | AWS default endpoint |
| MaxCacheSize | Max cached files | 500 |
| MaxFileSize | Max file size (bytes) | 64MB |
| RetryMaxAttempts | Max retry attempts | 3 |
| RetryBaseDelay | Base retry delay | 100ms |
| RequestTimeout | Request timeout | 30s |
| LeaseDuration | Lock lease duration | 30s |
| LeaseRenewal | Lease renewal interval | 10s |
| Logger | Custom logger for debug traces | stdout |

## Architecture

```
LevelDB → S3Storage → S3Client → S3
                ↕         ↕
              S3Lock    Retry
```

### Components

- **S3Client**: Wraps AWS SDK v2 S3 operations with context timeout support
- **S3Lock**: Distributed lock with lease renewal and stale lock detection
- **S3Storage**: Implements leveldb/storage.Storage interface
- **Retry**: Exponential backoff retry for transient failures

### Key Design Decisions

1. **Rename Operation**: Uses S3 CopyObject + DeleteObject to simulate atomic rename for LevelDB compaction
2. **Distributed Lock**: Uses S3 PutObject with lease mechanism to handle process crashes
3. **Memory Safety**: LRU cache with configurable limits prevents OOM
4. **Error Handling**: S3 operation errors are wrapped with context for better diagnostics; best-effort cleanup operations intentionally ignore errors

## License

BSD-style
