package s3

import (
	"context"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

// S3ClientAPI defines the interface for S3 operations.
// This abstraction enables:
//   - Dependency injection for testing (mock implementations)
//   - Decoupling S3Lock and S3Storage from the concrete S3Client
//   - Swapping the underlying AWS SDK implementation without affecting callers
type S3ClientAPI interface {
	// PutBytes uploads data to the specified key.
	// Returns error if the file exceeds MaxFileSize (unless it's a metadata key).
	PutBytes(ctx context.Context, key string, data []byte) error

	// PutBytesIfNotExists uploads data only if the key does not already exist.
	// Returns (true, nil) on successful creation, (false, nil) if key already exists.
	// Returns error if the file exceeds MaxFileSize.
	PutBytesIfNotExists(ctx context.Context, key string, data []byte) (bool, error)

	// GetBytes downloads data for the specified key.
	// Returns os.ErrNotExist if the key does not exist.
	GetBytes(ctx context.Context, key string) ([]byte, error)

	// Remove deletes the specified key.
	Remove(ctx context.Context, key string) error

	// Exists checks whether the specified key exists in S3.
	Exists(ctx context.Context, key string) (bool, error)

	// List returns all LevelDB file descriptors stored in the S3 path.
	List(ctx context.Context) ([]storage.FileDesc, error)

	// Copy copies an object from srcKey to dstKey within the same bucket.
	Copy(ctx context.Context, srcKey, dstKey string) error
}
