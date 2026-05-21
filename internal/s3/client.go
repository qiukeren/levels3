package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// S3Client implements S3ClientAPI using AWS SDK v2.
// It wraps each S3 operation with retry logic, timeout control, and file size checks.
type S3Client struct {
	client *s3.Client
	opt    OpenOption
	logger *log.Logger
}

// NewS3Client creates a new S3Client, validates bucket accessibility.
// If opt.Endpoint is empty, the AWS SDK default endpoint for the region is used.
func NewS3Client(opt OpenOption) (*S3Client, error) {
	opt.ApplyDefaults()
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	creds := credentials.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID:     opt.Ak,
			SecretAccessKey: opt.Sk,
		},
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(opt.Region),
		config.WithCredentialsProvider(creds),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				// When no custom endpoint is specified, return empty to use AWS default.
				if opt.Endpoint == "" {
					return aws.Endpoint{}, &aws.EndpointNotFoundError{}
				}
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           opt.Endpoint,
					SigningRegion: opt.Region,
				}, nil
			}),
		),
		config.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// For Tencent Cloud COS and other compatible services usually work better with virtual-hosted style
		o.UsePathStyle = false
		// 腾讯云 COS 不支持 AWS checksum 校验，关闭相关日志避免 "Response has no supported checksum" 警告
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	ctx, cancel := context.WithTimeout(context.TODO(), opt.RequestTimeout)
	defer cancel()

	_, err = s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(opt.Bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to access bucket %s: %w", opt.Bucket, err)
	}

	return &S3Client{
		client: s3Client,
		opt:    opt,
		logger: opt.Logger,
	}, nil
}

// operationContext wraps a context with RequestTimeout to prevent infinite hangs.
func (c *S3Client) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.opt.RequestTimeout)
}

// PutBytes implements S3ClientAPI.PutBytes.
func (c *S3Client) PutBytes(ctx context.Context, key string, data []byte) error {
	if !isMetadataKey(key) && int64(len(data)) > c.opt.MaxFileSize {
		return ErrFileTooLarge
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	start := time.Now()
	c.debug("PutBytes start: %s (%d bytes)", key, len(data))
	err := c.retry(ctx, func() error {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.opt.Bucket),
			Key:    aws.String(c.pathKey(key)),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			return &RetryableError{Err: fmt.Errorf("failed to put object %s: %w", key, err)}
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		c.debug("PutBytes FAIL: %s (%v, elapsed=%v)", key, err, elapsed)
	} else {
		c.debug("PutBytes OK: %s (%d bytes, elapsed=%v)", key, len(data), elapsed)
	}
	return err
}

// PutBytesIfNotExists implements S3ClientAPI.PutBytesIfNotExists.
//
// Note: This has a TOCTOU race condition (check-then-act between Exists and PutObject).
// For LevelDB workflows this is acceptable because the storage layer serializes writes
// via the distributed lock. For strict conditional writes, use S3 object lock or versioning.
func (c *S3Client) PutBytesIfNotExists(ctx context.Context, key string, data []byte) (bool, error) {
	if !isMetadataKey(key) && int64(len(data)) > c.opt.MaxFileSize {
		return false, ErrFileTooLarge
	}

	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	start := time.Now()
	c.debug("PutBytesIfNotExists start: %s (%d bytes)", key, len(data))

	exists, err := c.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check existence before put: %w", err)
	}
	if exists {
		c.debug("PutBytesIfNotExists skip (exists): %s (elapsed=%v)", key, time.Since(start))
		return false, nil
	}

	err = c.retry(ctx, func() error {
		_, putErr := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.opt.Bucket),
			Key:    aws.String(c.pathKey(key)),
			Body:   bytes.NewReader(data),
		})
		if putErr != nil {
			return &RetryableError{Err: fmt.Errorf("failed to put object %s: %w", key, putErr)}
		}
		return nil
	})
	if err != nil {
		c.debug("PutBytesIfNotExists FAIL: %s (%v, elapsed=%v)", key, err, time.Since(start))
		return false, err
	}
	c.debug("PutBytesIfNotExists OK: %s (%d bytes, elapsed=%v)", key, len(data), time.Since(start))
	return true, nil
}

// GetBytes implements S3ClientAPI.GetBytes.
func (c *S3Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	var data []byte
	start := time.Now()
	c.debug("GetBytes start: %s", key)
	err := c.retry(ctx, func() error {
		resp, getErr := c.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(c.opt.Bucket),
			Key:    aws.String(c.pathKey(key)),
		})
		if getErr != nil {
			if isNotFoundError(getErr) {
				return getErr
			}
			return &RetryableError{Err: getErr}
		}
		defer resp.Body.Close()

		var readErr error
		data, readErr = io.ReadAll(resp.Body)
		if readErr != nil {
			return readErr
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		if isNotFoundError(err) {
			c.debug("GetBytes NOT_FOUND: %s (elapsed=%v)", key, elapsed)
			return nil, os.ErrNotExist
		}
		c.debug("GetBytes FAIL: %s (%v, elapsed=%v)", key, err, elapsed)
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}
	c.debug("GetBytes OK: %s (%d bytes, elapsed=%v)", key, len(data), elapsed)
	return data, nil
}

// Remove implements S3ClientAPI.Remove.
func (c *S3Client) Remove(ctx context.Context, key string) error {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	start := time.Now()
	c.debug("Remove start: %s", key)
	err := c.retry(ctx, func() error {
		_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(c.opt.Bucket),
			Key:    aws.String(c.pathKey(key)),
		})
		if err != nil {
			return &RetryableError{Err: fmt.Errorf("failed to delete object %s: %w", key, err)}
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		c.debug("Remove FAIL: %s (%v, elapsed=%v)", key, err, elapsed)
	} else {
		c.debug("Remove OK: %s (elapsed=%v)", key, elapsed)
	}
	return err
}

// Exists implements S3ClientAPI.Exists.
func (c *S3Client) Exists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	start := time.Now()
	c.debug("Exists start: %s", key)
	err := c.retry(ctx, func() error {
		_, err := c.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(c.opt.Bucket),
			Key:    aws.String(c.pathKey(key)),
		})
		if err != nil {
			if isNotFoundError(err) {
				return err
			}
			return &RetryableError{Err: err}
		}
		return nil
	})
	elapsed := time.Since(start)

	if err == nil {
		c.debug("Exists OK (true): %s (elapsed=%v)", key, elapsed)
		return true, nil
	}

	if isNotFoundError(err) {
		c.debug("Exists OK (false): %s (elapsed=%v)", key, elapsed)
		return false, nil
	}

	c.debug("Exists FAIL: %s (%v, elapsed=%v)", key, err, elapsed)
	return false, fmt.Errorf("failed to check object %s: %w", key, err)
}

// List implements S3ClientAPI.List.
func (c *S3Client) List(ctx context.Context) ([]storage.FileDesc, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	var files []storage.FileDesc
	start := time.Now()
	c.debug("List start")

	err := c.retry(ctx, func() error {
		files = files[:0]
		pageCount := 0
		paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
			Bucket: aws.String(c.opt.Bucket),
			Prefix: aws.String(c.pathPrefix()),
		})

		for paginator.HasMorePages() {
			pageStart := time.Now()
			page, err := paginator.NextPage(ctx)
			if err != nil {
				c.debug("List page %d FAIL (%v, page_elapsed=%v)", pageCount+1, err, time.Since(pageStart))
				return &RetryableError{Err: err}
			}
			pageCount++
			c.debug("List page %d OK (%d objects, page_elapsed=%v)", pageCount, len(page.Contents), time.Since(pageStart))

			for _, obj := range page.Contents {
				_, relName := parseS3Key(*obj.Key)
				fd, ok := parseFileDesc(relName)
				if ok {
					files = append(files, fd)
				}
			}
		}
		c.debug("List pages done: %d pages, %d total files", pageCount, len(files))
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		c.debug("List FAIL (%v, elapsed=%v)", err, elapsed)
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	c.debug("List OK: %d files (elapsed=%v)", len(files), elapsed)
	return files, nil
}

// Copy implements S3ClientAPI.Copy.
func (c *S3Client) Copy(ctx context.Context, srcKey, dstKey string) error {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()

	start := time.Now()
	c.debug("Copy start: %s -> %s", srcKey, dstKey)
	err := c.retry(ctx, func() error {
		_, err := c.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(c.opt.Bucket),
			CopySource: aws.String(c.opt.Bucket + "/" + c.pathKey(srcKey)),
			Key:        aws.String(c.pathKey(dstKey)),
		})
		if err != nil {
			return &RetryableError{Err: fmt.Errorf("failed to copy %s to %s: %w", srcKey, dstKey, err)}
		}
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		c.debug("Copy FAIL: %s -> %s (%v, elapsed=%v)", srcKey, dstKey, err, elapsed)
	} else {
		c.debug("Copy OK: %s -> %s (elapsed=%v)", srcKey, dstKey, elapsed)
	}
	return err
}

// retry runs fn with WithRetry using the configured max attempts and base delay.
func (c *S3Client) retry(ctx context.Context, fn func() error) error {
	return WithRetry(ctx, c.opt.RetryMaxAttempts, c.opt.RetryBaseDelay, fn)
}

// debug logs a formatted trace message if a logger is configured.
func (c *S3Client) debug(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Printf("[S3Client] "+format, args...)
	}
}

func (c *S3Client) pathKey(key string) string {
	return c.opt.Path + "/" + key
}

func (c *S3Client) pathPrefix() string {
	return c.opt.Path + "/"
}

func parseS3Key(fullKey string) (dir, name string) {
	if idx := strings.LastIndex(fullKey, "/"); idx >= 0 {
		return fullKey[:idx], fullKey[idx+1:]
	}
	return "", fullKey
}

func isNotFoundError(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}

func isMetadataKey(key string) bool {
	return key == "CURRENT" || key == "LOCK" || strings.HasPrefix(key, "MANIFEST-")
}
