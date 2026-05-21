package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"levels3/internal/s3"

	"github.com/syndtr/goleveldb/leveldb"
	lvldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	opt := s3.OpenOption{
		Bucket:         envOrDefault("S3_BUCKET", "data-1251933044"),
		Path:           envOrDefault("S3_PATH", "new-test-db"),
		Ak:             os.Getenv("S3_ACCESS_KEY"),
		Sk:             os.Getenv("S3_SECRET_KEY"),
		Endpoint:       envOrDefault("S3_ENDPOINT", "https://cos.ap-shanghai.myqcloud.com"),
		Region:         envOrDefault("S3_REGION", "ap-shanghai"),
		LocalCacheDir:  envOrDefault("S3_CACHE_DIR", "/tmp/levels3-cache"),
		MaxCacheSize:   500,
		RequestTimeout: 60 * time.Second,
	}

	if opt.Ak == "" || opt.Sk == "" {
		log.Fatal("S3_ACCESS_KEY and S3_SECRET_KEY environment variables must be set")
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
		if lvldberrors.IsCorrupted(err) {
			log.Printf("DB corrupted (likely SSTables missing after recovery), trying Recover()...")
			db, err = leveldb.Recover(st, nil)
		}
		if err != nil {
			log.Fatalf("Failed to open leveldb: %v", err)
		}
	}
	defer db.Close()

	log.Println("Reading all key-value pairs from S3-backed LevelDB...")

	iter := db.NewIterator(&util.Range{}, nil)
	defer iter.Release()

	count := 0
	for iter.Next() {
		key := string(iter.Key())
		value := string(iter.Value())
		fmt.Printf("%s -> %s\n", key, value)
		count++
	}
	if err := iter.Error(); err != nil {
		log.Fatalf("Iterator error: %v", err)
	}

	log.Printf("Done! Printed %d key-value pairs.", count)
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
