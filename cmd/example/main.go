package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/qiukeren/levels3/internal/s3"

	"github.com/syndtr/goleveldb/leveldb"
	lvldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	opt1 "github.com/syndtr/goleveldb/leveldb/opt"
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

	leveldbOpts := &opt1.Options{
		CompactionTableSize:           16 * 1024 * 1024, // 16MB
		CompactionTableSizeMultiplier: 2.0,
		CompactionTotalSize:           80 * 1024 * 1024, // 80MB
		// 其他参数...
	}

	db, err := leveldb.Open(st, leveldbOpts)
	if err != nil {
		if lvldberrors.IsCorrupted(err) {
			log.Printf("DB corrupted (likely SSTables missing after recovery), trying Recover()...")
			db, err = leveldb.Recover(st, leveldbOpts)
		}
		if err != nil {
			log.Fatalf("Failed to open leveldb: %v", err)
		}
	}
	defer db.Close()

	const batchSize = 100000
	const totalKeys = 100000000

	for j := 1; j <= 3; j++ {
		batch := new(leveldb.Batch)
		for i := 1; i <= totalKeys; i++ {
			key := fmt.Sprintf("key_%d_%d", j, i)
			batch.Put([]byte(key), []byte("hello3"))
			if i%batchSize == 0 || i == totalKeys {
				if err := db.Write(batch, nil); err != nil {
					log.Fatalf("Failed to write batch %d: %v", j, err)
				}
				batch.Reset()
				if i%100000 == 0 {
					log.Printf("Wrote batch %d", i)
				}
			}
		}

		//iter := db.NewIterator(&util.Range{}, nil)
		//count := 0
		//for iter.Next() {
		//	count++
		//}
		//iter.Release()
		//
		//fmt.Printf("Batch %d: wrote %d keys, total keys: %d\n", j, totalKeys, count)
	}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
