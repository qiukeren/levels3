package main

import (
	"fmt"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	startTime := time.Now()
	dbPath := "./test-leveldb"

	log.Printf("Opening LevelDB at %s...", dbPath)
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		log.Fatalf("Failed to open LevelDB: %v", err)
	}
	defer db.Close()

	totalKeys := 1000000
	log.Printf("Starting to write %d keys...", totalKeys)

	batchStart := time.Now()
	for i := 1; i <= totalKeys; i++ {
		key := fmt.Sprintf("key_%08d", i)
		value := fmt.Sprintf("value_%08d_hello_world", i)
		if err := db.Put([]byte(key), []byte(value), nil); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
		}
		if i%100000 == 0 {
			elapsed := time.Since(batchStart)
			log.Printf("Written %d/%d keys (%.2f%%), elapsed: %v", 
				i, totalKeys, float64(i)/float64(totalKeys)*100, elapsed)
			batchStart = time.Now()
		}
	}

	log.Printf("Verifying written keys...")
	iter := db.NewIterator(nil, nil)
	count := 0
	for iter.Next() {
		count++
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		log.Printf("Iterator error: %v", err)
	}

	totalElapsed := time.Since(startTime)
	log.Printf("Done! Total keys: %d, total time: %v, avg: %v/key", 
		count, totalElapsed, totalElapsed/time.Duration(totalKeys))
}
