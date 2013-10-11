package main

/*
  This is a little program that writes 100m keys to a level db to check the size.
  Then deletes a bunch of keys to see how that is reflected.
*/

import (
	"fmt"
	"github.com/jmhodges/levigo"
	"math/rand"
	"os"
	"time"
)

func main() {
	seriesNames := []string{"events",
		"some_host_2342.stats.cpu.idle", "actions", "host_another_2.stats.cpu.idle",
		"some_long_name.with-other_stuff.in.it.and_whatnot"}
	pointsWritten := 0
	dbDir := "/tmp/chronos_db_test"
	os.RemoveAll(dbDir)
	pointsToWritePerSeries := 3000000
	pointsToDeletePerSeries := pointsToWritePerSeries / 2

	opts := levigo.NewOptions()
	hundregMegabytes := 100 * 1048576
	opts.SetCache(levigo.NewLRUCache(hundregMegabytes))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(262144)
	os.MkdirAll(dbDir, 0744)
	db, err := levigo.Open(dbDir, opts)
	if err != nil {
		panic(err)
	}
	writeOptions := levigo.NewWriteOptions()
	defer writeOptions.Close()

	// this initializes the ends of the keyspace so seeks don't mess with us.
	db.Put(writeOptions, []byte("a"), []byte(""))
	db.Put(writeOptions, []byte("z"), []byte(""))

	start := time.Now()
	for _, seriesName := range seriesNames {
		fmt.Println("Writing Series: ", seriesName)
		for i := 0; i < pointsToWritePerSeries; i++ {
			key := fmt.Sprintf("c~%s~1381456156%012d~%d", seriesName, i, rand.Int())
			db.Put(writeOptions, []byte(key), []byte(fmt.Sprintf("%f", rand.Float64())))
			pointsWritten += 1
			if pointsWritten%500000 == 0 {
				fmt.Println("COUNT: ", pointsWritten)
			}
		}
	}
	diff := time.Now().Sub(start)
	fmt.Printf("DONE. %d points written in %s\n", pointsWritten, diff.String())
	fmt.Printf("hit enter after checking size of dir and ready to continue...")
	var s string
	fmt.Scanf("%s", &s)
	deleteCount := 0
	start = time.Now()
	for _, seriesName := range seriesNames {
		fmt.Println("Deleting: ", seriesName)
		count := 0
		key := fmt.Sprintf("c~%s~", seriesName)
		ro := levigo.NewReadOptions()
		it := db.NewIterator(ro)
		it.Seek([]byte(key))
		var lastKey []byte
		for it = it; it.Valid() && count < pointsToDeletePerSeries; it.Next() {
			lastKey = it.Key()
			db.Delete(writeOptions, it.Key())
			count += 1
			deleteCount += 1
		}
		it.Close()
		ro.Close()
		rangeToCompact := &levigo.Range{[]byte(key), lastKey}
		db.CompactRange(*rangeToCompact)
	}
	diff = time.Now().Sub(start)
	fmt.Printf("DONE. %d points deleted in %s\n", deleteCount, diff.String())
}
