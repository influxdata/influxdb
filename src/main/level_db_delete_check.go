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
	"os/exec"
	"time"
)

func main() {
	seriesNames := []string{"events",
		"some_host_2342.stats.cpu.idle", "actions", "host_another_2.stats.cpu.idle",
		"some_long_name.with-other_stuff.in.it.and_whatnot"}
	pointsWritten := 0

	dbDir := "/tmp/chronos_db_test"
	os.RemoveAll(dbDir)
	pointsToWritePerSeries := 2000000
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
		out, _ := exec.Command("du", "-h", dbDir).Output()
		fmt.Println("SIZE: ", string(out))
	}
	diff := time.Now().Sub(start)
	fmt.Printf("DONE. %d points written in %s\n", pointsWritten, diff.String())
	out, _ := exec.Command("du", "-h", dbDir).Output()
	fmt.Println("SIZE: ", string(out))
	deleteCount := 0
	start = time.Now()
	ranges := make([]*levigo.Range, 0)
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
		ranges = append(ranges, rangeToCompact)
		out, _ := exec.Command("du", "-h", dbDir).Output()
		fmt.Println("SIZE: ", string(out))
	}
	fmt.Printf("DONE. %d points deleted in %s\n", deleteCount, diff.String())

	fmt.Println("Starting compaction in background...")
	compactionDone := make(chan int)
	for _, r := range ranges {
		go func(r *levigo.Range) {
			db.CompactRange(*r)
			compactionDone <- 1
		}(r)
	}

	start = time.Now()
	fmt.Println("Writing new points...")
	pointsWritten = 0
	for _, seriesName := range seriesNames {
		fmt.Println("Writing Series: ", seriesName)
		for i := pointsToWritePerSeries; i < pointsToWritePerSeries*2; i++ {
			key := fmt.Sprintf("c~%s~1381456156%012d~%d", seriesName, i, rand.Int())
			db.Put(writeOptions, []byte(key), []byte(fmt.Sprintf("%f", rand.Float64())))
			pointsWritten += 1
			if pointsWritten%500000 == 0 {
				fmt.Println("COUNT: ", pointsWritten)
			}
		}
		out, _ := exec.Command("du", "-h", dbDir).Output()
		fmt.Println("SIZE: ", string(out))
	}

	fmt.Println("waiting for compaction to finish...")
	for i := 0; i < len(ranges); i++ {
		<-compactionDone
	}
	diff = time.Now().Sub(start)
	out, _ = exec.Command("du", "-h", dbDir).Output()
	fmt.Printf("DONE. %d points written during compaction in %s\n", deleteCount, diff.String())
	fmt.Println("SIZE: ", string(out))
}
