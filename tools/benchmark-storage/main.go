package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/influxdb/influxdb/datastore/storage"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	points := flag.Int("points", 200000000, "Number of points")
	batchSize := flag.Int("batch", 1000, "Batch size")
	series := flag.Int("series", 1, "Number of series")
	path := flag.String("path", "/tmp", "Path to DB files")
	flag.Parse()

	os.RemoveAll("/tmp/test-leveldb")
	os.RemoveAll("/tmp/test-lmdb")
	os.RemoveAll("/tmp/test-rocksdb")
	os.RemoveAll("/tmp/test-hyperleveldb")

	benchmark("lmdb", Config{*points, *batchSize, *series, 0, 0, time.Now(), *path})
	benchmark("leveldb", Config{*points, *batchSize, *series, 0, 0, time.Now(), *path})
	benchmark("rocksdb", Config{*points, *batchSize, *series, 0, 0, time.Now(), *path})
	benchmark("hyperleveldb", Config{*points, *batchSize, *series, 0, 0, time.Now(), *path})
}

func benchmark(name string, c Config) {
	init, err := storage.GetInitializer(name)
	if err != nil {
		panic(err)
	}

	conf := init.NewConfig()
	db, err := init.Initialize(fmt.Sprintf("%s/test-%s", c.path, name), conf)

	defer db.Close()

	benchmarkDbCommon(db, c)
}

func getSize(path string) string {
	cmd := exec.Command("du", "-sh", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}
	return strings.Fields(string(out))[0]
}

func benchmarkDbCommon(db storage.Engine, c Config) {
	fmt.Printf("################ Benchmarking: %s\n", db.Name())
	start := time.Now()
	count := 0
	for p := c.points; p > 0; {
		c := writeBatch(db, &c)
		count += c
		p -= c
	}
	d := time.Now().Sub(start)

	fmt.Printf("Writing %d points in batches of %d points took %s (%f microsecond per point)\n",
		count,
		c.batch,
		d,
		float64(d.Nanoseconds())/1000.0/float64(count),
	)

	timeQuerying(db, c.series)
	fmt.Printf("Size: %s\n", getSize(db.Path()))
	queryAndDelete(db, c.points, c.series)
	timeQuerying(db, c.series)
	fmt.Printf("Size: %s\n", getSize(db.Path()))

	start = time.Now()
	count = 0
	for p := c.points / 2; p > 0; {
		c := writeBatch(db, &c)
		count += c
		p -= c
	}
	d = time.Now().Sub(start)
	fmt.Printf("Writing %d points in batches of %d points took %s (%f microsecond per point)\n",
		count,
		c.batch,
		d,
		float64(d.Nanoseconds())/1000.0/float64(count),
	)
	fmt.Printf("Size: %s\n", getSize(db.Path()))
}

func timeQuerying(db storage.Engine, series int) {
	s := time.Now()
	count := 0
	for series -= 1; series >= 0; series-- {
		query(db, int64(series), func(itr storage.Iterator) {
			count++
		})
	}
	d := time.Now().Sub(s)
	fmt.Printf("Querying %d points took %s (%f microseconds per point)\n",
		count, d, float64(d.Nanoseconds())/1000.0/float64(count))

}

func queryAndDelete(db storage.Engine, points, series int) {
	// query the database
	startCount := points / series / 4
	endCount := points * 3 / series / 4

	total := 0
	var d time.Duration

	for series -= 1; series >= 0; series-- {
		count := 0
		var delStart []byte
		var delEnd []byte
		query(db, int64(series), func(itr storage.Iterator) {
			count++
			if count == startCount {
				delStart = itr.Key()
			}
			if count == endCount-1 {
				delEnd = itr.Key()
				total += endCount - startCount
			}
		})

		start := time.Now()
		err := db.Del(delStart, delEnd)
		if err != nil {
			panic(err)
		}
		d += time.Now().Sub(start)
	}
	fmt.Printf("Took %s to delete %d points\n", d, total)
	start := time.Now()
	db.Compact()
	fmt.Printf("Took %s to compact\n", time.Now().Sub(start))
}

func query(db storage.Engine, s int64, yield func(storage.Iterator)) {
	sb := bytes.NewBuffer(nil)
	binary.Write(sb, binary.BigEndian, s)
	binary.Write(sb, binary.BigEndian, int64(0))
	binary.Write(sb, binary.BigEndian, int64(0))
	eb := bytes.NewBuffer(nil)
	binary.Write(eb, binary.BigEndian, s)
	binary.Write(eb, binary.BigEndian, int64(-1))
	binary.Write(eb, binary.BigEndian, int64(-1))

	itr := db.Iterator()
	defer itr.Close()
	count := 0
	for itr.Seek(sb.Bytes()); itr.Valid(); itr.Next() {
		key := itr.Key()
		if bytes.Compare(key, eb.Bytes()) > 0 {
			break
		}

		count++
		yield(itr)
	}
}

func writeBatch(db storage.Engine, c *Config) int {
	ws := c.MakeBatch()
	if err := db.BatchPut(ws); err != nil {
		panic(err)
	}
	return len(ws)
}
