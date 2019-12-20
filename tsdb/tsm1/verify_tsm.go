package tsm1

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type VerifyTSM struct {
	Stdout   io.Writer
	Paths    []string
	OrgID    influxdb.ID
	BucketID influxdb.ID
}

func (v *VerifyTSM) Run() error {
	for _, path := range v.Paths {
		if err := v.processFile(path); err != nil {
			fmt.Fprintf(v.Stdout, "Error processing file %q: %v", path, err)
		}
	}
	return nil
}

func (v *VerifyTSM) processFile(path string) error {
	fmt.Println("processing file: " + path)

	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return fmt.Errorf("OpenFile: %v", err)
	}

	reader, err := NewTSMReader(file)
	if err != nil {
		return fmt.Errorf("failed to create TSM reader for %q: %v", path, err)
	}
	defer reader.Close()

	var start []byte
	if v.OrgID.Valid() {
		if v.BucketID.Valid() {
			v := tsdb.EncodeName(v.OrgID, v.BucketID)
			start = v[:]
		} else {
			v := tsdb.EncodeOrgName(v.OrgID)
			start = v[:]
		}
	}

	var ts cursors.TimestampArray
	count := 0
	totalErrors := 0
	iter := reader.Iterator(start)
	for iter.Next() {
		key := iter.Key()
		if len(start) > 0 && (len(key) < len(start) || !bytes.Equal(key[:len(start)], start)) {
			break
		}

		entries := iter.Entries()
		for i := range entries {
			entry := &entries[i]

			checksum, buf, err := reader.ReadBytes(entry, nil)
			if err != nil {
				fmt.Fprintf(v.Stdout, "could not read block %d due to error: %q\n", count, err)
				count++
				continue
			}

			if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				totalErrors++
				fmt.Fprintf(v.Stdout, "unexpected checksum %d, expected %d for key %v, block %d\n", checksum, expected, key, count)
			}

			if err = DecodeTimestampArrayBlock(buf, &ts); err != nil {
				totalErrors++
				fmt.Fprintf(v.Stdout, "unable to decode timestamps for block %d: %q\n", count, err)
			}

			if got, exp := entry.MinTime, ts.MinTime(); got != exp {
				totalErrors++
				fmt.Fprintf(v.Stdout, "unexpected min time %d, expected %d for block %d: %q\n", got, exp, count, err)
			}
			if got, exp := entry.MaxTime, ts.MaxTime(); got != exp {
				totalErrors++
				fmt.Fprintf(v.Stdout, "unexpected max time %d, expected %d for block %d: %q\n", got, exp, count, err)
			}

			count++
		}
	}

	fmt.Fprintf(v.Stdout, "Completed checking %d block(s)\n", count)

	return nil
}
