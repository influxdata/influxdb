package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
	tsm "github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

type ShardReader interface {
	Open() error
	Next() (int64, interface{})
	Close() error
}

// Cursor represents an iterator over a series.
type Cursor interface {
	SeekTo(seek int64) (key int64, value interface{})
	Next() (key int64, value interface{})
}

type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

type MeasurementFields struct {
	Fields map[string]*Field `json:"fields"`
	Codec  *FieldCodec
}

type Series struct {
	Key  string
	Tags map[string]string
}

func MeasurementFromSeriesKey(key string) string {
	idx := strings.Index(key, ",")
	if idx == -1 {
		return key
	}
	return key[:strings.Index(key, ",")]
}

func Convert(path string) error {
	// Create a TSMWriter.
	// Walk reader, and write to tmp TSM directory.
	// All good?  Delete src.

	// What format?
	format, _, err := shardFormat(path)
	if err != nil {
		return err
	}

	// Create a suitable reader and open it.
	var reader ShardReader
	switch format {
	case b1:
		reader = NewB1Reader(path)
	case bz1:
		reader = NewBZ1Reader(path)
	default:
		return fmt.Errorf("conversion of %s shard format unsupported", format.String())
	}

	if err := reader.Open(); err != nil {
		return err
	}

	// Create the TSM file and TSM writer.
	tfd, err := os.Create(fmt.Sprintf("%s.%s", path, tsm.TSMFileExtension))
	if err != nil {
		return err
	}
	writer, err := tsm.NewTSMWriter(tfd)
	if err != nil {
		return err
	}

	// Conversion complete! Close everything and delete source.
	if err := reader.Close(); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}

	return nil
}

// shardFormat returns the format and size on disk of the shard at path.
func shardFormat(path string) (EngineFormat, int64, error) {
	// If it's a directory then it's a tsm1 engine
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	fi, err := f.Stat()
	f.Close()
	if err != nil {
		return 0, 0, err
	}
	if fi.Mode().IsDir() {
		return tsm1, fi.Size(), nil
	}

	// It must be a BoltDB-based engine.
	db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()

	var format EngineFormat
	err = db.View(func(tx *bolt.Tx) error {
		// Retrieve the meta bucket.
		b := tx.Bucket([]byte("meta"))

		// If no format is specified then it must be an original b1 database.
		if b == nil {
			format = b1
			return nil
		}

		// "v1" engines are also b1.
		if string(b.Get([]byte("format"))) == "v1" {
			format = b1
			return nil
		}

		format = bz1
		return nil
	})
	return format, fi.Size(), err
}

// decodeKeyValue decodes the key and value from bytes.
func decodeKeyValue(field string, dec *FieldCodec, k, v []byte) (int64, interface{}) {
	// Convert key to a timestamp.
	key := int64(btou64(k[0:8]))

	decValue, err := dec.DecodeByName(field, v)
	if err != nil {
		return -1, nil
	}
	return key, decValue
}

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts an 8-byte slice into an uint64.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
