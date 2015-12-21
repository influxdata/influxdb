package main

import (
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/influxql"
)

type ShardReader interface {
	Open() error
	Next() (int64, []byte)
	Close() error
}

type Field struct {
	ID   uint8             `json:"id,omitempty"`
	Name string            `json:"name,omitempty"`
	Type influxql.DataType `json:"type,omitempty"`
}

type FieldCodec struct {
	fieldsByID   map[uint8]*Field
	fieldsByName map[string]*Field
}

type MeasurementFields struct {
	Fields map[string]*Field `json:"fields"`
	Codec  *FieldCodec
}

type Series struct {
	Key  string
	Tags map[string]string

	id uint64
	//measurement *Measurement
	shardIDs map[uint64]bool
}

func Convert(path string) error {
	// Check file format
	// Create the right reader.
	// Create a TSMWriter.
	// Walk reader, and write to tmp TSM.
	// All good?  Delete src.

	format, _, err := shardFormat(path)
	if err != nil {
		return err
	}

	var reader ShardReader
	switch format {
	case b1:
		reader = NewB1Reader(path)
	case bz1:
		reader = NewBZ1Reader(path)
	default:
		return fmt.Errorf("conversion of %s engine format unsupported", format.String())
	}

	if err := reader.Open(); err != nil {
		return err
	}
	defer reader.Close()
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
