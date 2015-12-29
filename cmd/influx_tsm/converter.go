package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

type KeyIterator interface {
	Next() bool
	Read() (string, []tsm1.Value, error)
}

// Converter encapsulates the logic for converting b*1 shards to tsm1 shards.
type Converter struct {
	path           string
	maxTSMFileSize uint32
	generation     int
}

// NewConverter returns a new instance of the Converter.
func NewConverter(path string, sz uint32) *Converter {
	return &Converter{
		path:           path,
		maxTSMFileSize: sz,
	}
}

// Process writes the data provided by iter to a tsm1 shard.
func (c *Converter) Process(iter KeyIterator) error {
	// Ensure the tsm1 directory exists.
	if err := os.MkdirAll(c.path, 0777); err != nil {
		return err
	}

	w, err := c.nextTSMWriter()
	if err != nil {
		return err
	}
	defer w.Close()

	// Iterate until no more data remains.
	for iter.Next() {
		k, v, err := iter.Read()
		if err != nil {
			return err
		}
		w.Write(k, v)

		// If we have a max file size configured and we're over it, start a new TSM file.
		if w.Size() > c.maxTSMFileSize {
			if err := w.WriteIndex(); err != nil {
				return err
			}
			if err := w.Close(); err != nil {
				return err
			}

			w, err = c.nextTSMWriter()
			if err != nil {
				return err
			}
		}
	}

	// All done!
	return w.WriteIndex()
}

// nextTSMWriter returns the next TSMWriter for the Converter.
func (c *Converter) nextTSMWriter() (tsm1.TSMWriter, error) {
	c.generation++
	fileName := filepath.Join(c.path, fmt.Sprintf("%09d-%09d.%s", c.generation, 0, tsm1.TSMFileExtension))

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// Create the writer for the new TSM file.
	w, err := tsm1.NewTSMWriter(fd)
	if err != nil {
		return nil, err
	}

	return w, nil
}
