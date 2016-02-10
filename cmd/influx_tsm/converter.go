package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type KeyIterator interface {
	Next() bool
	Read() (string, []tsm1.Value, error)
}

// Converter encapsulates the logic for converting b*1 shards to tsm1 shards.
type Converter struct {
	path           string
	maxTSMFileSize uint32
	sequence       int
	tracker        *tracker
}

// NewConverter returns a new instance of the Converter.
func NewConverter(path string, sz uint32, t *tracker) *Converter {
	return &Converter{
		path:           path,
		maxTSMFileSize: sz,
		tracker:        t,
	}
}

// Process writes the data provided by iter to a tsm1 shard.
func (c *Converter) Process(iter KeyIterator) error {
	// Ensure the tsm1 directory exists.
	if err := os.MkdirAll(c.path, 0777); err != nil {
		return err
	}

	// Iterate until no more data remains.
	var w tsm1.TSMWriter
	for iter.Next() {
		k, v, err := iter.Read()
		if err != nil {
			return err
		}
		scrubbed := c.scrubValues(v)

		if w == nil {
			w, err = c.nextTSMWriter()
			if err != nil {
				return err
			}
		}
		if err := w.Write(k, scrubbed); err != nil {
			return err
		}

		c.tracker.AddPointsRead(len(v))
		c.tracker.AddPointsWritten(len(scrubbed))

		// If we have a max file size configured and we're over it, start a new TSM file.
		if w.Size() > c.maxTSMFileSize {
			if err := w.WriteIndex(); err != nil && err != tsm1.ErrNoValues {
				return err
			}

			c.tracker.AddTSMBytes(w.Size())

			if err := w.Close(); err != nil {
				return err
			}
			w = nil
		}
	}

	if w != nil {
		if err := w.WriteIndex(); err != nil && err != tsm1.ErrNoValues {
			return err
		}
		c.tracker.AddTSMBytes(w.Size())

		if err := w.Close(); err != nil {
			return err
		}
	}

	return nil
}

// nextTSMWriter returns the next TSMWriter for the Converter.
func (c *Converter) nextTSMWriter() (tsm1.TSMWriter, error) {
	c.sequence++
	fileName := filepath.Join(c.path, fmt.Sprintf("%09d-%09d.%s", 1, c.sequence, tsm1.TSMFileExtension))

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// Create the writer for the new TSM file.
	w, err := tsm1.NewTSMWriter(fd)
	if err != nil {
		return nil, err
	}

	c.tracker.IncrTSMFileCount()
	return w, nil
}

// scrubValues takes a slice and removes float64 NaN and Inf. If neither is
// present in the slice, the original slice is returned. This is to avoid
// copying slices unnecessarily.
func (c *Converter) scrubValues(values []tsm1.Value) []tsm1.Value {
	var scrubbed []tsm1.Value

	if values == nil {
		return nil
	}

	for i, v := range values {
		if f, ok := v.Value().(float64); ok {
			var filter bool
			if math.IsNaN(f) {
				filter = true
				c.tracker.IncrNaN()
			}
			if math.IsInf(f, 0) {
				filter = true
				c.tracker.IncrInf()
			}

			if filter {
				if scrubbed == nil {
					// Take every value up to the NaN, indicating that scrubbed
					// should now be used.
					scrubbed = values[:i]
				}
			} else {
				if scrubbed != nil {
					// We've filtered at least 1 value, so add value to filtered slice.
					scrubbed = append(scrubbed, v)
				}
			}
		}
	}

	if scrubbed != nil {
		return scrubbed
	}
	return values
}
