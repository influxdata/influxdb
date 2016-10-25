package inmem

import (
	"sort"

	"github.com/influxdata/influxdb/models"
)

// Index represents an in-memory index.
type Index struct {
	series       Series
	measurements map[string]Measurement
}

// NewIndex returns a new instance of Index.
func NewIndex() *Index {
	return &Index{
		measurements: make(map[string]Measurement),
	}
}

// MeasurementNames returns a sorted list of measurement names.
func (i *Index) MeasurementNames() []string {
	a := make([]string, 0, len(m))
	for name := range m {
		a = append(a, name)
	}
	sort.Strings(a)
	return a
}

// Measurement represents a measurement in the index.
type Measurement struct {
	Name    []byte
	Deleted bool
	TagSet  TagSet
}

// TagSet represents a collection of tags.
type TagSet map[string]Tag

// Tag represents a tag key and its associated values.
type Tag struct {
	Name    []byte
	Deleted bool
	Values  TagValues
}

// TagValue represents a collection of tag values.
type TagValues map[string]TagValue

// TagValue represents a single tag value and its associated series.
type TagValue struct {
	Name    []byte
	Deleted bool
	Series  []Serie
}

// Series represents a sorted list of serie.
type Series []Serie

// Serie represents an individual series.
type Serie struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}
