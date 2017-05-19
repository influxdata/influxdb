package tsdb

//go:generate protoc --gogo_out=. internal/meta.proto

import (
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/escape"
)

// MarshalTags converts a tag set to bytes for use as a lookup key.
func MarshalTags(tags map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(tags) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(tags) * 2) - 1 // separators
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '|'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := tags[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '|'
			buf = buf[len(v)+1:]
		}
	}
	return b
}

// MeasurementFromSeriesKey returns the name of the measurement from a key that
// contains a measurement name.
func MeasurementFromSeriesKey(key []byte) []byte {
	// Ignoring the error because the func returns "missing fields"
	k, _ := models.ParseName(key)
	return escape.Unescape(k)
}
