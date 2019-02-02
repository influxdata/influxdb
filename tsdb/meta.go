package tsdb

//go:generate protoc --gogo_out=. internal/meta.proto

import (
	"sort"

	"github.com/influxdata/influxdb/models"
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

// MakeTagsKey converts a tag set to bytes for use as a lookup key.
func MakeTagsKey(keys []string, tags models.Tags) []byte {
	// precondition: keys is sorted
	// precondition: models.Tags is sorted

	// Empty maps marshal to empty bytes.
	if len(keys) == 0 || len(tags) == 0 {
		return nil
	}

	sel := make([]int, 0, len(keys))

	sz := 0
	i, j := 0, 0
	for i < len(keys) && j < len(tags) {
		if keys[i] < string(tags[j].Key) {
			i++
		} else if keys[i] > string(tags[j].Key) {
			j++
		} else {
			sel = append(sel, j)
			sz += len(keys[i]) + len(tags[j].Value)
			i++
			j++
		}
	}

	if len(sel) == 0 {
		// no tags matched the requested keys
		return nil
	}

	sz += (len(sel) * 2) - 1 // selected tags, add separators

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range sel {
		copy(buf, tags[k].Key)
		buf[len(tags[k].Key)] = '|'
		buf = buf[len(tags[k].Key)+1:]
	}

	for i, k := range sel {
		copy(buf, tags[k].Value)
		if i < len(sel)-1 {
			buf[len(tags[k].Value)] = '|'
			buf = buf[len(tags[k].Value)+1:]
		}
	}

	return b
}
