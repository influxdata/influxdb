package influxdb

import (
	"hash/fnv"
	"sort"
	"time"
)

// Point defines the values that will be written to the database
type Point struct {
	Name   string
	Tags   Tags
	Time   time.Time
	Fields map[string]interface{}
}

func (p *Point) SeriesID() uint64 {

	// <measurementName>|<tagKey>|<tagKey>|<tagValue>|<tagValue>
	// cpu|host|servera
	encodedTags := p.Tags.Marshal()
	size := len(p.Name) + len(encodedTags)
	if len(encodedTags) > 0 {
		size++
	}
	b := make([]byte, 0, size)
	b = append(b, p.Name...)
	if len(encodedTags) > 0 {
		b = append(b, '|')
	}
	b = append(b, encodedTags...)
	// TODO pick a better hashing that guarantees uniqueness
	// TODO create a cash for faster lookup
	h := fnv.New64a()
	h.Write(b)
	sum := h.Sum64()
	return sum
}

type Tags map[string]string

func (t Tags) Marshal() []byte {
	// Empty maps marshal to empty bytes.
	if len(t) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(t) * 2) - 1 // separators
	keys := make([]string, 0, len(t))
	for k, v := range t {
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
		v := t[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '|'
			buf = buf[len(v)+1:]
		}
	}
	return b
}
