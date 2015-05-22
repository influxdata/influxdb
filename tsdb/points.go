package tsdb

import (
	"hash/fnv"
	"sort"
	"time"
)

// Point defines the values that will be written to the database
type Point struct {
	name   string
	Tags   Tags
	time   time.Time
	Fields map[string]interface{}
	key    string
	data   []byte
}

// NewPoint returns a new point with the given measurement name, tags, fiels and timestamp
func NewPoint(name string, tags Tags, fields map[string]interface{}, time time.Time) Point {
	return Point{
		name:   name,
		Tags:   tags,
		time:   time,
		Fields: fields,
	}
}

func (p *Point) Key() string {
	if p.key == "" {
		p.key = p.Name() + "," + string(p.Tags.HashKey())
	}
	return p.key
}

// Name return the measurement name for the point
func (p *Point) Name() string {
	return p.name
}

// SetName updates the measurement name for the point
func (p *Point) SetName(name string) {
	p.name = name
}

// Time return the timesteamp for the point
func (p *Point) Time() time.Time {
	return p.time
}

// SetTime updates the timestamp for the point
func (p *Point) SetTime(t time.Time) {
	p.time = t
}

func (p *Point) HashID() uint64 {

	// <measurementName>|<tagKey>|<tagKey>|<tagValue>|<tagValue>
	// cpu|host|servera
	encodedTags := p.Tags.HashKey()
	size := len(p.Name()) + len(encodedTags)
	if len(encodedTags) > 0 {
		size++
	}
	b := make([]byte, 0, size)
	b = append(b, p.Name()...)
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

func (t Tags) HashKey() []byte {
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
