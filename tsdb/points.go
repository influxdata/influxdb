package tsdb

import (
	"hash/fnv"
	"sort"
	"time"
)

// Point defines the values that will be written to the database
type Point interface {
	Name() string
	SetName(string)

	Tags() Tags
	AddTag(key, value string)
	SetTags(tags Tags)

	Fields() map[string]interface{}
	AddField(name string, value interface{})

	Time() time.Time
	SetTime(t time.Time)

	HashID() uint64
	Key() string

	Data() []byte
	SetData(buf []byte)
}

// point is the default implementation of Point.
type point struct {
	name   string
	tags   Tags
	time   time.Time
	fields map[string]interface{}
	key    string
	data   []byte
}

// NewPoint returns a new point with the given measurement name, tags, fiels and timestamp
func NewPoint(name string, tags Tags, fields map[string]interface{}, time time.Time) Point {
	return &point{
		name:   name,
		tags:   tags,
		time:   time,
		fields: fields,
	}
}

func (p *point) Data() []byte {
	return p.data
}

func (p *point) SetData(b []byte) {
	p.data = b
}

func (p *point) Key() string {
	if p.key == "" {
		p.key = p.Name() + "," + string(p.tags.HashKey())
	}
	return p.key
}

// Name return the measurement name for the point
func (p *point) Name() string {
	return p.name
}

// SetName updates the measurement name for the point
func (p *point) SetName(name string) {
	p.name = name
}

// Time return the timesteamp for the point
func (p *point) Time() time.Time {
	return p.time
}

// SetTime updates the timestamp for the point
func (p *point) SetTime(t time.Time) {
	p.time = t
}

// Tags returns the tag set for the point
func (p *point) Tags() Tags {
	return p.tags
}

// SetTags replaces the tags for the point
func (p *point) SetTags(tags Tags) {
	p.tags = tags
}

// AddTag adds or replaces a tag value for a point
func (p *point) AddTag(key, value string) {
	p.tags[key] = value
}

// Fields returns the fiels for the point
func (p *point) Fields() map[string]interface{} {
	return p.fields
}

// AddField adds or replaces a field value for a point
func (p *point) AddField(name string, value interface{}) {
	p.fields[name] = value
}

func (p *point) HashID() uint64 {

	// <measurementName>|<tagKey>|<tagKey>|<tagValue>|<tagValue>
	// cpu|host|servera
	encodedTags := p.tags.HashKey()
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
