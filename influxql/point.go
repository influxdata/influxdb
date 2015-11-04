package influxql

import (
	"sort"
)

// ZeroTime is the Unix nanosecond timestamp for time.Time{}.
const ZeroTime = int64(-6795364578871345152)

// Point represents a value in a series that occurred at a given time.
type Point interface {
	// Name and tags uniquely identify the series the value belongs to.
	name() string
	tags() Tags

	// The time that the value occurred at.
	time() int64

	// The value at the given time.
	value() interface{}

	// Auxillary values passed along with the value.
	aux() []interface{}
}

// Points represents a list of points.
type Points []Point

// FloatPoint represents a point with a float value.
type FloatPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value float64
	Aux   []interface{}
}

func (v *FloatPoint) name() string       { return v.Name }
func (v *FloatPoint) tags() Tags         { return v.Tags }
func (v *FloatPoint) time() int64        { return v.Time }
func (v *FloatPoint) value() interface{} { return v.Value }
func (v *FloatPoint) aux() []interface{} { return v.Aux }

// Clone returns a copy of v.
func (v *FloatPoint) Clone() *FloatPoint {
	if v == nil {
		return nil
	}

	other := *v
	if v.Aux != nil {
		other.Aux = make([]interface{}, len(v.Aux))
		copy(other.Aux, v.Aux)
	}

	return &other
}

// floatPoints represents a slice of points sortable by value.
type floatPoints []FloatPoint

func (a floatPoints) Len() int           { return len(a) }
func (a floatPoints) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a floatPoints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// floatPointsByValue represents a slice of points sortable by value.
type floatPointsByValue []FloatPoint

func (a floatPointsByValue) Len() int           { return len(a) }
func (a floatPointsByValue) Less(i, j int) bool { return a[i].Value < a[j].Value }
func (a floatPointsByValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// StringPoint represents a point with a string value.
type StringPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value string
	Aux   []interface{}
}

func (v *StringPoint) name() string       { return v.Name }
func (v *StringPoint) tags() Tags         { return v.Tags }
func (v *StringPoint) time() int64        { return v.Time }
func (v *StringPoint) value() interface{} { return v.Value }
func (v *StringPoint) aux() []interface{} { return v.Aux }

// stringPoints represents a slice of points sortable by value.
type stringPoints []StringPoint

func (a stringPoints) Len() int           { return len(a) }
func (a stringPoints) Less(i, j int) bool { return a[i].Time < a[j].Time }
func (a stringPoints) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// BooleanPoint represents a point with a boolean value.
type BooleanPoint struct {
	Name string
	Tags Tags

	Time  int64
	Value bool
	Aux   []interface{}
}

func (v *BooleanPoint) name() string       { return v.Name }
func (v *BooleanPoint) tags() Tags         { return v.Tags }
func (v *BooleanPoint) time() int64        { return v.Time }
func (v *BooleanPoint) value() interface{} { return v.Value }
func (v *BooleanPoint) aux() []interface{} { return v.Aux }

// Tags represent a map of keys and values.
// It memoizes its key so it can be used efficiently during query execution.
type Tags struct {
	id string
	m  map[string]string
}

// NewTags returns a new instance of Tags.
func NewTags(m map[string]string) Tags {
	if len(m) == 0 {
		return Tags{}
	}
	return Tags{
		id: string(encodeTags(m)),
		m:  m,
	}
}

// ID returns the string identifier for the tags.
func (t Tags) ID() string { return t.id }

// KeyValues returns the underlying map for the tags.
func (t Tags) KeyValues() map[string]string { return t.m }

// Keys returns a sorted list of all keys on the tag.
func (t *Tags) Keys() []string {
	if t == nil {
		return nil
	}

	var a []string
	for k := range t.m {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Value returns the value for a given key.
func (t *Tags) Value(k string) string {
	if t == nil {
		return ""
	}
	return t.m[k]
}

// Subset returns a new tags object with a subset of the keys.
func (t *Tags) Subset(keys []string) Tags {
	if t.m == nil || len(keys) == 0 {
		return Tags{}
	}

	// If keys match existing keys, simply return this tagset.
	if keysMatch(t.m, keys) {
		return *t
	}

	// Otherwise create new tag set.
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		m[k] = t.m[k]
	}
	return NewTags(m)
}

// Equals returns true if t equals other.
func (t *Tags) Equals(other *Tags) bool {
	if t == nil && other == nil {
		return true
	} else if t == nil || other == nil {
		return false
	}
	return t.id == other.id
}

// keysMatch returns true if m has exactly the same keys as listed in keys.
func keysMatch(m map[string]string, keys []string) bool {
	if len(keys) != len(m) {
		return false
	}

	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}

	return true
}

// encodeTags converts a map of strings to an identifier.
func encodeTags(m map[string]string) []byte {
	// Empty maps marshal to empty bytes.
	if len(m) == 0 {
		return nil
	}

	// Extract keys and determine final size.
	sz := (len(m) * 2) - 1 // separators
	keys := make([]string, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		sz += len(k) + len(v)
	}
	sort.Strings(keys)

	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	for _, k := range keys {
		copy(buf, k)
		buf[len(k)] = '\x00'
		buf = buf[len(k)+1:]
	}
	for i, k := range keys {
		v := m[k]
		copy(buf, v)
		if i < len(keys)-1 {
			buf[len(v)] = '\x00'
			buf = buf[len(v)+1:]
		}
	}
	return b
}
