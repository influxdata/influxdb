package influxql

import (
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql/internal"
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

// newTagsID returns a new instance of Tags parses from a tag id.
func newTagsID(id string) Tags {
	m := decodeTags([]byte(id))
	if len(m) == 0 {
		return Tags{}
	}
	return Tags{id: id, m: m}
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

// decodeTags parses an identifier into a map of tags.
func decodeTags(id []byte) map[string]string { panic("FIXME: implement") }

func encodeAux(aux []interface{}) []*internal.Aux {
	pb := make([]*internal.Aux, len(aux))
	for i := range aux {
		switch v := aux[i].(type) {
		case float64:
			pb[i] = &internal.Aux{DataType: proto.Int32(Float), FloatValue: proto.Float64(v)}
		case *float64:
			pb[i] = &internal.Aux{DataType: proto.Int32(Float)}
		case int64:
			pb[i] = &internal.Aux{DataType: proto.Int32(Integer), IntegerValue: proto.Int64(v)}
		case *int64:
			pb[i] = &internal.Aux{DataType: proto.Int32(Integer)}
		case string:
			pb[i] = &internal.Aux{DataType: proto.Int32(String), StringValue: proto.String(v)}
		case *string:
			pb[i] = &internal.Aux{DataType: proto.Int32(String)}
		case bool:
			pb[i] = &internal.Aux{DataType: proto.Int32(Boolean), BooleanValue: proto.Bool(v)}
		case *bool:
			pb[i] = &internal.Aux{DataType: proto.Int32(Boolean)}
		default:
			pb[i] = &internal.Aux{DataType: proto.Int32(int32(Unknown))}
		}
	}
	return pb
}

func decodeAux(pb []*internal.Aux) []interface{} {
	aux := make([]interface{}, len(pb))
	for i := range pb {
		switch pb[i].GetDataType() {
		case Float:
			if pb[i].FloatValue != nil {
				aux[i] = *pb[i].FloatValue
			} else {
				aux[i] = (*float64)(nil)
			}
		case Integer:
			if pb[i].IntegerValue != nil {
				aux[i] = *pb[i].IntegerValue
			} else {
				aux[i] = (*int64)(nil)
			}
		case String:
			if pb[i].StringValue != nil {
				aux[i] = *pb[i].StringValue
			} else {
				aux[i] = (*string)(nil)
			}
		case Boolean:
			if pb[i].BooleanValue != nil {
				aux[i] = *pb[i].BooleanValue
			} else {
				aux[i] = (*bool)(nil)
			}
		default:
			aux[i] = nil
		}
	}
	return aux
}
