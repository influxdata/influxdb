package models

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	FieldKeyTagKey    = "\xff"
	MeasurementTagKey = "\x00"

	fieldKeyTagKeySep = ",\xff="
)

var (
	fieldKeyTagKeySepBytes = []byte(fieldKeyTagKeySep)
)

type escapeSet struct {
	k   [1]byte
	esc [2]byte
}

// Many routines in this package make assumptions that k is 1 byte
// and esc is 2 bytes. This code ensures that if these invariants
// change, a compile-time error occurs and this entire package
// must be carefully evaluated for potential bugs.
var (
	_ [1]byte = escapeSet{}.k
	_ [2]byte = escapeSet{}.esc
)

var (
	measurementEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
	}

	tagEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
		{k: [1]byte{'='}, esc: [2]byte{'\\', '='}},
	}

	// ErrMeasurementTagExpected is returned by ParseMeasurement when parsing a
	// series key where the first tag key is not a measurement.
	ErrMeasurementTagExpected = errors.New("measurement tag expected")

	// ErrInvalidKey is returned by ParseMeasurement when parsing an empty
	// or invalid series key.
	ErrInvalidKey = errors.New("invalid key")
)

// scanTo returns the end position in buf and the next consecutive block
// of bytes, starting from i and ending with stop byte, where stop byte
// has not been escaped.
//
// If there are leading spaces, they are skipped.
func scanTo(buf Escaped, i int, stop byte) (int, Escaped) {
	start := i
	for {
		// reached the end of buf?
		if i >= len(buf.B) {
			break
		}

		// Reached unescaped stop value?
		if buf.B[i] == stop && (i == 0 || buf.B[i-1] != '\\') {
			break
		}
		i++
	}

	return i, buf.Slice(start, i)
}

func walkTags(buf Escaped, fn func(key, value []byte) bool) {
	if len(buf.B) == 0 {
		return
	}

	pos, name := scanTo(buf, 0, ',')

	// it's an empty key, so there are no tags
	if len(name.B) == 0 {
		return
	}

	hasEscape := bytes.IndexByte(buf.B, '\\') != -1
	i := pos + 1
	var key, value Escaped
	for {
		if i >= len(buf.B) {
			break
		}
		i, key = scanTo(buf, i, '=')
		i, value = scanTagValue(buf, i+1)

		if len(value.B) == 0 {
			continue
		}

		if hasEscape {
			if !fn(UnescapeToken(key), UnescapeToken(value)) {
				return
			}
		} else {
			if !fn(key.B, value.B) {
				return
			}
		}

		i++
	}
}

func scanTagValue(buf Escaped, i int) (int, Escaped) {
	start := i
	for {
		if i >= len(buf.B) {
			break
		}

		if buf.B[i] == ',' && buf.B[i-1] != '\\' {
			break
		}
		i++
	}
	if i > len(buf.B) {
		return i, Escaped{}
	}
	return i, buf.Slice(start, i)
}

// parseTags parses buf into the provided destination tags, returning destination
// Tags, which may have a different length and capacity.
func parseTags(buf Escaped, dst Tags) Tags {
	if len(buf.B) == 0 {
		return nil
	}

	n := bytes.Count(buf.B, []byte(","))
	if cap(dst) < n {
		dst = make(Tags, n)
	} else {
		dst = dst[:n]
	}

	// Ensure existing behaviour when point has no tags and nil slice passed in.
	if dst == nil {
		dst = Tags{}
	}

	// Series keys can contain escaped commas, therefore the number of commas
	// in a series key only gives an estimation of the upper bound on the number
	// of tags.
	var i int
	walkTags(buf, func(key, value []byte) bool {
		dst[i].Key, dst[i].Value = key, value
		i++
		return true
	})
	return dst[:i]
}

// Escaped represents an escaped portion of line-protocol syntax
// using an underlying byte slice. It's defined as a struct to avoid
// unintentional implicit conversion between []byte and Escaped.
//
// In general you should avoid using the B field unless necessary,
// as doing so risks breaking the typed assertion that the data is
// validly escaped syntax.
type Escaped struct{ B []byte }

// MakeEscaped returns the equivalent of Escaped{B: b}.
// Only use this when you're sure that b is either already
// escaped or (in tests) when b has no characters that need escaping.
func MakeEscaped(b []byte) Escaped {
	return Escaped{b}
}

// S returns the EscapedString form of m.
func (e Escaped) S() EscapedString {
	return EscapedString(e.B)
}

// Clone makes a copy of e and its underlying byte slice.
func (e Escaped) Clone() Escaped {
	return Escaped{B: cloneBytes(e.B)}
}

// Slice returns a slice of the underlying data from i0 to i1.
func (e Escaped) Slice(i0, i1 int) Escaped {
	return Escaped{e.B[i0:i1]}
}

// SliceToEnd returns a slice of the underlying data from i0 to the end of the slice.
func (e Escaped) SliceToEnd(i0 int) Escaped {
	return e.Slice(i0, len(e.B))
}

// EscapedString represents an escaped portion of line-protocol syntax.
// See Escaped for more information.
type EscapedString string

// B returns the byte-slice form of e.
func (e EscapedString) B() Escaped {
	return Escaped{[]byte(e)}
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}

func UnescapeToken(in Escaped) []byte {
	out := in.B
	if bytes.IndexByte(out, '\\') == -1 {
		return out
	}

	for i := range tagEscapeCodes {
		c := &tagEscapeCodes[i]
		if bytes.IndexByte(out, c.k[0]) != -1 {
			out = bytes.Replace(out, c.esc[:], c.k[:], -1)
		}
	}
	return out
}

func UnescapeMeasurement(inm Escaped) []byte {
	in := inm.B
	if bytes.IndexByte(in, '\\') == -1 {
		return in
	}

	for i := range measurementEscapeCodes {
		c := &measurementEscapeCodes[i]
		if bytes.IndexByte(in, c.k[0]) != -1 {
			in = bytes.Replace(in, c.esc[:], c.k[:], -1)
		}
	}
	return in
}

// The following constants allow us to specify which state to move to
// next, when scanning sections of a Point.
const (
	tagKeyState = iota
	tagValueState
	fieldsState
)

// scanMeasurement examines the measurement part of a Point, returning
// the next state to move to, and the current location in the buffer.
func scanMeasurement(bufe Escaped, i int) (int, int, error) {
	buf := bufe.B
	// Check first byte of measurement, anything except a comma is fine.
	// It can't be a space, since whitespace is stripped prior to this
	// function call.
	if i >= len(buf) || buf[i] == ',' {
		return -1, i, fmt.Errorf("missing measurement in: %q", buf)
	}

	for {
		i++
		if i >= len(buf) {
			// cpu
			return -1, i, fmt.Errorf("missing fields in: %d", buf)
		}

		if buf[i-1] == '\\' {
			// Skip character (it's escaped).
			continue
		}

		// Unescaped comma; move onto scanning the tags.
		if buf[i] == ',' {
			return tagKeyState, i + 1, nil
		}

		// Unescaped space; move onto scanning the fields.
		if buf[i] == ' ' {
			// cpu value=1.0
			return fieldsState, i, nil
		}
	}
}

func ParseTags(buf Escaped) Tags {
	return parseTags(buf, nil)
}

func ParseKeyBytesWithTags(buf Escaped, tags Tags) ([]byte, Tags) {
	// Ignore the error because scanMeasurement returns "missing fields" which we ignore
	// when just parsing a key
	state, i, _ := scanMeasurement(buf, 0)

	var name Escaped
	if state == tagKeyState {
		tags = parseTags(buf, tags)
		// scanMeasurement returns the location of the comma if there are tags, strip that off
		name.B = buf.B[:i-1]
	} else {
		name.B = buf.B[:i]
	}
	return UnescapeMeasurement(name), tags
}

// SeriesAndField parses splits the key at the last tag, which is the reserved
// \xff tag key.
func SeriesAndField(key Escaped) (series, field Escaped, found bool) {
	sep := bytes.LastIndex(key.B, fieldKeyTagKeySepBytes)
	if sep == -1 {
		// No field???
		return key, Escaped{}, false
	}
	return key.Slice(0, sep), key.SliceToEnd(sep + len(fieldKeyTagKeySepBytes)), true
}

// ParseMeasurement returns the value of the tag identified by MeasurementTagKey; otherwise,
// an error is returned.
//
// buf must be a normalized series key, such that the tags are
// lexicographically sorted and therefore the measurement tag is first.
func ParseMeasurement(buf Escaped) ([]byte, error) {
	pos, name := scanTo(buf, 0, ',')

	// it's an empty key, so there are no tags
	if len(name.B) == 0 {
		return nil, ErrInvalidKey
	}

	i := pos + 1
	var key, value Escaped
	i, key = scanTo(buf, i, '=')
	if key.S() != MeasurementTagKey {
		return nil, ErrMeasurementTagExpected
	}

	_, value = scanTagValue(buf, i+1)
	if bytes.IndexByte(value.B, '\\') != -1 {
		// hasEscape
		return UnescapeToken(value), nil
	}
	return value.B, nil
}

// Tag represents a single key/value tag pair.
type Tag struct {
	Key   []byte
	Value []byte
}

// NewTag returns a new Tag.
func NewTag(key, value []byte) Tag {
	return Tag{
		Key:   key,
		Value: value,
	}
}

// Size returns the size of the key and value.
func (t Tag) Size() int { return len(t.Key) + len(t.Value) }

// Clone returns a deep copy of Tag.
//
// Tags associated with a Point created by ParsePointsWithPrecision will hold references to the byte slice that was parsed.
// Use Clone to create a Tag with new byte slices that do not refer to the argument to ParsePointsWithPrecision.
func (t Tag) Clone() Tag {
	return Tag{
		Key:   cloneBytes(t.Key),
		Value: cloneBytes(t.Value),
	}
}

// String returns the string representation of the tag.
func (t *Tag) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.WriteString(string(t.Key))
	buf.WriteByte(' ')
	buf.WriteString(string(t.Value))
	buf.WriteByte('}')
	return buf.String()
}

// Tags represents a sorted list of tags.
type Tags []Tag
