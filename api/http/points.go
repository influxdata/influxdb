package http

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	// Compile the regex that detects unquoted double quote sequences
	quoteReplacer = regexp.MustCompile(`([^\\])"`)

	escapeCodes = map[byte][]byte{
		',': []byte(`\,`),
		'"': []byte(`\"`),
		' ': []byte(`\ `),
		'=': []byte(`\=`),
	}

	escapeCodesStr = map[string]string{}
)

func init() {
	for k, v := range escapeCodes {
		escapeCodesStr[string(k)] = string(v)
	}
}

// Point9 represents a 0.9 tsdb point
type Point9 struct {
	time time.Time

	// text encoding of measurement and tags
	// key must always be stored sorted by tags, if the original line was not sorted,
	// we need to resort it
	key []byte

	// text encoding of field data
	fields []byte

	// text encoding of timestamp
	ts []byte

	// binary encoded field data
	data []byte
}

// NewPoint returns a new point with the given measurement name, tags, fields and timestamp
func NewPoint9(name string, tags Tags, fields Fields, time time.Time) *Point9 {
	return &Point9{
		key:    makeKey([]byte(name), tags),
		time:   time,
		fields: fields.MarshalBinary(),
	}
}

func (p *Point9) String() string {
	if p.time.IsZero() {
		return fmt.Sprintf("%s %s", p.key, string(p.fields))
	}
	return fmt.Sprintf("%s %s %d", p.key, string(p.fields), p.time.UnixNano())
}

func makeKey(name []byte, tags Tags) []byte {
	return append(escape(name), tags.HashKey()...)
}

type Tags map[string]string

func (t Tags) HashKey() []byte {
	// Empty maps marshal to empty bytes.
	if len(t) == 0 {
		return nil
	}

	escaped := Tags{}
	for k, v := range t {
		ek := escapeString(k)
		ev := escapeString(v)
		escaped[ek] = ev
	}

	// Extract keys and determine final size.
	sz := len(escaped) + (len(escaped) * 2) // separators
	keys := make([]string, len(escaped)+1)
	i := 0
	for k, v := range escaped {
		keys[i] = k
		i += 1
		sz += len(k) + len(v)
	}
	keys = keys[:i]
	sort.Strings(keys)
	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	idx := 0
	for _, k := range keys {
		buf[idx] = ','
		idx += 1
		copy(buf[idx:idx+len(k)], k)
		idx += len(k)
		buf[idx] = '='
		idx += 1
		v := escaped[k]
		copy(buf[idx:idx+len(v)], v)
		idx += len(v)
	}
	return b[:idx]
}

type Fields map[string]interface{}

func (p Fields) MarshalBinary() []byte {
	b := []byte{}
	keys := make([]string, len(p))
	i := 0
	for k, _ := range p {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := p[k]
		b = append(b, []byte(escapeString(k))...)
		b = append(b, '=')
		switch t := v.(type) {
		case int:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
		case int32:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
		case uint64:
			b = append(b, []byte(strconv.FormatUint(t, 10))...)
		case int64:
			b = append(b, []byte(strconv.FormatInt(t, 10))...)
		case float64:
			// ensure there is a decimal in the encoded for

			val := []byte(strconv.FormatFloat(t, 'f', -1, 64))
			_, frac := math.Modf(t)
			hasDecimal := frac != 0
			b = append(b, val...)
			if !hasDecimal {
				b = append(b, []byte(".0")...)
			}
		case bool:
			b = append(b, []byte(strconv.FormatBool(t))...)
		case []byte:
			b = append(b, t...)
		case string:
			b = append(b, '"')
			b = append(b, []byte(escapeQuoteString(t))...)
			b = append(b, '"')
		case nil:
			// skip
		default:
			// Can't determine the type, so convert to string
			b = append(b, '"')
			b = append(b, []byte(escapeQuoteString(fmt.Sprintf("%v", v)))...)
			b = append(b, '"')

		}
		b = append(b, ',')
	}
	if len(b) > 0 {
		return b[0 : len(b)-1]
	}
	return b
}

func escapeString(in string) string {
	for b, esc := range escapeCodesStr {
		in = strings.Replace(in, b, esc, -1)
	}
	return in
}

// escapeQuoteString returns a copy of in with any double quotes that
// have not been escaped with escaped quotes
func escapeQuoteString(in string) string {
	if strings.IndexAny(in, `"`) == -1 {
		return in
	}
	return quoteReplacer.ReplaceAllString(in, `$1\"`)
}

func escape(in []byte) []byte {
	for b, esc := range escapeCodes {
		in = bytes.Replace(in, []byte{b}, esc, -1)
	}
	return in
}

func parseSeriesName(series, separator string) (string, map[string]string) {
	tags := make(map[string]string)
	vals := strings.Split(series, separator)
	name := vals[len(vals)-1]
	if len(vals)%2 != 1 {
		vals = append(vals[:len(vals)-1], "upgrade_artifacts", name)
	}
	for i := 0; i < len(vals)-1; i += 2 {
		tags[vals[i]] = vals[i+1]
	}
	return name, tags
}
