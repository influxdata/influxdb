package http

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
