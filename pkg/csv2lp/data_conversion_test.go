package csv2lp

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_EscapeMeasurement tests escapeMeasurement function
func Test_escapeMeasurement(t *testing.T) {
	var tests = []struct {
		value  string
		expect string
	}{
		{"a", "a"}, {"", ""},
		{"a,", `a\,`},
		{"a ", `a\ `},
		{"a=", `a=`},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, test.expect, escapeMeasurement(test.value))
		})
	}
}

// Test_EscapeTag tests escapeTag function
func Test_EscapeTag(t *testing.T) {
	var tests = []struct {
		value  string
		expect string
	}{
		{"a", "a"}, {"", ""},
		{"a,", `a\,`},
		{"a ", `a\ `},
		{"a=", `a\=`},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, test.expect, escapeTag(test.value))
		})
	}
}

// Test_EscapeString tests escapeString function
func Test_EscapeString(t *testing.T) {
	var tests = []struct {
		value  string
		expect string
	}{
		{"a", `a`}, {"", ``},
		{`a"`, `a\"`},
		{`a\`, `a\\`},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, test.expect, escapeString(test.value))
		})
	}
}

// Test_ToTypedValue tests toTypedValue function
func Test_ToTypedValue(t *testing.T) {
	epochTime, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:00Z")
	var tests = []struct {
		dataType string
		value    string
		expect   interface{}
	}{
		{"string", "a", "a"},
		{"double", "1.0", float64(1.0)},
		{"boolean", "true", true},
		{"boolean", "True", true},
		{"boolean", "y", true},
		{"boolean", "Yes", true},
		{"boolean", "1", true},
		{"boolean", "false", false},
		{"boolean", "False", false},
		{"boolean", "n", false},
		{"boolean", "No", false},
		{"boolean", "0", false},
		{"boolean", "", nil},
		{"boolean", "?", nil},
		{"long", "1", int64(1)},
		{"unsignedLong", "1", uint64(1)},
		{"duration", "1ns", time.Duration(1)},
		{"base64Binary", "YWFh", []byte("aaa")},
		{"dateTime:RFC3339", "1970-01-01T00:00:00Z", epochTime},
		{"dateTime:RFC3339Nano", "1970-01-01T00:00:00.0Z", epochTime},
		{"dateTime:RFC3339", "1970-01-01T00:00:00.000000001Z", epochTime.Add(time.Duration(1))},
		{"dateTime:RFC3339Nano", "1970-01-01T00:00:00.000000002Z", epochTime.Add(time.Duration(2))},
		{"dateTime:number", "3", epochTime.Add(time.Duration(3))},
		{"dateTime", "4", epochTime.Add(time.Duration(4))},
		{"dateTime:2006-01-02", "1970-01-01", epochTime},
		{"dateTime", "1970-01-01T00:00:00Z", epochTime},
		{"dateTime", "1970-01-01T00:00:00.000000001Z", epochTime.Add(time.Duration(1))},
		{"double:, .", "200 100.299,0", float64(200100299.0)},
		{"long:, .", "200 100.299,0", int64(200100299)},
		{"unsignedLong:, .", "200 100.299,0", uint64(200100299)},
		{"u.type", "", nil},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i)+" "+test.value, func(t *testing.T) {
			column := &CsvTableColumn{Label: "test"}
			column.setupDataType(test.dataType)
			val, err := toTypedValue(test.value, column, 1)
			if err != nil && test.expect != nil {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, val)
		})
	}
}

// Test_ToTypedValue_dateTimeCustomTimeZone tests custom timezone when calling toTypedValue function
func Test_ToTypedValue_dateTimeCustomTimeZone(t *testing.T) {
	epochTime, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:00Z")
	tz, _ := parseTimeZone("-0100")
	var tests = []struct {
		dataType string
		value    string
		expect   interface{}
	}{
		{"dateTime:RFC3339", "1970-01-01T00:00:00Z", epochTime},
		{"dateTime:RFC3339Nano", "1970-01-01T00:00:00.0Z", epochTime},
		{"dateTime:number", "3", epochTime.Add(time.Duration(3))},
		{"dateTime:2006-01-02", "1970-01-01", epochTime.Add(time.Hour)},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i)+" "+test.value, func(t *testing.T) {
			column := &CsvTableColumn{}
			column.TimeZone = tz
			column.setupDataType(test.dataType)
			val, err := toTypedValue(test.value, column, 1)
			if err != nil && test.expect != nil {
				require.Nil(t, err.Error())
			}
			if test.expect == nil {
				require.Equal(t, test.expect, val)
			} else {
				expectTime := test.expect.(time.Time)
				time := val.(time.Time)
				require.True(t, expectTime.Equal(time))
			}
		})
	}
}

// Test_WriteProtocolValue tests writeProtocolValue function
func Test_AppendProtocolValue(t *testing.T) {
	epochTime, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:00Z")
	var tests = []struct {
		value  interface{}
		expect string
	}{
		{uint64(1), "1u"},
		{int64(1), "1i"},
		{int(1), "1i"},
		{float64(1.1), "1.1"},
		{math.NaN(), ""},
		{math.Inf(1), ""},
		{float32(1), "1"},
		{float32(math.NaN()), ""},
		{float32(math.Inf(1)), ""},
		{"a", `"a"`},
		{[]byte("aaa"), "YWFh"},
		{true, "true"},
		{false, "false"},
		{epochTime, "0"},
		{time.Duration(100), "100i"},
		{struct{}{}, ""},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			val, err := appendProtocolValue(nil, test.value)
			if err != nil && test.expect != "" {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, string(val))
		})
	}
}

// Test_AppendConverted tests appendConverted function
func Test_AppendConverted(t *testing.T) {
	var tests = []struct {
		dataType string
		value    string
		expect   string
	}{
		{"", "1", "1"},
		{"long", "a", ""},
		{"dateTime", "a", ""},
		{"dateTime:number", "a", ""},
		{"string", "a", `"a"`},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			column := &CsvTableColumn{Label: "test"}
			column.setupDataType(test.dataType)
			val, err := appendConverted(nil, test.value, column, 1)
			if err != nil && test.expect != "" {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, string(val))
		})
	}
}

// Test_IsTypeSupported tests IsTypeSupported function
func Test_IsTypeSupported(t *testing.T) {
	require.True(t, IsTypeSupported(stringDatatype), true)
	require.True(t, IsTypeSupported(doubleDatatype), true)
	require.True(t, IsTypeSupported(boolDatatype), true)
	require.True(t, IsTypeSupported(longDatatype), true)
	require.True(t, IsTypeSupported(uLongDatatype), true)
	require.True(t, IsTypeSupported(durationDatatype), true)
	require.True(t, IsTypeSupported(base64BinaryDataType), true)
	require.True(t, IsTypeSupported(dateTimeDatatype), true)
	require.True(t, IsTypeSupported(""), true)
	require.False(t, IsTypeSupported(" "), false)
	// time format is not part of data type
	require.False(t, IsTypeSupported(dateTimeDatatype+":"+RFC3339))
	require.False(t, IsTypeSupported(dateTimeDatatype+":"+RFC3339Nano))
	require.False(t, IsTypeSupported(dateTimeDatatype+":"+dataFormatNumber))
}

// Test_NormalizeNumberString tests normalizeNumberString function
func Test_NormalizeNumberString(t *testing.T) {
	var tests = []struct {
		value          string
		format         string
		removeFraction bool
		expect         string
		warning        string
	}{
		{"123", "", true, "123", ""},
		{"123", ".", true, "123", ""},
		{"123.456", ".", true, "123", "::PREFIX::WARNING: line 1: column 'test': '123.456' truncated to '123' to fit into 'tst' data type\n"},
		{"123.456", ".", false, "123.456", ""},
		{"1 2.3,456", ",. ", false, "123.456", ""},
		{" 1 2\t3.456 \r\n", "", false, "123.456", ""},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			// customize logging to check warnings
			var buf bytes.Buffer
			log.SetOutput(&buf)
			oldFlags := log.Flags()
			log.SetFlags(0)
			oldPrefix := log.Prefix()
			prefix := "::PREFIX::"
			log.SetPrefix(prefix)
			defer func() {
				log.SetOutput(os.Stderr)
				log.SetFlags(oldFlags)
				log.SetPrefix(oldPrefix)
			}()

			require.Equal(t, test.expect,
				normalizeNumberString(test.value,
					&CsvTableColumn{Label: "test", DataType: "tst", DataFormat: test.format}, test.removeFraction, 1))
			require.Equal(t, test.warning, buf.String())
		})
	}
}

// Test_CreateDecoder tests CreateDecoder function
func Test_CreateDecoder(t *testing.T) {
	decoder, err := CreateDecoder("UTF-8")
	toUtf8 := func(in []byte) string {
		s, _ := ioutil.ReadAll(decoder(bytes.NewReader(in)))
		return string(s)
	}
	require.NotNil(t, decoder)
	require.Nil(t, err)
	require.Equal(t, "\u2318", toUtf8([]byte{226, 140, 152}))
	decoder, err = CreateDecoder("windows-1250")
	require.NotNil(t, decoder)
	require.Nil(t, err)
	require.Equal(t, "\u0160", toUtf8([]byte{0x8A}))
	decoder, err = CreateDecoder("whateveritis")
	require.NotNil(t, err)
	require.Nil(t, decoder)
	// we can have valid IANA names that are not supported by golang/x/text
	decoder, err = CreateDecoder("US-ASCII")
	log.Printf("US-ASCII encoding support: %v,%v", decoder != nil, err)
}

// Test_CreateBoolParseFn tests createBoolParseFn function
func Test_CreateBoolParseFn(t *testing.T) {
	type pairT struct {
		value  string
		expect string
	}
	var tests = []struct {
		format string
		pair   []pairT
	}{
		{"t,y,1:f,n,0", []pairT{
			{"y", "true"},
			{"0", "false"},
			{"T", "unsupported"},
		}},
		{"true", []pairT{
			{"true", "unsupported"},
			{"false", "unsupported"},
		}},
		{"true:", []pairT{
			{"true", "true"},
			{"other", "false"},
		}},
		{":false", []pairT{
			{"false", "false"},
			{"other", "true"},
		}},
	}

	for i, test := range tests {
		fn := createBoolParseFn(test.format)
		for j, pair := range test.pair {
			t.Run(fmt.Sprint(i)+"_"+fmt.Sprint(j), func(t *testing.T) {
				result, err := fn(pair.value, 1)
				switch pair.expect {
				case "true":
					require.Equal(t, true, result)
				case "false":
					require.Equal(t, false, result)
				default:
					require.NotNil(t, err)
					require.True(t, strings.Contains(fmt.Sprintf("%v", err), pair.expect))
				}
			})
		}
	}
}
