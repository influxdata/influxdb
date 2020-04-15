package csv2lp

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_escapeMeasurement
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

// Test_escapeTag
func Test_escapeTag(t *testing.T) {
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

// Test_escapeString
func Test_escapeString(t *testing.T) {
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

// Test_toTypedValue
func Test_toTypedValue(t *testing.T) {
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
			column := &CsvTableColumn{}
			setupDataType(column, test.dataType)
			val, err := toTypedValue(test.value, column)
			if err != nil && test.expect != nil {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, val)
		})
	}
}

// Test_toTypedValue_dateTime
func Test_toTypedValue_dateTimeCustomTimeZone(t *testing.T) {
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
			setupDataType(column, test.dataType)
			val, err := toTypedValue(test.value, column)
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

// Test_toLineProtocolValue
func Test_toLineProtocolValue(t *testing.T) {
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

// Test_convert
func Test_appendConverted(t *testing.T) {
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
			column := &CsvTableColumn{}
			setupDataType(column, test.dataType)
			val, err := appendConverted(nil, test.value, column)
			if err != nil && test.expect != "" {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, string(val))
		})
	}
}

// Test_IsTypeSupported
func Test_IsTypeSupported(t *testing.T) {
	require.Equal(t, IsTypeSupported(stringDatatype), true)
	require.Equal(t, IsTypeSupported(doubleDatatype), true)
	require.Equal(t, IsTypeSupported(boolDatatype), true)
	require.Equal(t, IsTypeSupported(longDatatype), true)
	require.Equal(t, IsTypeSupported(uLongDatatype), true)
	require.Equal(t, IsTypeSupported(durationDatatype), true)
	require.Equal(t, IsTypeSupported(base64BinaryDataType), true)
	require.Equal(t, IsTypeSupported(dateTimeDatatype), true)
	require.Equal(t, IsTypeSupported(""), true)
	require.Equal(t, IsTypeSupported(" "), false)
	// time format is not part of data type
	require.Equal(t, IsTypeSupported(dateTimeDatatype+":"+dateTimeDataFormatRFC3339), false)
	require.Equal(t, IsTypeSupported(dateTimeDatatype+":"+dateTimeDataFormatRFC3339Nano), false)
	require.Equal(t, IsTypeSupported(dateTimeDatatype+":"+dateTimeDataFormatNumber), false)
}

// Test_normalizeNumberString
func Test_normalizeNumberString(t *testing.T) {
	var tests = []struct {
		value          string
		format         string
		removeFraction bool
		expect         string
	}{
		{"123", "", true, "123"},
		{"123", ".", true, "123"},
		{"123.456", ".", true, "123"},
		{"123.456", ".", false, "123.456"},
		{"1 2.3,456", ",. ", false, "123.456"},
		{" 1 2\t3.456 \r\n", "", false, "123.456"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, test.expect, normalizeNumberString(test.value, test.format, test.removeFraction))
		})
	}
}

// TestCreateDecoder tests the decoding reader factory
func TestCreateDecoder(t *testing.T) {
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
	// we have valid IANA names that are not supported by the golang/x/text
	decoder, err = CreateDecoder("US-ASCII")
	log.Printf("US-ASCII encoding support: %v,%v", decoder != nil, err)
}

func Test_parseTimeZone(t *testing.T) {
	now := time.Now()
	_, localOffset := now.Zone()
	var tests = []struct {
		value  string
		offset int
	}{
		{"local", localOffset},
		{"Local", localOffset},
		{"-0000", 0},
		{"+0000", 0},
		{"-0100", -3600},
		{"+0100", 3600},
		{"+0130", 3600 + 3600/2},
		{"", 0},
		{"-01", -1},
		{"0000", -1},
		{"UTC", 0},
		{"EST", -5 * 3600},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			tz, err := parseTimeZone(test.value)
			require.NotEqual(t, tz, err) // both cannot be nil
			if err != nil {
				require.Nil(t, tz)
				// fmt.Println(err)
				if test.offset >= 0 {
					require.Fail(t, "offset expected")
				}
				return
			}
			require.NotNil(t, tz)
			testDate := fmt.Sprintf("%d-%02d-%02d", now.Year(), now.Month(), now.Day())
			result, err := time.ParseInLocation("2006-01-02", testDate, tz)
			require.Nil(t, err)
			_, offset := result.Zone()
			require.Equal(t, test.offset, offset)
		})
	}
}

func Test_createBoolParseFn(t *testing.T) {
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
				result, err := fn(pair.value)
				switch pair.expect {
				case "true":
					require.Equal(t, result, true)
				case "false":
					require.Equal(t, result, false)
				default:
					require.NotNil(t, err)
					require.True(t, strings.Contains(fmt.Sprintf("%v", err), pair.expect))
				}
			})
		}
	}
}
