package write

import (
	"fmt"
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

func splitTypeAndFormat(val string) (dataType string, dataFormat string) {
	colonIndex := strings.Index(val, ":")
	if colonIndex > 1 {
		dataType = val[:colonIndex]
		dataFormat = val[colonIndex+1:]
	} else {
		dataType = val
	}
	return dataType, dataFormat
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
			dataType, dataFormat := splitTypeAndFormat(test.dataType)
			val, err := toTypedValue(test.value, dataType, dataFormat)
			if err != nil && test.expect != nil {
				require.Nil(t, err.Error())
			}
			require.Equal(t, test.expect, val)
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
			dataType, dataFormat := splitTypeAndFormat(test.dataType)
			val, err := appendConverted(nil, test.value, &CsvTableColumn{DataType: dataType, DataFormat: dataFormat})
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

// Test_escapeMeasurement
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
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			require.Equal(t, test.expect, normalizeNumberString(test.value, test.format, test.removeFraction))
		})
	}
}
