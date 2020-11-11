package csv2lp

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func annotation(name string) annotationComment {
	for _, a := range supportedAnnotations {
		if a.prefix == name {
			return a
		}
	}
	panic("no annotation named " + name + " found!")
}

// Test_GroupAnnotation tests #group annotation
func Test_GroupAnnotation(t *testing.T) {
	subject := annotation("#group")
	require.True(t, subject.matches("#Group"))
	require.False(t, subject.isTableAnnotation())
	var tests = []struct {
		value  string
		expect int
	}{
		{"#group true", linePartTag},
		{"#group false", 0},
		{"false", 0},
		{"unknown", 0},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			col := &CsvTableColumn{}
			subject.setupColumn(col, test.value)
			require.Equal(t, test.expect, col.LinePart)
		})
	}
}

// Test_DefaultAnnotation tests #default annotation
func Test_DefaultAnnotation(t *testing.T) {
	subject := annotation("#default")
	require.True(t, subject.matches("#Default"))
	require.False(t, subject.isTableAnnotation())
	var tests = []struct {
		value  string
		expect string
	}{
		{"#default 1", "1"},
		{"#default ", ""},
		{"whatever", "whatever"},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			col := &CsvTableColumn{}
			subject.setupColumn(col, test.value)
			require.Equal(t, test.expect, col.DefaultValue)
		})
	}
}

// Test_DatatypeAnnotation tests #datatype annotation
func Test_DatatypeAnnotation(t *testing.T) {
	subject := annotation("#datatype")
	require.True(t, subject.matches("#dataType"))
	require.False(t, subject.isTableAnnotation())
	var tests = []struct {
		value          string
		expectType     string
		expectFormat   string
		expectLinePart int
	}{
		{"#datatype long", "long", "", 0},
		{"#datatype ", "", "", 0},
		{"#datatype measurement", "_", "", linePartMeasurement},
		{"#datatype tag", "_", "", linePartTag},
		{"#datatype field", "", "", linePartField},
		{"dateTime", "dateTime", "", linePartTime},
		{"dateTime:RFC3339", "dateTime", "RFC3339", linePartTime},
		{"#datatype dateTime:RFC3339", "dateTime", "RFC3339", linePartTime},
		{"whatever:format", "whatever", "format", 0},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			col := &CsvTableColumn{}
			subject.setupColumn(col, test.value)
			if test.expectType != "_" {
				require.Equal(t, test.expectType, col.DataType)
			}
			require.Equal(t, test.expectFormat, col.DataFormat)
		})
	}
}

// Test_ConstantAnnotation tests #constant annotation
func Test_ConstantAnnotation(t *testing.T) {
	subject := annotation("#constant")
	require.True(t, subject.matches("#Constant"))
	require.True(t, subject.isTableAnnotation())
	var tests = []struct {
		value          []string
		expectLabel    string
		expectDefault  string
		expectLinePart int
	}{
		{[]string{"#constant "}, "", "", 0}, // means literally nothing
		{[]string{"#constant measurement", "a"}, "_", "a", linePartMeasurement},
		{[]string{"#constant measurement", "a", "b"}, "_", "b", linePartMeasurement},
		{[]string{"#constant measurement", "a", ""}, "_", "a", linePartMeasurement},
		{[]string{"#constant tag", "tgName", "tgValue"}, "tgName", "tgValue", linePartTag},
		{[]string{"#constant", "tag", "tgName", "tgValue"}, "tgName", "tgValue", linePartTag},
		{[]string{"#constant field", "fName", "fVal"}, "fName", "fVal", linePartField},
		{[]string{"#constant", "field", "fName", "fVal"}, "fName", "fVal", linePartField},
		{[]string{"dateTime", "1"}, "_", "1", linePartTime},
		{[]string{"dateTime", "1", "2"}, "_", "2", linePartTime},
		{[]string{"dateTime", "", "2"}, "_", "2", linePartTime},
		{[]string{"dateTime", "3", ""}, "_", "3", linePartTime},
		{[]string{"long", "fN", "fV"}, "fN", "fV", 0},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			table := &CsvTable{}
			subject.setupTable(table, test.value)
			require.Equal(t, 1, len(table.extraColumns))
			col := table.extraColumns[0]
			require.Equal(t, test.expectLinePart, col.LinePart)
			require.Greater(t, 0, col.Index)
			if test.expectLabel != "_" {
				require.Equal(t, test.expectLabel, col.Label)
			} else {
				require.NotEqual(t, "", col.Label)
			}
			require.Equal(t, test.expectDefault, col.DefaultValue)
		})
	}
}

// Test_ConcatAnnotation tests #concat annotation
func Test_ConcatAnnotation(t *testing.T) {
	subject := annotation("#concat")
	require.True(t, subject.matches("#Concat"))
	require.True(t, subject.isTableAnnotation())
	var tests = []struct {
		value          []string
		expectLabel    string
		expectValue    string
		expectLinePart int
	}{
		// all possible specifications
		{[]string{"#concat "}, "", "", 0}, // means literally nothing
		{[]string{"#concat measurement", "a"}, "_", "a", linePartMeasurement},
		{[]string{"#concat measurement", "a", "b"}, "_", "b", linePartMeasurement},
		{[]string{"#concat measurement", "a", ""}, "_", "a", linePartMeasurement},
		{[]string{"#concat tag", "tgName", "tgValue"}, "tgName", "tgValue", linePartTag},
		{[]string{"#concat", "tag", "tgName", "tgValue"}, "tgName", "tgValue", linePartTag},
		{[]string{"#concat field", "fName", "fVal"}, "fName", "fVal", linePartField},
		{[]string{"#concat", "field", "fName", "fVal"}, "fName", "fVal", linePartField},
		{[]string{"dateTime", "1"}, "_", "1", linePartTime},
		{[]string{"dateTime", "1", "2"}, "_", "2", linePartTime},
		{[]string{"dateTime", "", "2"}, "_", "2", linePartTime},
		{[]string{"dateTime", "3", ""}, "_", "3", linePartTime},
		{[]string{"long", "fN", "fV"}, "fN", "fV", 0},
		// concat values
		{[]string{"string", "fN", "$-${b}-${a}"}, "fN", "$-2-1", 0},
	}
	exampleRow := []string{"1", "2"}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			table := &CsvTable{columns: []*CsvTableColumn{
				{Label: "a", Index: 0},
				{Label: "b", Index: 1},
			}}
			subject.setupTable(table, test.value)
			// validator
			require.Equal(t, 1, len(table.validators))
			require.Equal(t, table.validators[0](table), nil)
			// columns
			require.Equal(t, 1, len(table.extraColumns))
			col := table.extraColumns[0]
			require.Equal(t, test.expectLinePart, col.LinePart)
			require.Greater(t, 0, col.Index)
			if test.expectLabel != "_" {
				require.Equal(t, test.expectLabel, col.Label)
			} else {
				require.NotEqual(t, "", col.Label)
			}
			require.Equal(t, test.expectValue, col.Value(exampleRow))
		})
	}
	t.Run("concat template references unknown column", func(t *testing.T) {
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

		table := &CsvTable{columns: []*CsvTableColumn{
			{Label: "x", Index: 0},
		}}
		subject.setupTable(table, []string{"string", "fN", "a${y}-${x}z"})
		require.Equal(t, 1, len(table.validators))
		require.NotNil(t, table.validators[0](table))
		require.Equal(t,
			"column 'fN': 'a${y}-${x}z' references an unknown column 'y', available columns are: x",
			table.validators[0](table).Error())
		// columns
		require.Equal(t, 1, len(table.extraColumns))
		col := table.extraColumns[0]
		require.Greater(t, 0, col.Index)
		require.Equal(t, "a-1z", col.Value(exampleRow))
		// a warning is printed to console
		require.Equal(t,
			"::PREFIX::WARNING: column fN: column 'y' cannot be replaced, no such column available",
			strings.TrimSpace(buf.String()))
	})
}

// Test_TimeZoneAnnotation tests #timezone annotation
func Test_TimeZoneAnnotation(t *testing.T) {
	subject := annotation("#timezone")
	require.True(t, subject.matches("#timeZone"))
	require.True(t, subject.isTableAnnotation())
	var tests = []struct {
		value string
		err   string
	}{
		{"#timezone ", ""},
		{"#timezone EST", ""},
		{"#timezone,EST", ""},
		{"#timezone,+0100", ""},
		{"#timezone,whatever", "#timezone annotation"},
	}
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			table := &CsvTable{}
			err := subject.setupTable(table, strings.Split(test.value, ","))
			if test.err == "" {
				require.Nil(t, err)
				require.NotNil(t, table.timeZone != nil)
			} else {
				require.NotNil(t, err)
				require.True(t, strings.Contains(fmt.Sprintf("%v", err), test.err))
			}
		})
	}
}

// Test_ParseTimeZone tests parseTimeZone fn
func Test_ParseTimeZone(t *testing.T) {
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
