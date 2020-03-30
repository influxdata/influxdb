package write

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func annotation(name string) annotationComment {
	for _, a := range supportedAnnotations {
		if a.label == name {
			return a
		}
	}
	panic("no annotation named " + name + " found!")
}

// TestGroupAnnotation validates construction of table columns from Query CSV result
func TestGroupAnnotation(t *testing.T) {
	subject := annotation("#group")
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
func TestDefaultAnnotation(t *testing.T) {
	subject := annotation("#default")
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
func TestDatatypeAnnotation(t *testing.T) {
	subject := annotation("#datatype")
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
func TestConstantAnnotation(t *testing.T) {
	subject := annotation("#constant")
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
