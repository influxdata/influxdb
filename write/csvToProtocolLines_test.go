package write

import (
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCsvData checks data that are writen in an annotated CSV file
func Test_CsvToProtocolLines(t *testing.T) {
	var tests = []struct {
		name  string
		csv   string
		lines string
		err   string
	}{
		{
			"simple1",
			"_measurement,a,b\ncpu,1,1\ncpu,b2\n",
			"cpu a=1,b=1\ncpu a=b2\n",
			"",
		},
		{
			"simple1_withSep",
			"sep=;\n_measurement;a;b\ncpu;1;1\ncpu;b2\n",
			"cpu a=1,b=1\ncpu a=b2\n",
			"",
		},
		{
			"simple2",
			"_measurement,a,b\ncpu,1,1\ncpu,\n",
			"",
			"no field data",
		},
		{
			"simple3",
			"_measurement,a,_time\ncpu,1,1\ncpu,2,invalidTime\n",
			"",
			"_time", // error in _time column
		},
		{
			"constant_annotations",
			"#constant,measurement,,cpu\n" +
				"#constant,tag,xpu,xpu1\n" +
				"#constant,tag,cpu,cpu1\n" +
				"#constant,long,of,100\n" +
				"#constant,dateTime,,2\n" +
				"x,y\n" +
				"1,2\n" +
				"3,4\n",
			"cpu,cpu=cpu1,xpu=xpu1 x=1,y=2,of=100i 2\n" +
				"cpu,cpu=cpu1,xpu=xpu1 x=3,y=4,of=100i 2\n",
			"", // no error
		},
	}
	bufferSizes := []int{40, 7, 3, 1}

	for _, test := range tests {
		for _, bufferSize := range bufferSizes {
			t.Run(test.name+"_"+strconv.Itoa(bufferSize), func(t *testing.T) {
				reader := CsvToProtocolLines(strings.NewReader(test.csv))
				buffer := make([]byte, bufferSize)
				lines := make([]byte, 0, 100)
				for {
					n, err := reader.Read(buffer)
					if err != nil {
						if err == io.EOF {
							break
						}
						if test.err != "" {
							// fmt.Println(err)
							if err := err.Error(); !strings.Contains(err, test.err) {
								require.Equal(t, err, test.err)
							}
							return
						}
						require.Nil(t, err.Error())
						break
					}
					lines = append(lines, buffer[:n]...)
				}
				if test.err == "" {
					require.Equal(t, test.lines, string(lines))
				} else {
					require.Fail(t, "error message with '"+test.err+"' expected")
				}
			})
		}
	}
}

// Test_CsvLineError checks formating of line errors
func Test_CsvLineError(t *testing.T) {
	var tests = []struct {
		err   CsvLineError
		value string
	}{
		{
			CsvLineError{Line: 1, Err: errors.New("cause")},
			"line 1: cause",
		},
		{
			CsvLineError{Line: 2, Err: CsvColumnError{"a", errors.New("cause")}},
			"line 2: column 'a': cause",
		},
	}
	for _, test := range tests {
		require.Equal(t, test.value, test.err.Error())
	}
}
