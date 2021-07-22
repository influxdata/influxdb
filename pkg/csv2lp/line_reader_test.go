package csv2lp_test

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/influxdata/influxdb/v2/pkg/csv2lp"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	"github.com/stretchr/testify/require"
)

// TestLineReader tests correctness of line reporting and reader implementation of LineReader
func TestLineReader(t *testing.T) {
	type TestInput = struct {
		lines               [4]string
		withDataErrorReader bool
	}
	tests := []TestInput{
		{
			lines:               [4]string{"a\n", "\n", "\n", "bcxy"},
			withDataErrorReader: false,
		}, {
			lines:               [4]string{"a\n", "\n", "\n", "bcxy"},
			withDataErrorReader: true,
		}, {
			lines:               [4]string{"a\n", "\n", "\n", "bcx\n"},
			withDataErrorReader: false,
		}, {
			lines:               [4]string{"a\n", "\n", "\n", "bcx\n"},
			withDataErrorReader: true,
		}}
	for _, test := range tests {
		lines := test.lines
		input := strings.Join(lines[:], "")
		withDataErrorReader := test.withDataErrorReader
		t.Run(fmt.Sprintf("%s withDataErrorReader=%v", input, withDataErrorReader), func(t *testing.T) {
			var reader io.Reader = strings.NewReader(input)
			if withDataErrorReader {
				// ensures that the reader reports the last EOF error also with data
				reader = iotest.DataErrReader(reader)
			}
			lineReader := csv2lp.NewLineReaderSize(reader, 2)
			var buf []byte = make([]byte, 4)
			var err error
			var read int

			// patologic case: reading from empty buffer returns 0 without an error
			read, err = lineReader.Read(buf[0:0])
			require.Equal(t, 0, read)
			require.Nil(t, err)
			require.Equal(t, 0, lineReader.LastLineNumber)

			// 1st line
			read, err = lineReader.Read(buf)
			require.Equal(t, []byte(lines[0]), buf[0:read])
			require.Nil(t, err)
			require.Equal(t, 0, lineReader.LastLineNumber)

			// 2nd
			read, err = lineReader.Read(buf)
			require.Equal(t, []byte(lines[1]), buf[0:read])
			require.Nil(t, err)
			require.Equal(t, 1, lineReader.LastLineNumber)

			// reading into empty does not change the game
			read, err = lineReader.Read(buf[0:0])
			require.Equal(t, 0, read)
			require.Nil(t, err)
			require.Equal(t, 1, lineReader.LastLineNumber)

			// 3rd
			read, err = lineReader.Read(buf)
			require.Equal(t, []byte(lines[2]), buf[0:read])
			require.Nil(t, err)
			require.Equal(t, 2, lineReader.LastLineNumber)

			// 4th line cannot be fully read, because buffer size is 2
			read, err = lineReader.Read(buf)
			require.Equal(t, []byte(lines[3][:2]), buf[0:read])
			require.Nil(t, err)
			require.Equal(t, 3, lineReader.LastLineNumber)
			read, err = lineReader.Read(buf)
			require.Equal(t, []byte(lines[3][2:]), buf[0:read])
			require.Nil(t, err)
			require.Equal(t, 3, lineReader.LastLineNumber)

			// 5th line => error
			_, err = lineReader.Read(buf)
			require.NotNil(t, err)
		})
	}
}

// TestLineReader_Read_BufferOverflow ensures calling Read into
// a slice does not panic. Fixes https://github.com/influxdata/influxdb/issues/19586
func TestLineReader_Read_BufferOverflow(t *testing.T) {
	sr := strings.NewReader("foo\nbar")
	rd := csv2lp.NewLineReader(sr)
	buf := make([]byte, 2)

	n, err := rd.Read(buf)
	assert.Equal(t, n, 2)
	assert.NoError(t, err)
}

// TestLineReader_viaCsv tests correct line reporting when read through a CSV reader with various buffer sizes
// to emulate multiple required reads with a small test data set
func TestLineReader_viaCsv(t *testing.T) {
	type RowWithLine = struct {
		row        []string
		lineNumber int
	}

	input := "a\n\nb\n\nc"
	expected := []RowWithLine{
		{[]string{"a"}, 1},
		{[]string{"b"}, 3},
		{[]string{"c"}, 5},
	}

	bufferSizes := []int{-1, 0, 2, 50}
	for _, bufferSize := range bufferSizes {
		t.Run(fmt.Sprintf("buffer size: %d", bufferSize), func(t *testing.T) {
			var lineReader *csv2lp.LineReader
			if bufferSize < 0 {
				lineReader = csv2lp.NewLineReader(strings.NewReader(input))
			} else {
				lineReader = csv2lp.NewLineReaderSize(strings.NewReader(input), bufferSize)
			}
			lineReader.LineNumber = 1 // start with 1
			csvReader := csv.NewReader(lineReader)

			results := make([]RowWithLine, 0, 3)
			for {
				row, _ := csvReader.Read()
				lineNumber := lineReader.LastLineNumber
				if row == nil {
					break
				}
				results = append(results, RowWithLine{row, lineNumber})
			}
			require.Equal(t, expected, results)
		})
	}
}
