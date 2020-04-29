package csv2lp

import (
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// simulates the reader that returns all data together with EOF
type readOnceWithEOF struct {
	reader io.Reader
}

func (r *readOnceWithEOF) Read(p []byte) (n int, err error) {
	n, _ = r.reader.Read(p)
	return n, io.EOF
}

// Test_SkipHeaderLines checks that first lines are skipped
func Test_SkipHeaderLines(t *testing.T) {

	var tests = []struct {
		skipCount int
		input     string
		result    string
	}{
		{
			10,
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
			"",
		},
		{
			0,
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
		},
		{
			1,
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
			"2\n3\n4\n5\n6\n7\n8\n9\n0\n",
		},
		{
			5,
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
			"6\n7\n8\n9\n0\n",
		},
		{
			20,
			"1\n2\n3\n4\n5\n6\n7\n8\n9\n0\n",
			"",
		},
		{
			1,
			"\"\n\"\"\n\"\n2",
			"2",
		},
	}

	for i, test := range tests {
		input := test.input
		bufferSizes := []int{1, 2, 7, 0, len(input), len(input) + 1}
		for _, bufferSize := range bufferSizes {
			t.Run(strconv.Itoa(i)+"_"+strconv.Itoa(bufferSize), func(t *testing.T) {
				var reader io.Reader
				if bufferSize == 0 {
					// emulate a reader that returns EOF together with data
					bufferSize = len(input)
					reader = SkipHeaderLinesReader(test.skipCount, &readOnceWithEOF{strings.NewReader(input)})
				} else {
					reader = SkipHeaderLinesReader(test.skipCount, strings.NewReader(input))
				}
				buffer := make([]byte, bufferSize)
				result := make([]byte, 0, 100)
				for {
					n, err := reader.Read(buffer)
					if n > 0 {
						result = append(result, buffer[:n]...)
					}
					if err != nil {
						if err != io.EOF {
							require.Nil(t, err.Error())
						}
						break
					}
				}
				require.Equal(t, test.result, string(result))
			})
		}
	}
}
