package httpd

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestTruncatedReader_Read(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		n    int64
		err  error
	}{
		{"in(1000)-max(1000)", make([]byte, 1000), 1000, nil},
		{"in(1000)-max(1001)", make([]byte, 1000), 1001, nil},
		{"in(1001)-max(1000)", make([]byte, 1001), 1000, errTruncated},
		{"in(10000)-max(1000)", make([]byte, 1e5), 1e3, errTruncated},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := truncateReader(bytes.NewReader(tc.in), tc.n)
			_, err := ioutil.ReadAll(b)
			if err != tc.err {
				t.Errorf("unexpected error; got=%v, exp=%v", err, tc.err)
			}
		})
	}
}
