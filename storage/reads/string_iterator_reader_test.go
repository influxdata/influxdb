package reads_test

import (
	"io"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type mockStringIterator struct {
	values    []string
	nextValue *string
	stats     cursors.CursorStats
}

func newMockStringIterator(scannedValues, scannedBytes int, values ...string) *mockStringIterator {
	return &mockStringIterator{
		values: values,
		stats: cursors.CursorStats{
			ScannedValues: scannedValues,
			ScannedBytes:  scannedBytes,
		},
	}
}

func (si *mockStringIterator) Next() bool {
	if len(si.values) > 0 {
		si.nextValue = &si.values[0]
		si.values = si.values[1:]
		return true
	}
	si.nextValue = nil
	return false
}

func (si *mockStringIterator) Value() string {
	if si.nextValue != nil {
		return *si.nextValue
	}

	// Better than panic.
	return ""
}

func (si *mockStringIterator) Stats() cursors.CursorStats {
	if len(si.values) > 0 {
		return cursors.CursorStats{}
	}
	return si.stats
}

type mockStringValuesStreamReader struct {
	responses []*datatypes.StringValuesResponse
}

func newMockStringValuesStreamReader(responseValuess ...[]string) *mockStringValuesStreamReader {
	responses := make([]*datatypes.StringValuesResponse, len(responseValuess))
	for i := range responseValuess {
		responses[i] = &datatypes.StringValuesResponse{
			Values: make([][]byte, len(responseValuess[i])),
		}
		for j := range responseValuess[i] {
			responses[i].Values[j] = []byte(responseValuess[i][j])
		}
	}
	return &mockStringValuesStreamReader{
		responses: responses,
	}
}

func (r *mockStringValuesStreamReader) Recv() (*datatypes.StringValuesResponse, error) {
	if len(r.responses) > 0 {
		tr := r.responses[0]
		r.responses = r.responses[1:]
		return tr, nil
	}

	return nil, io.EOF
}

func TestStringIteratorStreamReader(t *testing.T) {
	tests := []struct {
		name             string
		responseValuess  [][]string // []string is the values from one response
		expectReadValues []string
	}{
		{
			name:             "simple",
			responseValuess:  [][]string{{"foo", "bar"}},
			expectReadValues: []string{"foo", "bar"},
		},
		{
			name:             "no deduplication expected",
			responseValuess:  [][]string{{"foo", "bar", "bar"}, {"foo"}},
			expectReadValues: []string{"foo", "bar", "bar", "foo"},
		},
		{
			name:             "not as simple",
			responseValuess:  [][]string{{"foo", "bar", "baz"}, {"qux"}, {"more"}},
			expectReadValues: []string{"foo", "bar", "baz", "qux", "more"},
		},
	}

	for _, tt := range tests {
		stream := newMockStringValuesStreamReader(tt.responseValuess...)
		r := reads.NewStringIteratorStreamReader(stream)

		var got []string
		for r.Next() {
			got = append(got, r.Value())
		}

		if !reflect.DeepEqual(tt.expectReadValues, got) {
			t.Errorf("expected %v got %v", tt.expectReadValues, got)
		}
	}
}
