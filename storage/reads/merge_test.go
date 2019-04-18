package reads_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func newStreamSeries(v ...string) *sliceStreamReader {
	var frames []datatypes.ReadResponse_Frame
	for _, s := range v {
		frames = append(frames, seriesF(Float, s))
	}
	return newStreamReader(response(frames...))
}

func TestNewMergedResultSet(t *testing.T) {
	exp := `series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val01
  cursor:Float
series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
`

	tests := []struct {
		name    string
		streams []*sliceStreamReader
		exp     string
	}{
		{
			name: "outer inner",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val01", "m0,tag0=val02"),
				newStreamSeries("m0,tag0=val00", "m0,tag0=val03"),
			},
			exp: exp,
		},
		{
			name: "sequential",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00", "m0,tag0=val01"),
				newStreamSeries("m0,tag0=val02", "m0,tag0=val03"),
			},
			exp: exp,
		},
		{
			name: "interleaved",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val01", "m0,tag0=val03"),
				newStreamSeries("m0,tag0=val00", "m0,tag0=val02"),
			},
			exp: exp,
		},
		{
			name: "single resultset",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00", "m0,tag0=val01", "m0,tag0=val02", "m0,tag0=val03"),
			},
			exp: exp,
		},
		{
			name: "single series ordered",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00"),
				newStreamSeries("m0,tag0=val01"),
				newStreamSeries("m0,tag0=val02"),
				newStreamSeries("m0,tag0=val03"),
			},
			exp: exp,
		},
		{
			name: "single series random order",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val02"),
				newStreamSeries("m0,tag0=val03"),
				newStreamSeries("m0,tag0=val00"),
				newStreamSeries("m0,tag0=val01"),
			},
			exp: exp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rss := make([]reads.ResultSet, len(tt.streams))
			for i := range tt.streams {
				rss[i] = reads.NewResultSetStreamReader(tt.streams[i])
			}

			rs := reads.NewMergedResultSet(rss)
			sb := new(strings.Builder)
			ResultSetToString(sb, rs)

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func TestNewMergedStringIterator(t *testing.T) {
	tests := []struct {
		name           string
		iterators      []cursors.StringIterator
		expectedValues []string
	}{
		{
			name: "simple",
			iterators: []cursors.StringIterator{
				newMockStringIterator("foo", "bar"),
			},
			expectedValues: []string{"foo", "bar"},
		},
		{
			name: "duplicates",
			iterators: []cursors.StringIterator{
				newMockStringIterator("foo"),
				newMockStringIterator("bar", "bar"),
				newMockStringIterator("foo"),
				newMockStringIterator("baz", "qux"),
			},
			expectedValues: []string{"foo", "bar", "baz", "qux"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := reads.NewMergedStringIterator(tt.iterators)
			var gotValues []string
			for m.Next() {
				gotValues = append(gotValues, m.Value())
			}
			if !reflect.DeepEqual(tt.expectedValues, gotValues) {
				t.Errorf("expected %v, got %v", tt.expectedValues, gotValues)
			}
		})
	}
}
