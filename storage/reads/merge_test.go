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

func TestNewSequenceResultSet(t *testing.T) {
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
			exp: `series: _m=m0,tag0=val01
  cursor:Float
series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
`,
		},
		{
			name: "sequential",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00", "m0,tag0=val01"),
				newStreamSeries("m0,tag0=val02", "m0,tag0=val03"),
			},
			exp: `series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val01
  cursor:Float
series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
`,
		},
		{
			name: "single resultset",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00", "m0,tag0=val01", "m0,tag0=val02", "m0,tag0=val03"),
			},
			exp: `series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val01
  cursor:Float
series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
`,
		},
		{
			name: "single series ordered",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val00"),
				newStreamSeries("m0,tag0=val01"),
				newStreamSeries("m0,tag0=val02"),
				newStreamSeries("m0,tag0=val03"),
			},
			exp: `series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val01
  cursor:Float
series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
`,
		},
		{
			name: "single series random order",
			streams: []*sliceStreamReader{
				newStreamSeries("m0,tag0=val02"),
				newStreamSeries("m0,tag0=val03"),
				newStreamSeries("m0,tag0=val00"),
				newStreamSeries("m0,tag0=val01"),
			},
			exp: `series: _m=m0,tag0=val02
  cursor:Float
series: _m=m0,tag0=val03
  cursor:Float
series: _m=m0,tag0=val00
  cursor:Float
series: _m=m0,tag0=val01
  cursor:Float
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rss := make([]reads.ResultSet, len(tt.streams))
			for i := range tt.streams {
				rss[i] = reads.NewResultSetStreamReader(tt.streams[i])
			}

			rs := reads.NewSequenceResultSet(rss)
			sb := new(strings.Builder)
			ResultSetToString(sb, rs)

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
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
				newMockStringIterator(1, 2, "bar", "foo"),
			},
			expectedValues: []string{"bar", "foo"},
		},
		{
			name: "duplicates",
			iterators: []cursors.StringIterator{
				newMockStringIterator(1, 10, "c"),
				newMockStringIterator(10, 100, "b", "b"), // This kind of duplication is not explicitly documented, but works.
				newMockStringIterator(1, 10, "a", "c"),
				newMockStringIterator(1, 10, "b", "d"),
				newMockStringIterator(1, 10, "0", "a", "b", "e"),
			},
			expectedValues: []string{"0", "a", "b", "c", "d", "e"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := reads.NewMergedStringIterator(tt.iterators)

			// Expect no stats before any iteration
			var expectStats cursors.CursorStats
			if !reflect.DeepEqual(expectStats, m.Stats()) {
				t.Errorf("expected %+v, got %+v", expectStats, m.Stats())
			}

			var gotValues []string
			for m.Next() {
				gotValues = append(gotValues, m.Value())
			}
			if !reflect.DeepEqual(tt.expectedValues, gotValues) {
				t.Errorf("expected %v, got %v", tt.expectedValues, gotValues)
			}
			for _, iterator := range tt.iterators {
				expectStats.Add(iterator.Stats())
			}
			if !reflect.DeepEqual(expectStats, m.Stats()) {
				t.Errorf("expected %+v, got %+v", expectStats, m.Stats())
			}
		})
	}
}
