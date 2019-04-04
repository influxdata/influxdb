package reads_test

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
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
