package reads_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func newGroupNoneStreamSeries(tagKeys string, v ...string) *sliceStreamReader {
	var frames []datatypes.ReadResponse_Frame
	frames = append(frames, groupF(tagKeys, ""))
	for _, s := range v {
		frames = append(frames, seriesF(Float, s))
	}
	return newStreamReader(response(frames...))
}

func TestNewGroupNoneMergedGroupResultSet(t *testing.T) {
	exp := `group:
  tag key      : m0,tag0,tag1,tag2
  partition key: 
    series: _m=m0,tag0=val00
      cursor:Float
    series: _m=m0,tag0=val01
      cursor:Float
    series: _m=m0,tag1=val10
      cursor:Float
    series: _m=m0,tag2=val20
      cursor:Float
`

	tests := []struct {
		name    string
		streams []*sliceStreamReader
		exp     string
	}{
		{
			name: "merge tagKey schemas  series total order",
			streams: []*sliceStreamReader{
				newGroupNoneStreamSeries("m0,tag0", "m0,tag0=val00", "m0,tag0=val01"),
				newGroupNoneStreamSeries("m0,tag1,tag2", "m0,tag1=val10", "m0,tag2=val20"),
			},
			exp: exp,
		},
		{
			name: "merge tagKey schemas  series mixed",
			streams: []*sliceStreamReader{
				newGroupNoneStreamSeries("m0,tag0,tag2", "m0,tag0=val01", "m0,tag2=val20"),
				newGroupNoneStreamSeries("m0,tag0,tag1", "m0,tag0=val00", "m0,tag1=val10"),
			},
			exp: exp,
		},
		{
			name: "merge single group schemas  ordered",
			streams: []*sliceStreamReader{
				newGroupNoneStreamSeries("m0,tag0", "m0,tag0=val00"),
				newGroupNoneStreamSeries("m0,tag0", "m0,tag0=val01"),
				newGroupNoneStreamSeries("m0,tag1", "m0,tag1=val10"),
				newGroupNoneStreamSeries("m0,tag2", "m0,tag2=val20"),
			},
			exp: exp,
		},
		{
			name: "merge single group schemas  unordered",
			streams: []*sliceStreamReader{
				newGroupNoneStreamSeries("m0,tag2", "m0,tag2=val20"),
				newGroupNoneStreamSeries("m0,tag0", "m0,tag0=val00"),
				newGroupNoneStreamSeries("m0,tag1", "m0,tag1=val10"),
				newGroupNoneStreamSeries("m0,tag0", "m0,tag0=val01"),
			},
			exp: exp,
		},
		{
			name: "merge single group",
			streams: []*sliceStreamReader{
				newGroupNoneStreamSeries("m0,tag0,tag1,tag2", "m0,tag0=val00", "m0,tag0=val01", "m0,tag1=val10", "m0,tag2=val20"),
			},
			exp: exp,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grss := make([]reads.GroupResultSet, len(tt.streams))
			for i := range tt.streams {
				grss[i] = reads.NewGroupResultSetStreamReader(tt.streams[i])
			}

			grs := reads.NewGroupNoneMergedGroupResultSet(grss)
			sb := new(strings.Builder)
			GroupResultSetToString(sb, grs)

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(strings.Split(got, "\n"), strings.Split(tt.exp, "\n")))
			}

			grs.Close()
		})
	}
}

func TestGroupNoneMergedGroupResultSet_ErrNoData(t *testing.T) {
	exp := "no data"
	streams := []reads.StreamReader{
		newGroupNoneStreamSeries("m0,tag2", "m0,tag2=val20"),
		errStreamReader(exp),
	}

	grss := make([]reads.GroupResultSet, len(streams))
	for i := range streams {
		grss[i] = reads.NewGroupResultSetStreamReader(streams[i])
	}

	grs := reads.NewGroupNoneMergedGroupResultSet(grss)
	if got := grs.Next(); got != nil {
		t.Errorf("expected nil")
	}

	if got, expErr := grs.Err(), errors.New(exp); !cmp.Equal(got, expErr, cmp.Comparer(errCmp)) {
		t.Errorf("unexpected error; -got/+exp\n%s", cmp.Diff(got, expErr, cmp.Transformer("err", errTr)))
	}
}

func TestGroupNoneMergedGroupResultSet_ErrStreamNoData(t *testing.T) {
	streams := []reads.StreamReader{
		newGroupNoneStreamSeries("m0,tag2", "m0,tag2=val20"),
		&emptyStreamReader{},
	}

	grss := make([]reads.GroupResultSet, len(streams))
	for i := range streams {
		grss[i] = reads.NewGroupResultSetStreamReader(streams[i])
	}

	grs := reads.NewGroupNoneMergedGroupResultSet(grss)
	if got := grs.Next(); got != nil {
		t.Errorf("expected nil")
	}

	if got, expErr := grs.Err(), reads.ErrStreamNoData; !cmp.Equal(got, expErr, cmp.Comparer(errCmp)) {
		t.Errorf("unexpected error; -got/+exp\n%s", cmp.Diff(got, expErr, cmp.Transformer("err", errTr)))
	}
}

func groupByF(tagKeys, parKeys string, v ...string) datatypes.ReadResponse {
	var frames []datatypes.ReadResponse_Frame
	frames = append(frames, groupF(tagKeys, parKeys))
	for _, s := range v {
		frames = append(frames, seriesF(Float, s))
	}
	return response(frames...)
}

func TestNewGroupByMergedGroupResultSet(t *testing.T) {
	exp := `group:
  tag key      : _m,tag0,tag1
  partition key: val00,<nil>
    series: _m=aaa,tag0=val00
      cursor:Float
    series: _m=cpu,tag0=val00,tag1=val10
      cursor:Float
    series: _m=cpu,tag0=val00,tag1=val11
      cursor:Float
    series: _m=cpu,tag0=val00,tag1=val12
      cursor:Float
group:
  tag key      : _m,tag0
  partition key: val01,<nil>
    series: _m=aaa,tag0=val01
      cursor:Float
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val20
    series: _m=mem,tag1=val10,tag2=val20
      cursor:Float
    series: _m=mem,tag1=val11,tag2=val20
      cursor:Float
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val21
    series: _m=mem,tag1=val11,tag2=val21
      cursor:Float
`
	tests := []struct {
		name    string
		streams []*sliceStreamReader
		exp     string
	}{
		{
			streams: []*sliceStreamReader{
				newStreamReader(
					groupByF("_m,tag0,tag1", "val00,<nil>", "aaa,tag0=val00", "cpu,tag0=val00,tag1=val11"),
					groupByF("_m,tag1,tag2", "<nil>,val20", "mem,tag1=val10,tag2=val20"),
					groupByF("_m,tag1,tag2", "<nil>,val21", "mem,tag1=val11,tag2=val21"),
				),
				newStreamReader(
					groupByF("_m,tag0,tag1", "val00,<nil>", "cpu,tag0=val00,tag1=val10", "cpu,tag0=val00,tag1=val12"),
					groupByF("_m,tag0", "val01,<nil>", "aaa,tag0=val01"),
				),
				newStreamReader(
					groupByF("_m,tag1,tag2", "<nil>,val20", "mem,tag1=val11,tag2=val20"),
				),
			},
			exp: exp,
		},
		{
			streams: []*sliceStreamReader{
				newStreamReader(
					groupByF("_m,tag1,tag2", "<nil>,val20", "mem,tag1=val10,tag2=val20"),
					groupByF("_m,tag1,tag2", "<nil>,val21", "mem,tag1=val11,tag2=val21"),
				),
				newStreamReader(
					groupByF("_m,tag1,tag2", "<nil>,val20", "mem,tag1=val11,tag2=val20"),
				),
				newStreamReader(
					groupByF("_m,tag0,tag1", "val00,<nil>", "cpu,tag0=val00,tag1=val10", "cpu,tag0=val00,tag1=val12"),
					groupByF("_m,tag0", "val01,<nil>", "aaa,tag0=val01"),
				),
				newStreamReader(
					groupByF("_m,tag0,tag1", "val00,<nil>", "aaa,tag0=val00", "cpu,tag0=val00,tag1=val11"),
				),
			},
			exp: exp,
		},
		{
			name: "does merge keys",
			streams: []*sliceStreamReader{
				newStreamReader(
					groupByF("_m,tag1", "val00,<nil>", "aaa,tag0=val00", "cpu,tag0=val00,tag1=val11"),
					groupByF("_m,tag2", "<nil>,val20", "mem,tag1=val10,tag2=val20"),
					groupByF("_m,tag1,tag2", "<nil>,val21", "mem,tag1=val11,tag2=val21"),
				),
				newStreamReader(
					groupByF("_m,tag0,tag1", "val00,<nil>", "cpu,tag0=val00,tag1=val10", "cpu,tag0=val00,tag1=val12"),
					groupByF("_m,tag0", "val01,<nil>", "aaa,tag0=val01"),
				),
				newStreamReader(
					groupByF("_m,tag1", "<nil>,val20", "mem,tag1=val11,tag2=val20"),
				),
			},
			exp: exp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grss := make([]reads.GroupResultSet, len(tt.streams))
			for i := range tt.streams {
				grss[i] = reads.NewGroupResultSetStreamReader(tt.streams[i])
			}

			grs := reads.NewGroupByMergedGroupResultSet(grss)
			sb := new(strings.Builder)
			GroupResultSetToString(sb, grs, SkipNilCursor())

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(strings.Split(got, "\n"), strings.Split(tt.exp, "\n")))
			}

			grs.Close()
		})
	}
}
