package reads_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func TestNewGroupResultSet_Sorting(t *testing.T) {
	tests := []struct {
		name  string
		cur   reads.SeriesCursor
		group datatypes.ReadGroupRequest_Group
		keys  []string
		exp   string
	}{
		{
			name: "group by tag1 in all series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"cpu,tag0=val00,tag1=val10",
					"cpu,tag0=val00,tag1=val11",
					"cpu,tag0=val00,tag1=val12",
					"cpu,tag0=val01,tag1=val10",
					"cpu,tag0=val01,tag1=val11",
					"cpu,tag0=val01,tag1=val12",
				)},
			group: datatypes.GroupBy,
			keys:  []string{"tag1"},
			exp: `group:
  tag key      : _m,tag0,tag1
  partition key: val10
    series: _m=cpu,tag0=val00,tag1=val10
    series: _m=cpu,tag0=val01,tag1=val10
group:
  tag key      : _m,tag0,tag1
  partition key: val11
    series: _m=cpu,tag0=val00,tag1=val11
    series: _m=cpu,tag0=val01,tag1=val11
group:
  tag key      : _m,tag0,tag1
  partition key: val12
    series: _m=cpu,tag0=val00,tag1=val12
    series: _m=cpu,tag0=val01,tag1=val12
`,
		},
		{
			name: "group by tags key collision",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"cpu,tag0=000,tag1=111",
					"cpu,tag0=00,tag1=0111",
					"cpu,tag0=0,tag1=00111",
					"cpu,tag0=0001,tag1=11",
					"cpu,tag0=00011,tag1=1",
				)},
			group: datatypes.GroupBy,
			keys:  []string{"tag0", "tag1"},
			exp: `group:
  tag key      : _m,tag0,tag1
  partition key: 0,00111
    series: _m=cpu,tag0=0,tag1=00111
group:
  tag key      : _m,tag0,tag1
  partition key: 00,0111
    series: _m=cpu,tag0=00,tag1=0111
group:
  tag key      : _m,tag0,tag1
  partition key: 000,111
    series: _m=cpu,tag0=000,tag1=111
group:
  tag key      : _m,tag0,tag1
  partition key: 0001,11
    series: _m=cpu,tag0=0001,tag1=11
group:
  tag key      : _m,tag0,tag1
  partition key: 00011,1
    series: _m=cpu,tag0=00011,tag1=1
`,
		},
		{
			name: "group by tag1 in partial series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"aaa,tag0=val00",
					"aaa,tag0=val01",
					"cpu,tag0=val00,tag1=val10",
					"cpu,tag0=val00,tag1=val11",
					"cpu,tag0=val00,tag1=val12",
					"cpu,tag0=val01,tag1=val10",
					"cpu,tag0=val01,tag1=val11",
					"cpu,tag0=val01,tag1=val12",
				)},
			group: datatypes.GroupBy,
			keys:  []string{"tag1"},
			exp: `group:
  tag key      : _m,tag0,tag1
  partition key: val10
    series: _m=cpu,tag0=val00,tag1=val10
    series: _m=cpu,tag0=val01,tag1=val10
group:
  tag key      : _m,tag0,tag1
  partition key: val11
    series: _m=cpu,tag0=val01,tag1=val11
    series: _m=cpu,tag0=val00,tag1=val11
group:
  tag key      : _m,tag0,tag1
  partition key: val12
    series: _m=cpu,tag0=val01,tag1=val12
    series: _m=cpu,tag0=val00,tag1=val12
group:
  tag key      : _m,tag0
  partition key: <nil>
    series: _m=aaa,tag0=val00
    series: _m=aaa,tag0=val01
`,
		},
		{
			name: "group by tag2,tag1 with partial series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"aaa,tag0=val00",
					"aaa,tag0=val01",
					"cpu,tag0=val00,tag1=val10",
					"cpu,tag0=val00,tag1=val11",
					"cpu,tag0=val00,tag1=val12",
					"mem,tag1=val10,tag2=val20",
					"mem,tag1=val11,tag2=val20",
					"mem,tag1=val11,tag2=val21",
				)},
			group: datatypes.GroupBy,
			keys:  []string{"tag2", "tag1"},
			exp: `group:
  tag key      : _m,tag1,tag2
  partition key: val20,val10
    series: _m=mem,tag1=val10,tag2=val20
group:
  tag key      : _m,tag1,tag2
  partition key: val20,val11
    series: _m=mem,tag1=val11,tag2=val20
group:
  tag key      : _m,tag1,tag2
  partition key: val21,val11
    series: _m=mem,tag1=val11,tag2=val21
group:
  tag key      : _m,tag0,tag1
  partition key: <nil>,val10
    series: _m=cpu,tag0=val00,tag1=val10
group:
  tag key      : _m,tag0,tag1
  partition key: <nil>,val11
    series: _m=cpu,tag0=val00,tag1=val11
group:
  tag key      : _m,tag0,tag1
  partition key: <nil>,val12
    series: _m=cpu,tag0=val00,tag1=val12
group:
  tag key      : _m,tag0
  partition key: <nil>,<nil>
    series: _m=aaa,tag0=val00
    series: _m=aaa,tag0=val01
`,
		},
		{
			name: "group by tag0,tag2 with partial series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"aaa,tag0=val00",
					"aaa,tag0=val01",
					"cpu,tag0=val00,tag1=val10",
					"cpu,tag0=val00,tag1=val11",
					"cpu,tag0=val00,tag1=val12",
					"mem,tag1=val10,tag2=val20",
					"mem,tag1=val11,tag2=val20",
					"mem,tag1=val11,tag2=val21",
				)},
			group: datatypes.GroupBy,
			keys:  []string{"tag0", "tag2"},
			exp: `group:
  tag key      : _m,tag0,tag1
  partition key: val00,<nil>
    series: _m=aaa,tag0=val00
    series: _m=cpu,tag0=val00,tag1=val10
    series: _m=cpu,tag0=val00,tag1=val11
    series: _m=cpu,tag0=val00,tag1=val12
group:
  tag key      : _m,tag0
  partition key: val01,<nil>
    series: _m=aaa,tag0=val01
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val20
    series: _m=mem,tag1=val10,tag2=val20
    series: _m=mem,tag1=val11,tag2=val20
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val21
    series: _m=mem,tag1=val11,tag2=val21
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			newCursor := func() (reads.SeriesCursor, error) {
				return tt.cur, nil
			}

			var hints datatypes.HintFlags
			hints.SetHintSchemaAllTime()
			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{
				Group:     tt.group,
				GroupKeys: tt.keys,
				// TODO(jlapacik):
				//     Hints is not used except for the tests in this file.
				//     Eventually this field should be removed entirely.
				Hints: hints,
			}, newCursor)

			sb := new(strings.Builder)
			GroupResultSetToString(sb, rs, SkipNilCursor())

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(strings.Split(got, "\n"), strings.Split(tt.exp, "\n")))
			}
		})
	}
}

func TestNewGroupResultSet_GroupNone_NoDataReturnsNil(t *testing.T) {
	newCursor := func() (reads.SeriesCursor, error) {
		return &sliceSeriesCursor{
			rows: newSeriesRows(
				"aaa,tag0=val00",
				"aaa,tag0=val01",
			)}, nil
	}

	rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupNone}, newCursor)
	if rs != nil {
		t.Errorf("expected nil cursor")
	}
}

func TestNewGroupResultSet_GroupBy_NoDataReturnsNil(t *testing.T) {
	newCursor := func() (reads.SeriesCursor, error) {
		return &sliceSeriesCursor{
			rows: newSeriesRows(
				"aaa,tag0=val00",
				"aaa,tag0=val01",
			)}, nil
	}

	rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupBy, GroupKeys: []string{"tag0"}}, newCursor)
	if rs != nil {
		t.Errorf("expected nil cursor")
	}
}

func TestNewGroupResultSet_SortOrder(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		opts []reads.GroupOption
		exp  string
	}{
		{
			name: "nil hi",
			keys: []string{"tag0", "tag2"},
			exp: `group:
  tag key      : _m,tag0,tag1
  partition key: val00,<nil>
    series: _m=aaa,tag0=val00
    series: _m=cpu,tag0=val00,tag1=val10
    series: _m=cpu,tag0=val00,tag1=val11
    series: _m=cpu,tag0=val00,tag1=val12
group:
  tag key      : _m,tag0
  partition key: val01,<nil>
    series: _m=aaa,tag0=val01
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val20
    series: _m=mem,tag1=val10,tag2=val20
    series: _m=mem,tag1=val11,tag2=val20
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val21
    series: _m=mem,tag1=val11,tag2=val21
`,
		},
		{
			name: "nil lo",
			keys: []string{"tag0", "tag2"},
			opts: []reads.GroupOption{reads.GroupOptionNilSortLo()},
			exp: `group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val20
    series: _m=mem,tag1=val11,tag2=val20
    series: _m=mem,tag1=val10,tag2=val20
group:
  tag key      : _m,tag1,tag2
  partition key: <nil>,val21
    series: _m=mem,tag1=val11,tag2=val21
group:
  tag key      : _m,tag0,tag1
  partition key: val00,<nil>
    series: _m=cpu,tag0=val00,tag1=val10
    series: _m=cpu,tag0=val00,tag1=val11
    series: _m=cpu,tag0=val00,tag1=val12
    series: _m=aaa,tag0=val00
group:
  tag key      : _m,tag0
  partition key: val01,<nil>
    series: _m=aaa,tag0=val01
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newCursor := func() (reads.SeriesCursor, error) {
				return &sliceSeriesCursor{
					rows: newSeriesRows(
						"aaa,tag0=val00",
						"aaa,tag0=val01",
						"cpu,tag0=val00,tag1=val10",
						"cpu,tag0=val00,tag1=val11",
						"cpu,tag0=val00,tag1=val12",
						"mem,tag1=val10,tag2=val20",
						"mem,tag1=val11,tag2=val20",
						"mem,tag1=val11,tag2=val21",
					)}, nil
			}

			var hints datatypes.HintFlags
			hints.SetHintSchemaAllTime()
			rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{
				Group:     datatypes.GroupBy,
				GroupKeys: tt.keys,
				// TODO(jlapacik):
				//     Hints is not used except for the tests in this file.
				//     Eventually this field should be removed entirely.
				Hints: hints,
			}, newCursor, tt.opts...)

			sb := new(strings.Builder)
			GroupResultSetToString(sb, rs, SkipNilCursor())

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(strings.Split(got, "\n"), strings.Split(tt.exp, "\n")))
			}
		})
	}
}

type sliceSeriesCursor struct {
	rows []reads.SeriesRow
	i    int
}

func newSeriesRows(keys ...string) []reads.SeriesRow {
	rows := make([]reads.SeriesRow, len(keys))
	for i := range keys {
		rows[i].Name, rows[i].SeriesTags = models.ParseKeyBytes([]byte(keys[i]))
		rows[i].Tags = rows[i].SeriesTags.Clone()
		rows[i].Tags.Set([]byte("_m"), rows[i].Name)
	}
	return rows
}

func (s *sliceSeriesCursor) Close()     {}
func (s *sliceSeriesCursor) Err() error { return nil }

func (s *sliceSeriesCursor) Next() *reads.SeriesRow {
	if s.i < len(s.rows) {
		s.i++
		return &s.rows[s.i-1]
	}
	return nil
}

func BenchmarkNewGroupResultSet_GroupBy(b *testing.B) {
	card := []int{10, 10, 10}
	vals := make([]gen.CountableSequence, len(card))
	for i := range card {
		vals[i] = gen.NewCounterByteSequenceCount(card[i])
	}

	tags := gen.NewTagsValuesSequenceValues("tag", vals)
	rows := make([]reads.SeriesRow, tags.Count())
	for i := range rows {
		tags.Next()
		t := tags.Value().Clone()
		rows[i].SeriesTags = t
		rows[i].Tags = t
		rows[i].Name = []byte("m0")
	}

	cur := &sliceSeriesCursor{rows: rows}
	newCursor := func() (reads.SeriesCursor, error) {
		cur.i = 0
		return cur, nil
	}

	b.ResetTimer()
	b.ReportAllocs()
	var hints datatypes.HintFlags
	hints.SetHintSchemaAllTime()

	for i := 0; i < b.N; i++ {
		rs := reads.NewGroupResultSet(context.Background(), &datatypes.ReadGroupRequest{Group: datatypes.GroupBy, GroupKeys: []string{"tag2"}, Hints: hints}, newCursor)
		rs.Close()
	}
}
