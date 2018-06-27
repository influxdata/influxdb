package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
)

func TestGroupGroupResultSetSorting(t *testing.T) {
	tests := []struct {
		name  string
		cur   seriesCursor
		group ReadRequest_Group
		keys  []string
		exp   []seriesRow
	}{
		{
			name: "group by tag1 in all series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"cpu,tag0=val0,tag1=val0",
					"cpu,tag0=val0,tag1=val1",
					"cpu,tag0=val0,tag1=val2",
					"cpu,tag0=val1,tag1=val0",
					"cpu,tag0=val1,tag1=val1",
					"cpu,tag0=val1,tag1=val2",
				)},
			group: GroupBy,
			keys:  []string{"tag1"},
			exp: newSeriesRows(
				"cpu,tag0=val0,tag1=val0",
				"cpu,tag0=val1,tag1=val0",
				"cpu,tag0=val0,tag1=val1",
				"cpu,tag0=val1,tag1=val1",
				"cpu,tag0=val0,tag1=val2",
				"cpu,tag0=val1,tag1=val2",
			),
		},
		{
			name: "group by tag1 in partial series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"aaa,tag0=val0",
					"aaa,tag0=val1",
					"cpu,tag0=val0,tag1=val0",
					"cpu,tag0=val0,tag1=val1",
					"cpu,tag0=val0,tag1=val2",
					"cpu,tag0=val1,tag1=val0",
					"cpu,tag0=val1,tag1=val1",
					"cpu,tag0=val1,tag1=val2",
				)},
			group: GroupBy,
			keys:  []string{"tag1"},
			exp: newSeriesRows(
				"cpu,tag0=val0,tag1=val0",
				"cpu,tag0=val1,tag1=val0",
				"cpu,tag0=val0,tag1=val1",
				"cpu,tag0=val1,tag1=val1",
				"cpu,tag0=val0,tag1=val2",
				"cpu,tag0=val1,tag1=val2",
				"aaa,tag0=val0",
				"aaa,tag0=val1",
			),
		},
		{
			name: "group by tag2,tag1 with partial series",
			cur: &sliceSeriesCursor{
				rows: newSeriesRows(
					"aaa,tag0=val0",
					"aaa,tag0=val1",
					"cpu,tag0=val0,tag1=val0",
					"cpu,tag0=val0,tag1=val1",
					"cpu,tag0=val0,tag1=val2",
					"mem,tag1=val0,tag2=val0",
					"mem,tag1=val1,tag2=val0",
					"mem,tag1=val1,tag2=val1",
				)},
			group: GroupBy,
			keys:  []string{"tag2,tag1"},
			exp: newSeriesRows(
				"mem,tag1=val0,tag2=val0",
				"mem,tag1=val1,tag2=val0",
				"mem,tag1=val1,tag2=val1",
				"cpu,tag0=val0,tag1=val0",
				"cpu,tag0=val0,tag1=val1",
				"cpu,tag0=val0,tag1=val2",
				"aaa,tag0=val0",
				"aaa,tag0=val1",
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			newCursor := func() (seriesCursor, error) {
				return tt.cur, nil
			}
			rs := newGroupResultSet(context.Background(), &ReadRequest{Group: tt.group, GroupKeys: tt.keys}, newCursor)

			var rows []seriesRow

			for i := range rs.rows {
				rows = append(rows, *rs.rows[i])
			}

			got := selectTags(rows, tt.keys)
			exp := selectTags(tt.exp, tt.keys)

			if !cmp.Equal(got, exp) {
				t.Errorf("unexpected rows -got/+exp\n%s", cmp.Diff(got, exp))
			}
		})
	}
}

func TestKeyMerger(t *testing.T) {
	tests := []struct {
		name string
		tags []models.Tags
		exp  string
	}{
		{
			name: "mixed",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v1")),
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag3=v0")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "mixed 2",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag3=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v0")),
				models.ParseTags([]byte("foo,tag0=v0,tag1=v0,tag2=v1")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
		{
			name: "all different",
			tags: []models.Tags{
				models.ParseTags([]byte("foo,tag0=v0")),
				models.ParseTags([]byte("foo,tag1=v0")),
				models.ParseTags([]byte("foo,tag2=v1")),
				models.ParseTags([]byte("foo,tag3=v0")),
			},
			exp: "tag0,tag1,tag2,tag3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var km keyMerger
			km.setTags(tt.tags[0])
			for _, tags := range tt.tags[1:] {
				km.mergeTagKeys(tags)
			}

			if got := km.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected keys -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}

func selectTags(rows []seriesRow, keys []string) string {
	var srows []string
	for _, row := range rows {
		var ss []string
		for _, key := range keys {
			for _, tag := range row.tags {
				if key == string(tag.Key) {
					ss = append(ss, string(tag.Key)+"="+string(tag.Value))
				}
			}
		}
		srows = append(srows, strings.Join(ss, ","))
	}
	return strings.Join(srows, "\n")
}

type sliceSeriesCursor struct {
	rows []seriesRow
	i    int
}

func newSeriesRows(keys ...string) []seriesRow {
	rows := make([]seriesRow, len(keys))
	for i := range keys {
		rows[i].name, rows[i].stags = models.ParseKeyBytes([]byte(keys[i]))
		rows[i].tags = rows[i].stags.Clone()
		rows[i].tags.Set([]byte("_m"), rows[i].name)
	}
	return rows
}

func (s *sliceSeriesCursor) Close()     {}
func (s *sliceSeriesCursor) Err() error { return nil }

func (s *sliceSeriesCursor) Next() *seriesRow {
	if s.i < len(s.rows) {
		s.i++
		return &s.rows[s.i-1]
	}
	return nil
}
