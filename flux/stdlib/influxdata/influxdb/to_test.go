package influxdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/dependenciestest"
	"github.com/influxdata/flux/dependency"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/execute/executetest"
	"github.com/influxdata/flux/interpreter"
	"github.com/influxdata/flux/querytest"
	"github.com/influxdata/flux/values/valuestest"
	"github.com/influxdata/influxdb/coordinator"
	_ "github.com/influxdata/influxdb/flux/init/static"
	"github.com/influxdata/influxdb/flux/stdlib/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/assert"
)

func TestTo_Query(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(bucket:"mydb") |> to(bucket:"myotherdb/autogen")`,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestTo_Process(t *testing.T) {
	type wanted struct {
		tables []*executetest.Table
		result *mockPointsWriter
	}
	testCases := []struct {
		name string
		spec *influxdb.ToProcedureSpec
		data []flux.Table
		want wanted
	}{
		{
			name: "default case",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my_db",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_value", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_value", 4.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a _value=2 11
a _value=2 21
b _value=1 21
a _value=3 31
c _value=4 41`),
					db: "my_db",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_value", 3.0},
						{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_value", 4.0},
					},
				}},
			},
		},
		{
			name: "default with heterogeneous tag columns",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				KeyCols: []string{"_measurement", "tag1", "tag2", "_field"},
				Data: [][]interface{}{
					{execute.Time(11), "a", "a", "aa", "_value", 2.0},
					{execute.Time(21), "a", "a", "bb", "_value", 2.0},
					{execute.Time(21), "a", "b", "cc", "_value", 1.0},
					{execute.Time(31), "a", "a", "dd", "_value", 3.0},
					{execute.Time(41), "a", "c", "ee", "_value", 4.0},
				},
			}),
				executetest.MustCopyTable(&executetest.Table{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "tagA", Type: flux.TString},
						{Label: "tagB", Type: flux.TString},
						{Label: "tagC", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					KeyCols: []string{"_measurement", "tagA", "tagB", "tagC", "_field"},
					Data: [][]interface{}{
						{execute.Time(11), "b", "a", "aa", "ff", "_value", 2.0},
						{execute.Time(21), "b", "a", "bb", "gg", "_value", 2.0},
						{execute.Time(21), "b", "b", "cc", "hh", "_value", 1.0},
						{execute.Time(31), "b", "a", "dd", "ii", "_value", 3.0},
						{execute.Time(41), "b", "c", "ee", "jj", "_value", 4.0},
					},
				}),
			},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a,tag1=a,tag2=aa _value=2 11
a,tag1=a,tag2=bb _value=2 21
a,tag1=b,tag2=cc _value=1 21
a,tag1=a,tag2=dd _value=3 31
a,tag1=c,tag2=ee _value=4 41
b,tagA=a,tagB=aa,tagC=ff _value=2 11
b,tagA=a,tagB=bb,tagC=gg _value=2 21
b,tagA=b,tagB=cc,tagC=hh _value=1 21
b,tagA=a,tagB=dd,tagC=ii _value=3 31
b,tagA=c,tagB=ee,tagC=jj _value=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					KeyCols: []string{"_measurement", "tag1", "tag2", "_field"},
					Data: [][]interface{}{
						{execute.Time(11), "a", "a", "aa", "_value", 2.0},
						{execute.Time(21), "a", "a", "bb", "_value", 2.0},
						{execute.Time(21), "a", "b", "cc", "_value", 1.0},
						{execute.Time(31), "a", "a", "dd", "_value", 3.0},
						{execute.Time(41), "a", "c", "ee", "_value", 4.0},
					},
				},
					{
						ColMeta: []flux.ColMeta{
							{Label: "_time", Type: flux.TTime},
							{Label: "_measurement", Type: flux.TString},
							{Label: "tagA", Type: flux.TString},
							{Label: "tagB", Type: flux.TString},
							{Label: "tagC", Type: flux.TString},
							{Label: "_field", Type: flux.TString},
							{Label: "_value", Type: flux.TFloat},
						},
						KeyCols: []string{"_measurement", "tagA", "tagB", "tagC", "_field"},
						Data: [][]interface{}{
							{execute.Time(11), "b", "a", "aa", "ff", "_value", 2.0},
							{execute.Time(21), "b", "a", "bb", "gg", "_value", 2.0},
							{execute.Time(21), "b", "b", "cc", "hh", "_value", 1.0},
							{execute.Time(31), "b", "a", "dd", "ii", "_value", 3.0},
							{execute.Time(41), "b", "c", "ee", "jj", "_value", 4.0},
						},
					},
				},
			},
		},
		{
			name: "no _measurement with multiple tag columns",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag1",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", "aa", "_value", 2.0},
					{execute.Time(21), "a", "bb", "_value", 2.0},
					{execute.Time(21), "b", "cc", "_value", 1.0},
					{execute.Time(31), "a", "dd", "_value", 3.0},
					{execute.Time(41), "c", "ee", "_value", 4.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a,tag2=aa _value=2 11
a,tag2=bb _value=2 21
b,tag2=cc _value=1 21
a,tag2=dd _value=3 31
c,tag2=ee _value=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(11), "a", "aa", "_value", 2.0},
						{execute.Time(21), "a", "bb", "_value", 2.0},
						{execute.Time(21), "b", "cc", "_value", 1.0},
						{execute.Time(31), "a", "dd", "_value", 3.0},
						{execute.Time(41), "c", "ee", "_value", 4.0},
					},
				}},
			},
		},
		{
			name: "explicit tags",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					TagColumns:        []string{"tag1", "tag2"},
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "m", "_value", 2.0, "a", "aa"},
					{execute.Time(21), "m", "_value", 2.0, "a", "bb"},
					{execute.Time(21), "m", "_value", 1.0, "b", "cc"},
					{execute.Time(31), "m", "_value", 3.0, "a", "dd"},
					{execute.Time(41), "m", "_value", 4.0, "c", "ee"},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`m,tag1=a,tag2=aa _value=2 11
m,tag1=a,tag2=bb _value=2 21
m,tag1=b,tag2=cc _value=1 21
m,tag1=a,tag2=dd _value=3 31
m,tag1=c,tag2=ee _value=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), "m", "_value", 2.0, "a", "aa"},
						{execute.Time(21), "m", "_value", 2.0, "a", "bb"},
						{execute.Time(21), "m", "_value", 1.0, "b", "cc"},
						{execute.Time(31), "m", "_value", 3.0, "a", "dd"},
						{execute.Time(41), "m", "_value", 4.0, "c", "ee"},
					},
				}},
			},
		},
		{
			name: "explicit field function",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
					FieldFn: interpreter.ResolvedFunction{
						Scope: valuestest.Scope(),
						Fn:    executetest.FunctionExpression(t, `(r) => ({temperature: r.temperature})`),
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(11), "a", 2.0},
					{execute.Time(21), "a", 2.0},
					{execute.Time(21), "b", 1.0},
					{execute.Time(31), "a", 3.0},
					{execute.Time(41), "c", 4.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a temperature=2 11
a temperature=2 21
b temperature=1 21
a temperature=3 31
c temperature=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(11), "a", 2.0},
						{execute.Time(21), "a", 2.0},
						{execute.Time(21), "b", 1.0},
						{execute.Time(31), "a", 3.0},
						{execute.Time(41), "c", 4.0},
					},
				}},
			},
		},
		{
			name: "infer tags from complex field function",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag",
					FieldFn: interpreter.ResolvedFunction{
						Scope: valuestest.Scope(),
						Fn:    executetest.FunctionExpression(t, `(r) => ({day: r.day, temperature: r.temperature, humidity: r.humidity, ratio: r.temperature / r.humidity})`),
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_time", Type: flux.TTime},
					{Label: "day", Type: flux.TString},
					{Label: "tag", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
					{Label: "humidity", Type: flux.TFloat},
					{Label: "_value", Type: flux.TString},
				},
				KeyCols: []string{"_measurement", "_field"},
				Data: [][]interface{}{
					{"m", "f", execute.Time(11), "Monday", "a", 2.0, 1.0, "bogus"},
					{"m", "f", execute.Time(21), "Tuesday", "a", 2.0, 2.0, "bogus"},
					{"m", "f", execute.Time(21), "Wednesday", "b", 1.0, 4.0, "bogus"},
					{"m", "f", execute.Time(31), "Thursday", "a", 3.0, 3.0, "bogus"},
					{"m", "f", execute.Time(41), "Friday", "c", 4.0, 5.0, "bogus"},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a day="Monday",humidity=1,ratio=2,temperature=2 11
a day="Tuesday",humidity=2,ratio=1,temperature=2 21
b day="Wednesday",humidity=4,ratio=0.25,temperature=1 21
a day="Thursday",humidity=3,ratio=1,temperature=3 31
c day="Friday",humidity=5,ratio=0.8,temperature=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_time", Type: flux.TTime},
						{Label: "day", Type: flux.TString},
						{Label: "tag", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
						{Label: "humidity", Type: flux.TFloat},
						{Label: "_value", Type: flux.TString},
					},
					KeyCols: []string{"_measurement", "_field"},
					Data: [][]interface{}{
						{"m", "f", execute.Time(11), "Monday", "a", 2.0, 1.0, "bogus"},
						{"m", "f", execute.Time(21), "Tuesday", "a", 2.0, 2.0, "bogus"},
						{"m", "f", execute.Time(21), "Wednesday", "b", 1.0, 4.0, "bogus"},
						{"m", "f", execute.Time(31), "Thursday", "a", 3.0, 3.0, "bogus"},
						{"m", "f", execute.Time(41), "Friday", "c", 4.0, 5.0, "bogus"},
					},
				}},
			},
		},
		{
			name: "explicit tag columns, multiple values in field function, and extra columns",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "tag1",
					TagColumns:        []string{"tag2"},
					FieldFn: interpreter.ResolvedFunction{
						Scope: valuestest.Scope(),
						Fn:    executetest.FunctionExpression(t, `(r) => ({temperature: r.temperature, humidity: r.humidity})`),
					},
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "tag1", Type: flux.TString},
					{Label: "tag2", Type: flux.TString},
					{Label: "other-string-column", Type: flux.TString},
					{Label: "temperature", Type: flux.TFloat},
					{Label: "humidity", Type: flux.TInt},
					{Label: "other-value-column", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "d", "misc", 2.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "d", "misc", 2.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", "d", "misc", 1.0, int64(50), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "e", "misc", 3.0, int64(60), 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", "e", "misc", 4.0, int64(65), 1.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a,tag2=d humidity=50i,temperature=2 11
a,tag2=d humidity=50i,temperature=2 21
b,tag2=d humidity=50i,temperature=1 21
a,tag2=e humidity=60i,temperature=3 31
c,tag2=e humidity=65i,temperature=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "tag1", Type: flux.TString},
						{Label: "tag2", Type: flux.TString},
						{Label: "other-string-column", Type: flux.TString},
						{Label: "temperature", Type: flux.TFloat},
						{Label: "humidity", Type: flux.TInt},
						{Label: "other-value-column", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(11), "a", "d", "misc", 2.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "a", "d", "misc", 2.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "b", "d", "misc", 1.0, int64(50), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(31), "a", "e", "misc", 3.0, int64(60), 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(41), "c", "e", "misc", 4.0, int64(65), 1.0},
					},
				}},
			},
		},
		{
			name: "multiple _field",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_start", Type: flux.TTime},
					{Label: "_stop", Type: flux.TTime},
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
					{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
					{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_hello", 3.0},
					{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_hello", 4.0},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`a _value=2 11
a _value=2 21
b _value=1 21
a _hello=3 31
c _hello=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_start", Type: flux.TTime},
						{Label: "_stop", Type: flux.TTime},
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
					},
					Data: [][]interface{}{
						{execute.Time(0), execute.Time(100), execute.Time(11), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "a", "_value", 2.0},
						{execute.Time(0), execute.Time(100), execute.Time(21), "b", "_value", 1.0},
						{execute.Time(0), execute.Time(100), execute.Time(31), "a", "_hello", 3.0},
						{execute.Time(0), execute.Time(100), execute.Time(41), "c", "_hello", 4.0},
					},
				}},
			},
		},
		{
			name: "unordered tags",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					TagColumns:        []string{"tag1", "tag2"},
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
					{Label: "tag2", Type: flux.TString},
					{Label: "tag1", Type: flux.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
					{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
					{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
					{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
					{execute.Time(41), "m", "_value", 4.0, "ee", "c"},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`m,tag1=a,tag2=aa _value=2 11
m,tag1=a,tag2=bb _value=2 21
m,tag1=b,tag2=cc _value=1 21
m,tag1=a,tag2=dd _value=3 31
m,tag1=c,tag2=ee _value=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
						{Label: "tag2", Type: flux.TString},
						{Label: "tag1", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
						{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
						{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
						{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
						{execute.Time(41), "m", "_value", 4.0, "ee", "c"},
					},
				}},
			},
		},
		{
			name: "nil timestamp",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					TagColumns:        []string{"tag1", "tag2"},
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
					{Label: "tag2", Type: flux.TString},
					{Label: "tag1", Type: flux.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
					{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
					{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
					{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
					{nil, "m", "_value", 4.0, "ee", "c"},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`m,tag1=a,tag2=aa _value=2 11
m,tag1=a,tag2=bb _value=2 21
m,tag1=b,tag2=cc _value=1 21
m,tag1=a,tag2=dd _value=3 31`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
						{Label: "tag2", Type: flux.TString},
						{Label: "tag1", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
						{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
						{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
						{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
					},
				}},
			},
		},
		{
			name: "nil tag",
			spec: &influxdb.ToProcedureSpec{
				Spec: &influxdb.ToOpSpec{
					Bucket:            "my-bucket",
					TimeColumn:        "_time",
					TagColumns:        []string{"tag1", "tag2"},
					MeasurementColumn: "_measurement",
				},
			},
			data: []flux.Table{executetest.MustCopyTable(&executetest.Table{
				ColMeta: []flux.ColMeta{
					{Label: "_time", Type: flux.TTime},
					{Label: "_measurement", Type: flux.TString},
					{Label: "_field", Type: flux.TString},
					{Label: "_value", Type: flux.TFloat},
					{Label: "tag2", Type: flux.TString},
					{Label: "tag1", Type: flux.TString},
				},
				Data: [][]interface{}{
					{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
					{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
					{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
					{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
					{execute.Time(41), "m", "_value", 4.0, nil, "c"},
				},
			})},
			want: wanted{
				result: &mockPointsWriter{
					points: mockPoints(`m,tag1=a,tag2=aa _value=2 11
m,tag1=a,tag2=bb _value=2 21
m,tag1=b,tag2=cc _value=1 21
m,tag1=a,tag2=dd _value=3 31
m,tag1=c _value=4 41`),
					db: "my-bucket",
					rp: "autogen",
				},
				tables: []*executetest.Table{{
					ColMeta: []flux.ColMeta{
						{Label: "_time", Type: flux.TTime},
						{Label: "_measurement", Type: flux.TString},
						{Label: "_field", Type: flux.TString},
						{Label: "_value", Type: flux.TFloat},
						{Label: "tag2", Type: flux.TString},
						{Label: "tag1", Type: flux.TString},
					},
					Data: [][]interface{}{
						{execute.Time(11), "m", "_value", 2.0, "aa", "a"},
						{execute.Time(21), "m", "_value", 2.0, "bb", "a"},
						{execute.Time(21), "m", "_value", 1.0, "cc", "b"},
						{execute.Time(31), "m", "_value", 3.0, "dd", "a"},
						{execute.Time(41), "m", "_value", 4.0, nil, "c"},
					},
				}},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			deps := influxdb.Dependencies{
				FluxDeps: dependenciestest.Default(),
				StorageDeps: influxdb.StorageDependencies{
					MetaClient: new(mockMetaClient),
					PointsWriter: &mockPointsWriter{
						db: tc.want.result.db,
						rp: tc.want.result.rp,
					},
				},
			}
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.tables,
				nil,
				func(d execute.Dataset, c execute.TableBuilderCache) execute.Transformation {
					ctx, span := dependency.Inject(context.Background())
					defer span.Finish()
					newT, err := influxdb.NewToTransformation(ctx, d, c, tc.spec, deps.StorageDeps)
					if err != nil {
						t.Error(err)
					}
					return newT
				},
			)
			pw := deps.StorageDeps.PointsWriter.(*mockPointsWriter)
			assert.Equal(t, len(tc.want.result.points), len(pw.points))

			gotStr := pointsToStr(pw.points)
			wantStr := pointsToStr(tc.want.result.points)

			assert.Equal(t, wantStr, gotStr)
		})
	}
}

type mockPointsWriter struct {
	points models.Points
	db     string
	rp     string
}

func (m *mockPointsWriter) WritePointsInto(request *coordinator.IntoWriteRequest) error {
	if m.db != request.Database {
		return fmt.Errorf("Wrong database - %s != %s", m.db, request.Database)
	}
	if m.rp != request.RetentionPolicy {
		return fmt.Errorf("Wrong retention policy - %s != %s", m.rp, request.RetentionPolicy)
	}
	m.points = append(m.points, request.Points...)
	return nil
}

type mockMetaClient struct {
}

func (m *mockMetaClient) Databases() []meta.DatabaseInfo {
	panic("mockMetaClient.Databases not implemented")
}

func (m *mockMetaClient) Database(name string) *meta.DatabaseInfo {
	return &meta.DatabaseInfo{
		Name:                   name,
		DefaultRetentionPolicy: "autogen",
		RetentionPolicies:      []meta.RetentionPolicyInfo{{Name: "autogen"}},
	}
}

func pointsToStr(points []models.Point) string {
	outStr := ""
	for _, x := range points {
		outStr += x.String() + "\n"
	}
	return outStr
}

func mockPoints(pointdata string) []models.Point {
	points, err := models.ParsePoints([]byte(pointdata))
	if err != nil {
		return nil
	}
	return points
}
