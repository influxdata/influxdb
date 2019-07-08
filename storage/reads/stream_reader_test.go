package reads_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

func errCmp(x, y error) bool {
	if x == nil {
		return y == nil
	}
	if y == nil {
		return false
	}
	return x.Error() == y.Error()
}

func errTr(x error) string {
	if x == nil {
		return ""
	}
	return x.Error()
}

func TestNewResultSetStreamReader(t *testing.T) {
	tests := []struct {
		name   string
		stream *sliceStreamReader
		exp    string
		expErr error
	}{
		{
			name: "float series",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
					floatF(floatS{
						0: 1.0,
						1: 2.0,
						2: 3.0,
					}),
					seriesF(Float, "cpu,tag0=val1"),
					floatF(floatS{
						10: 11.0,
						11: 12.0,
						12: 13.0,
					}),
				),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
                     0 |               1.00
                     1 |               2.00
                     2 |               3.00
series: _m=cpu,tag0=val1
  cursor:Float
                    10 |              11.00
                    11 |              12.00
                    12 |              13.00
`,
		},

		{
			name: "some empty series",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
					seriesF(Float, "cpu,tag0=val1"),
					floatF(floatS{
						10: 11.0,
						11: 12.0,
						12: 13.0,
					}),
				),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
series: _m=cpu,tag0=val1
  cursor:Float
                    10 |              11.00
                    11 |              12.00
                    12 |              13.00
`,
		},

		{
			name: "all data types",
			stream: newStreamReader(
				response(
					seriesF(Boolean, "cpu,tag0=booleans"),
					booleanF(booleanS{
						3: false,
						4: true,
						5: true,
					}),
					seriesF(Float, "cpu,tag0=floats"),
					floatF(floatS{
						0: 1.0,
						1: 2.0,
						2: 3.0,
					}),
					seriesF(Integer, "cpu,tag0=integers"),
					integerF(integerS{
						1: 1,
						2: 2,
						3: 3,
					}),
					seriesF(String, "cpu,tag0=strings"),
					stringF(stringS{
						33: "thing 1",
						34: "thing 2",
						35: "things",
					}),
					seriesF(Unsigned, "cpu,tag0=unsigned"),
					unsignedF(unsignedS{
						2: 55,
						3: 56,
						4: 57,
					}),
				),
			),
			exp: `series: _m=cpu,tag0=booleans
  cursor:Boolean
                     3 | false
                     4 | true
                     5 | true
series: _m=cpu,tag0=floats
  cursor:Float
                     0 |               1.00
                     1 |               2.00
                     2 |               3.00
series: _m=cpu,tag0=integers
  cursor:Integer
                     1 |                    1
                     2 |                    2
                     3 |                    3
series: _m=cpu,tag0=strings
  cursor:String
                    33 |              thing 1
                    34 |              thing 2
                    35 |               things
series: _m=cpu,tag0=unsigned
  cursor:Unsigned
                     2 |                   55
                     3 |                   56
                     4 |                   57
`,
		},

		{
			name: "invalid_points_no_series",
			stream: newStreamReader(
				response(
					floatF(floatS{0: 1.0}),
				),
			),
			expErr: errors.New("expected series frame, got *datatypes.ReadResponse_Frame_FloatPoints"),
		},

		{
			name: "no points frames",
			stream: newStreamReader(
				response(
					seriesF(Boolean, "cpu,tag0=booleans"),
					seriesF(Float, "cpu,tag0=floats"),
					seriesF(Integer, "cpu,tag0=integers"),
					seriesF(String, "cpu,tag0=strings"),
					seriesF(Unsigned, "cpu,tag0=unsigned"),
				),
			),
			exp: `series: _m=cpu,tag0=booleans
  cursor:Boolean
series: _m=cpu,tag0=floats
  cursor:Float
series: _m=cpu,tag0=integers
  cursor:Integer
series: _m=cpu,tag0=strings
  cursor:String
series: _m=cpu,tag0=unsigned
  cursor:Unsigned
`,
		},

		{
			name: "invalid_group_frame",
			stream: newStreamReader(
				response(
					groupF("tag0", "val0"),
					floatF(floatS{0: 1.0}),
				),
			),
			expErr: errors.New("expected series frame, got *datatypes.ReadResponse_Frame_Group"),
		},

		{
			name: "invalid_multiple_data_types",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
					floatF(floatS{0: 1.0}),
					integerF(integerS{0: 1}),
				),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
                     0 |               1.00
  cursor err: floatCursorStreamReader: unexpected frame type *datatypes.ReadResponse_Frame_IntegerPoints
`,
			expErr: errors.New("floatCursorStreamReader: unexpected frame type *datatypes.ReadResponse_Frame_IntegerPoints"),
		},

		{
			name: "some empty frames",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
				),
				response(
					floatF(floatS{
						0: 1.0,
						1: 2.0,
						2: 3.0,
					}),
				),
				response(),
				response(
					seriesF(Float, "cpu,tag0=val1"),
				),
				response(),
				response(
					floatF(floatS{
						10: 11.0,
						11: 12.0,
						12: 13.0,
					}),
				),
				response(),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
                     0 |               1.00
                     1 |               2.00
                     2 |               3.00
series: _m=cpu,tag0=val1
  cursor:Float
                    10 |              11.00
                    11 |              12.00
                    12 |              13.00
`,
		},

		{
			name: "last frame empty",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
					floatF(floatS{
						0: 1.0,
						1: 2.0,
						2: 3.0,
					}),
				),
				response(),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
                     0 |               1.00
                     1 |               2.00
                     2 |               3.00
`,
		},

		{
			name: "ErrUnexpectedEOF",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
				),
				response(
					floatF(floatS{
						0: 1.0,
						1: 2.0,
						2: 3.0,
					}),
				),
				response(),
				response(),
				response(),
			),
			exp: `series: _m=cpu,tag0=val0
  cursor:Float
                     0 |               1.00
                     1 |               2.00
                     2 |               3.00
  cursor err: peekFrame: no data
`,
			expErr: reads.ErrStreamNoData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := reads.NewResultSetStreamReader(tt.stream)
			sb := new(strings.Builder)
			ResultSetToString(sb, rs)

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}

			if got := rs.Err(); !cmp.Equal(got, tt.expErr, cmp.Comparer(errCmp)) {
				t.Errorf("unexpected error; -got/+exp\n%s", cmp.Diff(got, tt.expErr, cmp.Transformer("err", errTr)))
			}
		})
	}
}

func TestNewResultSetStreamReader_SkipSeriesCursors(t *testing.T) {
	stream := newStreamReader(
		response(
			seriesF(Float, "cpu,tag0=floats"),
			floatF(floatS{0: 1.0}),
			seriesF(Integer, "cpu,tag0=integers"),
			integerF(integerS{1: 1}),
			seriesF(Unsigned, "cpu,tag0=unsigned"),
			unsignedF(unsignedS{2: 55}),
		),
	)

	expSeries := []string{"_m=cpu,tag0=floats", "_m=cpu,tag0=integers", "_m=cpu,tag0=unsigned"}

	rs := reads.NewResultSetStreamReader(stream)
	for i := 0; i < 3; i++ {
		if got := rs.Next(); !cmp.Equal(got, true) {
			t.Errorf("expected true")
		}

		sb := new(strings.Builder)
		TagsToString(sb, rs.Tags())
		if got := strings.TrimSpace(sb.String()); !cmp.Equal(got, expSeries[i]) {
			t.Errorf("unexpected tags; -got/+exp\n%s", cmp.Diff(got, expSeries[i]))
		}

		cur := rs.Cursor()
		if cur == nil {
			t.Errorf("expected cursor")
		}

		cur.Close()
	}

	if got := rs.Next(); !cmp.Equal(got, false) {
		t.Errorf("expected false")
	}
	rs.Close()
}

func TestNewGroupResultSetStreamReader(t *testing.T) {
	tests := []struct {
		name   string
		stream *sliceStreamReader
		exp    string
		expErr error
	}{
		{
			name: "groups none no series no points",
			stream: newStreamReader(
				response(
					groupF("tag0,tag1", ""),
				),
			),
			exp: `group:
  tag key      : tag0,tag1
  partition key: 
`,
		},
		{
			name: "groups none series no points",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", ""),
					seriesF(Float, "cpu,tag0=floats"),
					seriesF(Integer, "cpu,tag0=integers"),
					seriesF(Unsigned, "cpu,tag0=unsigned"),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: 
    series: _m=cpu,tag0=floats
      cursor:Float
    series: _m=cpu,tag0=integers
      cursor:Integer
    series: _m=cpu,tag0=unsigned
      cursor:Unsigned
`,
		},
		{
			name: "groups none series points",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", ""),
					seriesF(Float, "cpu,tag0=floats"),
					floatF(floatS{0: 0.0, 1: 1.0, 2: 2.0}),
					seriesF(Integer, "cpu,tag0=integers"),
					integerF(integerS{10: 10, 20: 20, 30: 30}),
					seriesF(Unsigned, "cpu,tag0=unsigned"),
					unsignedF(unsignedS{100: 100, 200: 200, 300: 300}),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: 
    series: _m=cpu,tag0=floats
      cursor:Float
                         0 |               0.00
                         1 |               1.00
                         2 |               2.00
    series: _m=cpu,tag0=integers
      cursor:Integer
                        10 |                   10
                        20 |                   20
                        30 |                   30
    series: _m=cpu,tag0=unsigned
      cursor:Unsigned
                       100 |                  100
                       200 |                  200
                       300 |                  300
`,
		},
		{
			name: "groups by no series no points",
			stream: newStreamReader(
				response(
					groupF("tag00,tag10", "val00,val10"),
					groupF("tag00,tag10", "val00,val11"),
					groupF("tag00,tag10", "val01,val10"),
					groupF("tag00,tag10", "val01,val11"),
				),
			),
			exp: `group:
  tag key      : tag00,tag10
  partition key: val00,val10
group:
  tag key      : tag00,tag10
  partition key: val00,val11
group:
  tag key      : tag00,tag10
  partition key: val01,val10
group:
  tag key      : tag00,tag10
  partition key: val01,val11
`,
		},
		{
			name: "groups by series no points",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", "cpu,val0"),
					seriesF(Float, "cpu,tag0=val0"),
					seriesF(Float, "cpu,tag0=val0,tag1=val0"),
					groupF("_m,tag0", "cpu,val1"),
					seriesF(Float, "cpu,tag0=val1"),
					seriesF(Float, "cpu,tag0=val1,tag1=val0"),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: cpu,val0
    series: _m=cpu,tag0=val0
      cursor:Float
    series: _m=cpu,tag0=val0,tag1=val0
      cursor:Float
group:
  tag key      : _m,tag0
  partition key: cpu,val1
    series: _m=cpu,tag0=val1
      cursor:Float
    series: _m=cpu,tag0=val1,tag1=val0
      cursor:Float
`,
		},
		{
			name: "missing group frame",
			stream: newStreamReader(
				response(
					seriesF(Float, "cpu,tag0=val0"),
				),
			),
			expErr: errors.New("expected group frame, got *datatypes.ReadResponse_Frame_Series"),
		},
		{
			name: "incorrect points frame data type",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", "cpu,val0"),
					seriesF(Float, "cpu,tag0=val0"),
					integerF(integerS{0: 1}),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: cpu,val0
    series: _m=cpu,tag0=val0
      cursor:Float
      cursor err: floatCursorStreamReader: unexpected frame type *datatypes.ReadResponse_Frame_IntegerPoints
`,
			expErr: errors.New("floatCursorStreamReader: unexpected frame type *datatypes.ReadResponse_Frame_IntegerPoints"),
		},
		{
			name: "partition key order",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", "cpu,val1"),
					groupF("_m,tag0", "cpu,val0"),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: cpu,val1
`,
			expErr: reads.ErrPartitionKeyOrder,
		},
		{
			name: "partition key order",
			stream: newStreamReader(
				response(
					groupF("_m", "cpu,"),
					groupF("_m,tag0", "cpu,val0"),
				),
			),
			exp: `group:
  tag key      : _m
  partition key: cpu,<nil>
`,
			expErr: reads.ErrPartitionKeyOrder,
		},
		{
			name: "partition key order",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", ",val0"),
					groupF("_m,tag0", "cpu,val0"),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: <nil>,val0
`,
			expErr: reads.ErrPartitionKeyOrder,
		},
		{
			name: "partition key order",
			stream: newStreamReader(
				response(
					groupF("_m,tag0", "cpu,val0"),
					groupF("_m,tag0", "cpu,val1"),
					groupF("_m,tag0", ","),
					groupF("_m,tag0", ",val0"),
				),
			),
			exp: `group:
  tag key      : _m,tag0
  partition key: cpu,val0
group:
  tag key      : _m,tag0
  partition key: cpu,val1
group:
  tag key      : _m,tag0
  partition key: <nil>,<nil>
`,
			expErr: reads.ErrPartitionKeyOrder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := reads.NewGroupResultSetStreamReader(tt.stream)
			sb := new(strings.Builder)
			GroupResultSetToString(sb, rs)

			if got := sb.String(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value; -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}

			if got := rs.Err(); !cmp.Equal(got, tt.expErr, cmp.Comparer(errCmp)) {
				t.Errorf("unexpected error; -got/+exp\n%s", cmp.Diff(got, tt.expErr, cmp.Transformer("err", errTr)))
			}
		})
	}
}

func joinB(b [][]byte) string {
	return string(bytes.Join(b, []byte(",")))
}

func TestNewGroupResultSetStreamReader_SkipGroupCursors(t *testing.T) {
	stream := newStreamReader(
		response(
			groupF("_m,tag0", "cpu,val0"),
			seriesF(Float, "cpu,tag0=val0"),
			floatF(floatS{0: 1.0}),
			groupF("_m,tag0", "cpu,val1"),
			seriesF(Integer, "cpu,tag0=val1,tag1=val0"),
			integerF(integerS{1: 1}),
			seriesF(Integer, "cpu,tag0=val1,tag1=val1"),
			unsignedF(unsignedS{2: 55}),
		),
	)

	type expGroup struct {
		tagKeys string
		parKeys string
		series  []string
	}

	t.Run("skip series cursors", func(t *testing.T) {
		exp := []expGroup{
			{
				tagKeys: "_m,tag0",
				parKeys: "cpu,val0",
				series:  []string{"_m=cpu,tag0=val0"},
			},
			{
				tagKeys: "_m,tag0",
				parKeys: "cpu,val1",
				series:  []string{"_m=cpu,tag0=val1,tag1=val0", "_m=cpu,tag0=val1,tag1=val1"},
			},
		}

		stream.reset()
		grs := reads.NewGroupResultSetStreamReader(stream)

		for i := range exp {
			rs := grs.Next()
			if rs == nil {
				t.Errorf("expected group cursor")
			}

			if got := joinB(rs.Keys()); !cmp.Equal(got, exp[i].tagKeys) {
				t.Errorf("unexpected group keys; -got/+exp\n%s", cmp.Diff(got, exp[i].tagKeys))
			}
			if got := joinB(rs.PartitionKeyVals()); !cmp.Equal(got, exp[i].parKeys) {
				t.Errorf("unexpected group keys; -got/+exp\n%s", cmp.Diff(got, exp[i].parKeys))
			}

			for j := range exp[i].series {
				if got := rs.Next(); !cmp.Equal(got, true) {
					t.Errorf("expected true")
				}

				sb := new(strings.Builder)
				TagsToString(sb, rs.Tags())
				if got := strings.TrimSpace(sb.String()); !cmp.Equal(got, exp[i].series[j]) {
					t.Errorf("unexpected tags; -got/+exp\n%s", cmp.Diff(got, exp[i].series[j]))
				}

				cur := rs.Cursor()
				if cur == nil {
					t.Errorf("expected cursor")
				}

				cur.Close()
			}

			if got := rs.Next(); !cmp.Equal(got, false) {
				t.Errorf("expected false")
			}
			rs.Close()
		}

		if rs := grs.Next(); rs != nil {
			t.Errorf("unexpected group cursor")
		}

		grs.Close()
	})

	t.Run("skip series", func(t *testing.T) {
		exp := []expGroup{
			{
				tagKeys: "_m,tag0",
				parKeys: "cpu,val0",
			},
			{
				tagKeys: "_m,tag0",
				parKeys: "cpu,val1",
			},
		}

		stream.reset()
		grs := reads.NewGroupResultSetStreamReader(stream)

		for i := range exp {
			rs := grs.Next()
			if rs == nil {
				t.Errorf("expected group cursor")
			}

			if got := joinB(rs.Keys()); !cmp.Equal(got, exp[i].tagKeys) {
				t.Errorf("unexpected group keys; -got/+exp\n%s", cmp.Diff(got, exp[i].tagKeys))
			}
			if got := joinB(rs.PartitionKeyVals()); !cmp.Equal(got, exp[i].parKeys) {
				t.Errorf("unexpected group keys; -got/+exp\n%s", cmp.Diff(got, exp[i].parKeys))
			}
			rs.Close()
		}

		if rs := grs.Next(); rs != nil {
			t.Errorf("unexpected group cursor")
		}

		grs.Close()
	})

}

func response(f ...datatypes.ReadResponse_Frame) datatypes.ReadResponse {
	return datatypes.ReadResponse{Frames: f}
}

type sliceStreamReader struct {
	res []datatypes.ReadResponse
	p   int
}

func newStreamReader(res ...datatypes.ReadResponse) *sliceStreamReader {
	return &sliceStreamReader{res: res}
}

func (r *sliceStreamReader) reset() { r.p = 0 }

func (r *sliceStreamReader) Recv() (*datatypes.ReadResponse, error) {
	if r.p < len(r.res) {
		res := &r.res[r.p]
		r.p++
		return res, nil
	}
	return nil, io.EOF
}

func (r *sliceStreamReader) String() string {
	return ""
}

// errStreamReader is a reads.StreamReader that always returns an error.
type errStreamReader string

func (e errStreamReader) Recv() (*datatypes.ReadResponse, error) {
	return nil, errors.New(string(e))
}

// emptyStreamReader is a reads.StreamReader that returns no data.
type emptyStreamReader struct{}

func (s *emptyStreamReader) Recv() (*datatypes.ReadResponse, error) {
	return nil, nil
}

func groupF(tagKeys string, partitionKeyVals string) datatypes.ReadResponse_Frame {
	var pk [][]byte
	if partitionKeyVals != "" {
		pk = bytes.Split([]byte(partitionKeyVals), []byte(","))
		for i := range pk {
			if bytes.Equal(pk[i], nilValBytes) {
				pk[i] = nil
			}
		}
	}
	return datatypes.ReadResponse_Frame{
		Data: &datatypes.ReadResponse_Frame_Group{
			Group: &datatypes.ReadResponse_GroupFrame{
				TagKeys:          bytes.Split([]byte(tagKeys), []byte(",")),
				PartitionKeyVals: pk,
			},
		},
	}
}

const (
	Float    = datatypes.DataTypeFloat
	Integer  = datatypes.DataTypeInteger
	Unsigned = datatypes.DataTypeUnsigned
	Boolean  = datatypes.DataTypeBoolean
	String   = datatypes.DataTypeString
)

func seriesF(dt datatypes.ReadResponse_DataType, measurement string) datatypes.ReadResponse_Frame {
	name, tags := models.ParseKeyBytes([]byte(measurement))
	tags.Set([]byte("_m"), name)
	t := make([]datatypes.Tag, len(tags))
	for i, tag := range tags {
		t[i].Key = tag.Key
		t[i].Value = tag.Value
	}

	return datatypes.ReadResponse_Frame{
		Data: &datatypes.ReadResponse_Frame_Series{
			Series: &datatypes.ReadResponse_SeriesFrame{
				DataType: dt,
				Tags:     t,
			},
		},
	}
}
