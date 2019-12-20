package tsm1_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
	"github.com/influxdata/influxql"
)

func TestEngine_CancelContext(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	var (
		org    influxdb.ID = 0x6000
		bucket influxdb.ID = 0x6100
	)

	e.MustWritePointsString(org, bucket, `
cpuB,host=0B,os=linux value=1.1 101
cpuB,host=AB,os=linux value=1.2 102
cpuB,host=AB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 105
cpuB,host=DB,os=macOS value=1.3 106
memB,host=DB,os=macOS value=1.3 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	e.MustWritePointsString(org, bucket, `
cpuB,host=0B,os=linux value=1.1 201
cpuB,host=AB,os=linux value=1.2 202
cpuB,host=AB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 205
cpuB,host=EB,os=macOS value=1.3 206
memB,host=EB,os=macOS value=1.3 201`)

	t.Run("cancel tag values no predicate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		key := "host"

		iter, err := e.TagValues(ctx, org, bucket, key, 0, math.MaxInt64, nil)
		if err == nil {
			t.Fatal("TagValues: expected error but got nothing")
		} else if err.Error() != "context canceled" {
			t.Fatalf("TagValues: error %v", err)
		}

		if got := iter.Stats(); !cmp.Equal(got, cursors.CursorStats{}) {
			t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, cursors.CursorStats{}))
		}
	})

	t.Run("cancel tag values with predicate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		key := "host"
		predicate := influxql.MustParseExpr(`os = 'linux'`)

		iter, err := e.TagValues(ctx, org, bucket, key, 0, math.MaxInt64, predicate)
		if err == nil {
			t.Fatal("TagValues: expected error but got nothing")
		} else if err.Error() != "context canceled" {
			t.Fatalf("TagValues: error %v", err)
		}

		if got := iter.Stats(); !cmp.Equal(got, cursors.CursorStats{}) {
			t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, cursors.CursorStats{}))
		}
	})

	t.Run("cancel tag keys no predicate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter, err := e.TagKeys(ctx, org, bucket, 0, math.MaxInt64, nil)
		if err == nil {
			t.Fatal("TagKeys: expected error but got nothing")
		} else if err.Error() != "context canceled" {
			t.Fatalf("TagKeys: error %v", err)
		}

		if got := iter.Stats(); !cmp.Equal(got, cursors.CursorStats{}) {
			t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, cursors.CursorStats{}))
		}
	})

	t.Run("cancel tag keys with predicate", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		predicate := influxql.MustParseExpr(`os = 'linux'`)

		iter, err := e.TagKeys(ctx, org, bucket, 0, math.MaxInt64, predicate)
		if err == nil {
			t.Fatal("TagKeys: expected error but got nothing")
		} else if err.Error() != "context canceled" {
			t.Fatalf("TagKeys: error %v", err)
		}

		if got := iter.Stats(); !cmp.Equal(got, cursors.CursorStats{}) {
			t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, cursors.CursorStats{}))
		}
	})
}

func TestEngine_TagValues(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 101
cpuA,host=AA,os=linux value=1.2 102
cpuA,host=AA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 104
cpuA,host=CA,os=linux value=1.3 105
cpuA,host=DA,os=macOS value=1.3 106
memA,host=DA,os=macOS value=1.3 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 101
cpuB,host=AB,os=linux value=1.2 102
cpuB,host=AB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 104
cpuB,host=CB,os=linux value=1.3 105
cpuB,host=DB,os=macOS value=1.3 106
memB,host=DB,os=macOS value=1.3 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	// delete some data from the first bucket
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 105)

	// leave some points in the cache
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpuA,host=0A,os=linux value=1.1 201
cpuA,host=AA,os=linux value=1.2 202
cpuA,host=AA,os=linux value=1.3 204
cpuA,host=BA,os=macOS value=1.3 204
cpuA,host=BA,os=macOS value=1.3 205
cpuA,host=EA,os=linux value=1.3 206
memA,host=EA,os=linux value=1.3 201`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpuB,host=0B,os=linux value=1.1 201
cpuB,host=AB,os=linux value=1.2 202
cpuB,host=AB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 204
cpuB,host=BB,os=linux value=1.3 205
cpuB,host=EB,os=macOS value=1.3 206
memB,host=EB,os=macOS value=1.3 201`)

	type args struct {
		org      int
		key      string
		min, max int64
		expr     string
	}

	var tests = []struct {
		name     string
		args     args
		exp      []string
		expStats cursors.CursorStats
	}{
		// ***********************
		// * queries for the first org, which has some deleted data
		// ***********************

		// host tag
		{
			name: "TSM and cache",
			args: args{
				org: 0,
				key: "host",
				min: 0,
				max: 300,
			},
			exp:      []string{"0A", "AA", "BA", "DA", "EA"},
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},
		{
			name: "only TSM",
			args: args{
				org: 0,
				key: "host",
				min: 0,
				max: 199,
			},
			exp:      []string{"DA"},
			expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
		},
		{
			name: "only cache",
			args: args{
				org: 0,
				key: "host",
				min: 200,
				max: 299,
			},
			exp:      []string{"0A", "AA", "BA", "EA"},
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},
		{
			name: "one timestamp TSM/data",
			args: args{
				org: 0,
				key: "host",
				min: 106,
				max: 106,
			},
			exp:      []string{"DA"},
			expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
		},
		{
			name: "one timestamp cache/data",
			args: args{
				org: 0,
				key: "host",
				min: 201,
				max: 201,
			},
			exp:      []string{"0A", "EA"},
			expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
		},
		{
			name: "one timestamp TSM/nodata",
			args: args{
				org: 0,
				key: "host",
				min: 103,
				max: 103,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
		},
		{
			name: "one timestamp cache/nodata",
			args: args{
				org: 0,
				key: "host",
				min: 203,
				max: 203,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 7, ScannedBytes: 56},
		},

		// models.MeasurementTagKey tag
		{
			name: "_measurement/all",
			args: args{
				org: 0,
				key: models.MeasurementTagKey,
				min: 0,
				max: 399,
			},
			exp:      []string{"cpuA", "memA"},
			expStats: cursors.CursorStats{ScannedValues: 1, ScannedBytes: 8},
		},
		{
			name: "_measurement/some",
			args: args{
				org: 0,
				key: models.MeasurementTagKey,
				min: 205,
				max: 399,
			},
			exp:      []string{"cpuA"},
			expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
		},

		// queries with predicates
		{
			name: "predicate/macOS",
			args: args{
				org:  0,
				key:  "host",
				min:  0,
				max:  300,
				expr: `os = 'macOS'`,
			},
			exp:      []string{"BA", "DA"},
			expStats: cursors.CursorStats{ScannedValues: 2, ScannedBytes: 16},
		},
		{
			name: "predicate/linux",
			args: args{
				org:  0,
				key:  "host",
				min:  0,
				max:  300,
				expr: `os = 'linux'`,
			},
			exp:      []string{"0A", "AA", "EA"},
			expStats: cursors.CursorStats{ScannedValues: 4, ScannedBytes: 32},
		},

		// ***********************
		// * queries for the second org, which has no deleted data
		// ***********************
		{
			name: "all data",
			args: args{
				org: 1,
				key: "host",
				min: 0,
				max: 1000,
			},
			exp:      []string{"0B", "AB", "BB", "CB", "DB", "EB"},
			expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
		},

		// ***********************
		// * other scenarios
		// ***********************
		{
			// ensure StringIterator is never nil
			name: "predicate/no candidate series",
			args: args{
				org:  1,
				key:  "host",
				min:  0,
				max:  1000,
				expr: `foo = 'bar'`,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := tc.args
			var expr influxql.Expr
			if len(a.expr) > 0 {
				expr = influxql.MustParseExpr(a.expr)
			}

			iter, err := e.TagValues(context.Background(), orgs[a.org].org, orgs[a.org].bucket, a.key, a.min, a.max, expr)
			if err != nil {
				t.Fatalf("TagValues: error %v", err)
			}

			if got := cursors.StringIteratorToSlice(iter); !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected TagValues: -got/+exp\n%v", cmp.Diff(got, tc.exp))
			}

			if got := iter.Stats(); !cmp.Equal(got, tc.expStats) {
				t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, tc.expStats))
			}
		})
	}
}

func TestEngine_TagKeys(t *testing.T) {
	e, err := NewEngine(tsm1.NewConfig(), t)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	orgs := []struct {
		org, bucket influxdb.ID
	}{
		{
			org:    0x5020,
			bucket: 0x5100,
		},
		{
			org:    0x6000,
			bucket: 0x6100,
		},
	}

	// this org will require escaping the 0x20 byte
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
cpu,cpu0=v,cpu2=v        f=1 107
cpu,cpu2=v,cpu3=v        f=1 109
mem,mem0=v,mem1=v        f=1 101`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpu,cpu0=v,cpu1=v,cpu2=v f=1 101
cpu,cpu1=v               f=1 103
cpu,cpu2=v               f=1 105
cpu,cpu0=v,cpu2=v        f=1 107
cpu,cpu2=v,cpu3=v        f=1 109
mem,mem0=v,mem1=v        f=1 101`)

	// send some points to TSM data
	e.MustWriteSnapshot()

	// delete some data from the first bucket
	e.MustDeleteBucketRange(orgs[0].org, orgs[0].bucket, 0, 105)

	// leave some points in the cache
	e.MustWritePointsString(orgs[0].org, orgs[0].bucket, `
cpu,cpu3=v,cpu4=v,cpu5=v f=1 201
cpu,cpu4=v               f=1 203
cpu,cpu3=v               f=1 205
cpu,cpu3=v,cpu4=v        f=1 207
cpu,cpu4=v,cpu5=v        f=1 209
mem,mem1=v,mem2=v        f=1 201`)
	e.MustWritePointsString(orgs[1].org, orgs[1].bucket, `
cpu,cpu3=v,cpu4=v,cpu5=v f=1 201
cpu,cpu4=v               f=1 203
cpu,cpu3=v               f=1 205
cpu,cpu3=v,cpu4=v        f=1 207
cpu,cpu4=v,cpu5=v        f=1 209
mem,mem1=v,mem2=v        f=1 201`)

	type args struct {
		org      int
		min, max int64
		expr     string
	}

	var tests = []struct {
		name     string
		args     args
		exp      []string
		expStats cursors.CursorStats
	}{
		// ***********************
		// * queries for the first org, which has some deleted data
		// ***********************

		{
			name: "TSM and cache",
			args: args{
				org: 0,
				min: 0,
				max: 300,
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu2", "cpu3", "cpu4", "cpu5", "mem1", "mem2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24},
		},
		{
			name: "only TSM",
			args: args{
				org: 0,
				min: 0,
				max: 199,
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu2", "cpu3", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 5, ScannedBytes: 40},
		},
		{
			name: "only cache",
			args: args{
				org: 0,
				min: 200,
				max: 299,
			},
			exp:      []string{models.MeasurementTagKey, "cpu3", "cpu4", "cpu5", "mem1", "mem2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 4, ScannedBytes: 32},
		},
		{
			name: "one timestamp TSM/data",
			args: args{
				org: 0,
				min: 107,
				max: 107,
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},
		{
			name: "one timestamp cache/data",
			args: args{
				org: 0,
				min: 207,
				max: 207,
			},
			exp:      []string{models.MeasurementTagKey, "cpu3", "cpu4", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 5, ScannedBytes: 40},
		},
		{
			name: "one timestamp TSM/nodata",
			args: args{
				org: 0,
				min: 102,
				max: 102,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},
		{
			name: "one timestamp cache/nodata",
			args: args{
				org: 0,
				min: 202,
				max: 202,
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 6, ScannedBytes: 48},
		},

		// queries with predicates
		{
			name: "predicate/all time/cpu",
			args: args{
				org:  0,
				min:  0,
				max:  300,
				expr: "_m = 'cpu'",
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu2", "cpu3", "cpu4", "cpu5", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 2, ScannedBytes: 16},
		},
		{
			name: "predicate/all time/mem",
			args: args{
				org:  0,
				min:  0,
				max:  300,
				expr: "_m = 'mem'",
			},
			exp:      []string{models.MeasurementTagKey, "mem1", "mem2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 1, ScannedBytes: 8},
		},
		{
			name: "predicate/all time/cpu0",
			args: args{
				org:  0,
				min:  0,
				max:  300,
				expr: "cpu0 = 'v'",
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
		},
		{
			name: "predicate/all time/cpu3",
			args: args{
				org:  0,
				min:  0,
				max:  300,
				expr: "cpu3 = 'v'",
			},
			exp:      []string{models.MeasurementTagKey, "cpu2", "cpu3", "cpu4", "cpu5", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 2, ScannedBytes: 16},
		},

		// ***********************
		// * queries for the second org, which has no deleted data
		// ***********************
		{
			name: "TSM and cache",
			args: args{
				org: 1,
				min: 0,
				max: 300,
			},
			exp:      []string{models.MeasurementTagKey, "cpu0", "cpu1", "cpu2", "cpu3", "cpu4", "cpu5", "mem0", "mem1", "mem2", models.FieldKeyTagKey},
			expStats: cursors.CursorStats{ScannedValues: 2, ScannedBytes: 16},
		},

		// ***********************
		// * other scenarios
		// ***********************
		{
			// ensure StringIterator is never nil
			name: "predicate/no candidate series",
			args: args{
				org:  0,
				min:  0,
				max:  300,
				expr: "foo = 'bar'",
			},
			exp:      nil,
			expStats: cursors.CursorStats{ScannedValues: 0, ScannedBytes: 0},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("org%d/%s", tc.args.org, tc.name), func(t *testing.T) {
			a := tc.args
			var expr influxql.Expr
			if len(a.expr) > 0 {
				expr = influxql.MustParseExpr(a.expr)
				expr = influxql.RewriteExpr(expr, func(expr influxql.Expr) influxql.Expr {
					switch n := expr.(type) {
					case *influxql.BinaryExpr:
						if r, ok := n.LHS.(*influxql.VarRef); ok {
							if r.Val == "_m" {
								r.Val = models.MeasurementTagKey
							}
						}
					}
					return expr
				})
			}

			iter, err := e.TagKeys(context.Background(), orgs[a.org].org, orgs[a.org].bucket, a.min, a.max, expr)
			if err != nil {
				t.Fatalf("TagKeys: error %v", err)
			}

			if got := cursors.StringIteratorToSlice(iter); !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected TagKeys: -got/+exp\n%v", cmp.Diff(got, tc.exp))
			}

			if got := iter.Stats(); !cmp.Equal(got, tc.expStats) {
				t.Errorf("unexpected Stats: -got/+exp\n%v", cmp.Diff(got, tc.expStats))
			}
		})
	}
}

func TestValidateTagPredicate(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{
			expr:    `"_m" = 'foo'`,
			wantErr: false,
		},
		{
			expr:    `_m = 'foo'`,
			wantErr: false,
		},
		{
			expr:    `_m = foo`,
			wantErr: true,
		},
		{
			expr:    `_m = 5`,
			wantErr: true,
		},
		{
			expr:    `_m =~ //`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tsm1.ValidateTagPredicate(influxql.MustParseExpr(tt.expr)); (err != nil) != tt.wantErr {
				t.Errorf("ValidateTagPredicate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
