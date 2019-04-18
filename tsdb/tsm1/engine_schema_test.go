package tsm1_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxdb/tsdb/tsm1"
	"github.com/influxdata/influxql"
)

func TestEngine_TagValues(t *testing.T) {
	e, err := NewEngine()
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
		name string
		args args
		exp  []string
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
			exp: []string{"0A", "AA", "BA", "DA", "EA"},
		},
		{
			name: "only TSM",
			args: args{
				org: 0,
				key: "host",
				min: 0,
				max: 199,
			},
			exp: []string{"DA"},
		},
		{
			name: "only cache",
			args: args{
				org: 0,
				key: "host",
				min: 200,
				max: 299,
			},
			exp: []string{"0A", "AA", "BA", "EA"},
		},
		{
			name: "one timestamp TSM/data",
			args: args{
				org: 0,
				key: "host",
				min: 106,
				max: 106,
			},
			exp: []string{"DA"},
		},
		{
			name: "one timestamp cache/data",
			args: args{
				org: 0,
				key: "host",
				min: 201,
				max: 201,
			},
			exp: []string{"0A", "EA"},
		},
		{
			name: "one timestamp TSM/nodata",
			args: args{
				org: 0,
				key: "host",
				min: 103,
				max: 103,
			},
			exp: nil,
		},
		{
			name: "one timestamp cache/nodata",
			args: args{
				org: 0,
				key: "host",
				min: 203,
				max: 203,
			},
			exp: nil,
		},

		// _measurement tag
		{
			name: "_measurement/all",
			args: args{
				org: 0,
				key: "_measurement",
				min: 0,
				max: 399,
			},
			exp: []string{"cpuA", "memA"},
		},
		{
			name: "_measurement/some",
			args: args{
				org: 0,
				key: "_measurement",
				min: 205,
				max: 399,
			},
			exp: []string{"cpuA"},
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
			exp: []string{"BA", "DA"},
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
			exp: []string{"0A", "AA", "EA"},
		},

		// ***********************
		// * queries for the second org, which has no deleted data
		// ***********************
		{
			args: args{
				org: 1,
				key: "host",
				min: 0,
				max: 1000,
			},
			exp: []string{"0B", "AB", "BB", "CB", "DB", "EB"},
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

			got := cursors.StringIteratorToSlice(iter)
			if !cmp.Equal(got, tc.exp) {
				t.Errorf("unexpected TagValues: -got/+exp\n%v", cmp.Diff(got, tc.exp))
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
