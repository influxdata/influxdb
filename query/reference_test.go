package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/libflux/go/libflux"
	"github.com/influxdata/flux/semantic"
)

var opts = []cmp.Option{
	cmp.AllowUnexported(
		OptionPropertyReference{},
		semantic.MemberExpression{},
		semantic.IdentifierExpression{},
	),
	cmpopts.IgnoreTypes(semantic.Loc{}, semantic.MonoType{}),
	cmp.Transformer("typ", func(typ semantic.MonoType) string {
		return typ.CanonicalString()
	}),
}

func TestFindFluxOptionReferences(t *testing.T) {
	options := `
option v = {
	bucket: "buc",
	measurement: "disk",
	num: 1,
	timeRangeStart: -1h,
	timeRangeStop: now(),
	windowPeriod: 167ms
}`
	tests := []struct {
		name    string
		source  string
		options string
		want    map[string]*OptionPropertyReference
		wantErr bool
	}{
		{
			name: "no reference in query",
			source: `
from(bucket: "buc")
	|> range(start: 2018-05-22T00:00:00Z, stop: 2018-05-22T00:01:00Z)
	|> filter(fn: (r) => r._measurement == "disk")
	|> filter(fn: (r) => r.host == "host.local")
	|> aggregateWindow(every: 1m, fn: count)`,
			options: options,
			want: map[string]*OptionPropertyReference{},
		},
		{
			name: "simple reference in query",
			source: `
from(bucket: v.bucket)
	|> range(start: 2018-05-22T00:00:00Z, stop: 2018-05-22T00:01:00Z)
	|> filter(fn: (r) => r._measurement == v.measurement)
	|> filter(fn: (r) => r.host == "host.local")
	|> aggregateWindow(every: v.windowPeriod, fn: count)`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"windowPeriod": {
					propertyName: "windowPeriod",
					propertyValue: &semantic.DurationLiteral{
						Values: []ast.Duration{{Unit: "mo"}, {Magnitude: 167000000, Unit: "ns"}},
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "windowPeriod",
						},
					},
				},
				"bucket": {
					propertyName: "bucket",
					propertyValue: &semantic.StringLiteral{
						Value: "buc",
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "bucket",
						},
					},
				},
				"measurement": {
					propertyName: "measurement",
					propertyValue: &semantic.StringLiteral{
						Value: "disk",
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "measurement",
						},
					},
				},
			},
		},
		{
			name: "shadowed by function param",
			source: `
f = (v) => v.num + 1 - v.a
tmp = f(v: {num: 0, a: 2})
`,
			options: options,
			want: map[string]*OptionPropertyReference{},
		},
		{
			name: "shadowed in function body",
			source: `
g = (i) => {
	x = v.measurement
	v = {a: i, timeRangeStart: 2}
	return v.timeRangeStart
}
`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"measurement": {
					propertyName: "measurement",
					propertyValue: &semantic.StringLiteral{
						Value: "disk",
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "measurement",
						},
					},
				},
			},
		},
		{
			name: "overwrite v with itself in function body",
			source: `
k = (i) => {
	v = v
	return v.measurement
}
`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"measurement": {
					propertyName: "measurement",
					propertyValue: &semantic.StringLiteral{
						Value: "disk",
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "measurement",
						},
					},
				},
			},
		},
		{
			name: "overwrite v in function body",
			source: `
k = (i) => {
	x = v.num
	v = {measurement: "cpu"}
	return v.measurement
}
`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"num": {
					propertyName: "num",
					propertyValue: &semantic.IntegerLiteral{
						Value: 1,
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "v"},
							Property: "num",
						},
					},
				},
			},
		},
		{
			name: "copy v",
			source: `
s = v
m = s.timeRangeStop
`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"timeRangeStop": {
					propertyName: "timeRangeStop",
					propertyValue: &semantic.CallExpression{
						Callee: &semantic.IdentifierExpression{Name: "now"},
						Arguments: &semantic.ObjectExpression{
							Properties: []*semantic.Property{},
						},
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "s"},
							Property: "timeRangeStop",
						},
					},
				},
			},
		},
		{
			name: "object with v",
			source: `
s = {v with x: 2}
m = s.x
n = s.num
`,
			options: options,
			want: map[string]*OptionPropertyReference{
				"num": {
					propertyName: "num",

					propertyValue: &semantic.IntegerLiteral{
						Value: 1,
					},
					refs: []*semantic.MemberExpression{
						{
							Object:   &semantic.IdentifierExpression{Name: "s"},
							Property: "num",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			externByte, marshalErr := libflux.ParseString(tt.options).MarshalJSON()
			if marshalErr != nil {
				t.Error(marshalErr)
				return
			}
			extern := json.RawMessage(externByte)
			got, err := FindFluxOptionReferences(context.Background(), tt.source, extern)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindFluxOptionReferences() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got, opts...) {
				t.Errorf("unexpected OptionPropertyReferences: -want/+got\n%s", cmp.Diff(tt.want, got, opts...))
			}
		})
	}
}
