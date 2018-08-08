package query_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
)

var CmpOpts = []cmp.Option{
	cmpopts.IgnoreUnexported(query.ProxyRequest{}),
	cmpopts.IgnoreUnexported(query.Request{}),
}

type compilerA struct {
	A string `json:"a"`
}

func (c compilerA) Compile(ctx context.Context) (*query.Spec, error) {
	panic("not implemented")
}

func (c compilerA) CompilerType() query.CompilerType {
	return "compilerA"
}

var compilerMappings = query.CompilerMappings{
	"compilerA": func() query.Compiler { return new(compilerA) },
}

type dialectB struct {
	B int `json:"b"`
}

func (d dialectB) Encoder() query.MultiResultEncoder {
	panic("not implemented")
}

func (d dialectB) DialectType() query.DialectType {
	return "dialectB"
}

var dialectMappings = query.DialectMappings{
	"dialectB": func() query.Dialect { return new(dialectB) },
}

func TestRequest_JSON(t *testing.T) {
	testCases := []struct {
		name string
		data string
		want query.Request
	}{
		{
			name: "simple",
			data: `{"organization_id":"aaaaaaaaaaaaaaaa","compiler":{"a":"my custom compiler"},"compiler_type":"compilerA"}`,
			want: query.Request{
				OrganizationID: platform.ID{0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA},
				Compiler: &compilerA{
					A: "my custom compiler",
				},
			},
		},
	}
	for _, tc := range testCases {
		var r query.Request
		r.WithCompilerMappings(compilerMappings)

		if err := json.Unmarshal([]byte(tc.data), &r); err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(tc.want, r, CmpOpts...) {
			t.Fatalf("unexpected request: -want/+got:\n%s", cmp.Diff(tc.want, r, CmpOpts...))
		}
		marshalled, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(marshalled), tc.data; got != want {
			t.Fatalf("unexpected marshalled request: -want/+got:\n%s", cmp.Diff(want, got))
		}
	}
}

func TestProxyRequest_JSON(t *testing.T) {
	testCases := []struct {
		name string
		data string
		want query.ProxyRequest
	}{
		{
			name: "simple",
			data: `{"request":{"organization_id":"aaaaaaaaaaaaaaaa","compiler":{"a":"my custom compiler"},"compiler_type":"compilerA"},"dialect":{"b":42},"dialect_type":"dialectB"}`,
			want: query.ProxyRequest{
				Request: query.Request{
					OrganizationID: platform.ID{0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA},
					Compiler: &compilerA{
						A: "my custom compiler",
					},
				},
				Dialect: &dialectB{
					B: 42,
				},
			},
		},
	}
	for _, tc := range testCases {
		var pr query.ProxyRequest
		pr.WithCompilerMappings(compilerMappings)
		pr.WithDialectMappings(dialectMappings)

		if err := json.Unmarshal([]byte(tc.data), &pr); err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(tc.want, pr, CmpOpts...) {
			t.Fatalf("unexpected proxy request: -want/+got:\n%s", cmp.Diff(tc.want, pr, CmpOpts...))
		}
		marshalled, err := json.Marshal(pr)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(marshalled), tc.data; got != want {
			t.Fatalf("unexpected marshalled proxy request: -want/+got:\n%s", cmp.Diff(want, got))
		}
	}
}
