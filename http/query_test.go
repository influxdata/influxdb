package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/query"
	_ "github.com/influxdata/influxdb/query/builtin"
)

func TestQueryRequest_WithDefaults(t *testing.T) {
	type fields struct {
		Spec    *flux.Spec
		AST     *ast.Package
		Query   string
		Type    string
		Dialect QueryDialect
		org     *platform.Organization
	}
	tests := []struct {
		name   string
		fields fields
		want   QueryRequest
	}{
		{
			name: "empty query has defaults set",
			want: QueryRequest{
				Type: "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
					Header:         func(x bool) *bool { return &x }(true),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := QueryRequest{
				Spec:    tt.fields.Spec,
				AST:     tt.fields.AST,
				Query:   tt.fields.Query,
				Type:    tt.fields.Type,
				Dialect: tt.fields.Dialect,
				Org:     tt.fields.org,
			}
			if got := r.WithDefaults(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryRequest.WithDefaults() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryRequest_Validate(t *testing.T) {
	type fields struct {
		Spec    *flux.Spec
		AST     *ast.Package
		Query   string
		Type    string
		Dialect QueryDialect
		org     *platform.Organization
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "requires query, spec, or ast",
			fields: fields{
				Type: "flux",
			},
			wantErr: true,
		},
		{
			name: "requires flux type",
			fields: fields{
				Query: "howdy",
				Type:  "doody",
			},
			wantErr: true,
		},
		{
			name: "comment must be a single character",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					CommentPrefix: "error!",
				},
			},
			wantErr: true,
		},
		{
			name: "delimiter must be a single character",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter: "",
				},
			},
			wantErr: true,
		},
		{
			name: "characters must be unicode runes",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter: string([]byte{0x80}),
				},
			},
			wantErr: true,
		},
		{
			name: "unknown annotations",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:   ",",
					Annotations: []string{"error"},
				},
			},
			wantErr: true,
		},
		{
			name: "unknown date time format",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "error",
				},
			},
			wantErr: true,
		},
		{
			name: "valid query",
			fields: fields{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := QueryRequest{
				Spec:    tt.fields.Spec,
				AST:     tt.fields.AST,
				Query:   tt.fields.Query,
				Type:    tt.fields.Type,
				Dialect: tt.fields.Dialect,
				Org:     tt.fields.org,
			}
			if err := r.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("QueryRequest.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_toSpec(t *testing.T) {
	type args struct {
		p   *ast.Package
		now func() time.Time
	}
	tests := []struct {
		name    string
		args    args
		want    *flux.Spec
		wantErr bool
	}{
		{
			name: "ast converts to spec",
			args: args{
				p:   &ast.Package{},
				now: func() time.Time { return time.Unix(0, 0) },
			},
			want: &flux.Spec{
				Now: time.Unix(0, 0).UTC(),
			},
		},
		{
			name: "bad semantics error",
			args: args{
				p: &ast.Package{
					Files: []*ast.File{{
						Body: []ast.Statement{
							&ast.ReturnStatement{},
						},
					}},
				},
				now: func() time.Time { return time.Unix(0, 0) },
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got, err := toSpec(tt.args.p, tt.args.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("toSpec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toSpec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQueryRequest_proxyRequest(t *testing.T) {
	type fields struct {
		Spec    *flux.Spec
		AST     *ast.Package
		Query   string
		Type    string
		Dialect QueryDialect
		org     *platform.Organization
	}
	tests := []struct {
		name    string
		fields  fields
		now     func() time.Time
		want    *query.ProxyRequest
		wantErr bool
	}{
		{
			name: "requires query, spec, or ast",
			fields: fields{
				Type: "flux",
			},
			wantErr: true,
		},
		{
			name: "valid query",
			fields: fields{
				Query: "howdy",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				org: &platform.Organization{},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "howdy",
					},
				},
				Dialect: &csv.Dialect{
					ResultEncoderConfig: csv.ResultEncoderConfig{
						NoHeader:  false,
						Delimiter: ',',
					},
				},
			},
		},
		{
			name: "valid AST",
			fields: fields{
				AST:  &ast.Package{},
				Type: "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				org: &platform.Organization{},
			},
			now: func() time.Time { return time.Unix(0, 0).UTC() },
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.SpecCompiler{
						Spec: &flux.Spec{
							Now: time.Unix(0, 0).UTC(),
						},
					},
				},
				Dialect: &csv.Dialect{
					ResultEncoderConfig: csv.ResultEncoderConfig{
						NoHeader:  false,
						Delimiter: ',',
					},
				},
			},
		},
		{
			name: "valid spec",
			fields: fields{
				Type: "flux",
				Spec: &flux.Spec{
					Now: time.Unix(0, 0).UTC(),
				},
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				org: &platform.Organization{},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.SpecCompiler{
						Spec: &flux.Spec{
							Now: time.Unix(0, 0).UTC(),
						},
					},
				},
				Dialect: &csv.Dialect{
					ResultEncoderConfig: csv.ResultEncoderConfig{
						NoHeader:  false,
						Delimiter: ',',
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := QueryRequest{
				Spec:    tt.fields.Spec,
				AST:     tt.fields.AST,
				Query:   tt.fields.Query,
				Type:    tt.fields.Type,
				Dialect: tt.fields.Dialect,
				Org:     tt.fields.org,
			}
			got, err := r.proxyRequest(tt.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryRequest.ProxyRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("QueryRequest.ProxyRequest() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_decodeQueryRequest(t *testing.T) {
	type args struct {
		ctx context.Context
		r   *http.Request
		svc platform.OrganizationService
	}
	tests := []struct {
		name    string
		args    args
		want    *QueryRequest
		wantErr bool
	}{
		{
			name: "valid query request",
			args: args{
				r: httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"query": "from()"}`)),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &QueryRequest{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
					Header:         func(x bool) *bool { return &x }(true),
				},
				Org: &platform.Organization{
					ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
				},
			},
		},
		{
			name: "valid query request with explict content-type",
			args: args{
				r: func() *http.Request {
					r := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"query": "from()"}`))
					r.Header.Set("Content-Type", "application/json")
					return r
				}(),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &QueryRequest{
				Query: "from()",
				Type:  "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
					Header:         func(x bool) *bool { return &x }(true),
				},
				Org: &platform.Organization{
					ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
				},
			},
		},
		{
			name: "error decoding json",
			args: args{
				r: httptest.NewRequest("POST", "/", bytes.NewBufferString(`error`)),
			},
			wantErr: true,
		},
		{
			name: "error validating query",
			args: args{
				r: httptest.NewRequest("POST", "/", bytes.NewBufferString(`{}`)),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeQueryRequest(tt.args.ctx, tt.args.r, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeQueryRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeQueryRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeProxyQueryRequest(t *testing.T) {
	type args struct {
		ctx  context.Context
		r    *http.Request
		auth *platform.Authorization
		svc  platform.OrganizationService
	}
	tests := []struct {
		name    string
		args    args
		want    *query.ProxyRequest
		wantErr bool
	}{
		{
			name: "valid post query request",
			args: args{
				r: httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"query": "from()"}`)),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: &csv.Dialect{
					ResultEncoderConfig: csv.ResultEncoderConfig{
						NoHeader:  false,
						Delimiter: ',',
					},
				},
			},
		},
		{
			name: "valid post vnd.flux query request",
			args: args{
				r: func() *http.Request {
					r := httptest.NewRequest("POST", "/api/v2/query?org=myorg", strings.NewReader(`from(bucket: "mybucket")`))
					r.Header.Set("Content-Type", "application/vnd.flux")
					return r
				}(),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: func() platform.ID { s, _ := platform.IDFromString("deadbeefdeadbeef"); return *s }(),
					Compiler: lang.FluxCompiler{
						Query: "from(bucket: \"mybucket\")",
					},
				},
				Dialect: &csv.Dialect{
					ResultEncoderConfig: csv.ResultEncoderConfig{
						NoHeader:  false,
						Delimiter: ',',
					},
				},
			},
		},
	}
	var cmpOptions = cmp.Options{
		cmpopts.IgnoreUnexported(query.ProxyRequest{}),
		cmpopts.IgnoreUnexported(query.Request{}),
		cmpopts.EquateEmpty(),
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeProxyQueryRequest(tt.args.ctx, tt.args.r, tt.args.auth, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeProxyQueryRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
				t.Errorf("decodeProxyQueryRequest() = got/want %v", diff)
			}
		})
	}
}
