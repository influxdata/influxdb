package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb/v2"
	_ "github.com/influxdata/influxdb/v2/fluxinit/static"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
)

var cmpOptions = cmp.Options{
	cmpopts.IgnoreTypes(ast.BaseNode{}),
	cmpopts.IgnoreUnexported(query.ProxyRequest{}),
	cmpopts.IgnoreUnexported(query.Request{}),
	cmpopts.IgnoreUnexported(flux.Spec{}),
	cmpopts.EquateEmpty(),
}

func TestQueryRequest_WithDefaults(t *testing.T) {
	type fields struct {
		Spec    *flux.Spec
		AST     json.RawMessage
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
		Extern  json.RawMessage
		AST     json.RawMessage
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
				Extern:  tt.fields.Extern,
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

func TestQueryRequest_proxyRequest(t *testing.T) {
	type fields struct {
		Extern  json.RawMessage
		Spec    *flux.Spec
		AST     json.RawMessage
		Query   string
		Type    string
		Dialect QueryDialect
		Now     time.Time
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
			now: func() time.Time { return time.Unix(1, 1) },
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Now:   time.Unix(1, 1),
						Query: `howdy`,
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
				AST:  mustMarshal(&ast.Package{}),
				Type: "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				Now: time.Unix(1, 1),
				org: &platform.Organization{},
			},
			now: func() time.Time { return time.Unix(2, 2) },
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.ASTCompiler{
						AST: mustMarshal(&ast.Package{}),
						Now: time.Unix(1, 1),
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
			name: "valid AST with calculated now",
			fields: fields{
				AST:  mustMarshal(&ast.Package{}),
				Type: "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				org: &platform.Organization{},
			},
			now: func() time.Time { return time.Unix(2, 2) },
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.ASTCompiler{
						AST: mustMarshal(&ast.Package{}),
						Now: time.Unix(2, 2),
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
			name: "valid AST with extern",
			fields: fields{
				Extern: mustMarshal(&ast.File{
					Body: []ast.Statement{
						&ast.OptionStatement{
							Assignment: &ast.VariableAssignment{
								ID:   &ast.Identifier{Name: "x"},
								Init: &ast.IntegerLiteral{Value: 0},
							},
						},
					},
				}),
				AST:  mustMarshal(&ast.Package{}),
				Type: "flux",
				Dialect: QueryDialect{
					Delimiter:      ",",
					DateTimeFormat: "RFC3339",
				},
				org: &platform.Organization{},
			},
			now: func() time.Time { return time.Unix(1, 1) },
			want: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.ASTCompiler{
						Extern: mustMarshal(&ast.File{
							Body: []ast.Statement{
								&ast.OptionStatement{
									Assignment: &ast.VariableAssignment{
										ID:   &ast.Identifier{Name: "x"},
										Init: &ast.IntegerLiteral{Value: 0},
									},
								},
							},
						}),
						AST: mustMarshal(&ast.Package{}),
						Now: time.Unix(1, 1),
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
				Extern:  tt.fields.Extern,
				AST:     tt.fields.AST,
				Query:   tt.fields.Query,
				Type:    tt.fields.Type,
				Dialect: tt.fields.Dialect,
				Now:     tt.fields.Now,
				Org:     tt.fields.org,
			}
			got, err := r.proxyRequest(tt.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("QueryRequest.ProxyRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(got, tt.want, cmpOptions...) {
				t.Errorf("QueryRequest.ProxyRequest() -want/+got\n%s", cmp.Diff(tt.want, got, cmpOptions...))
			}
		})
	}
}

func mustMarshal(p ast.Node) []byte {
	bs, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return bs
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
							ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
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
					ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
				},
			},
		},
		{
			name: "valid query request with explicit content-type",
			args: args{
				r: func() *http.Request {
					r := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"query": "from()"}`))
					r.Header.Set("Content-Type", "application/json")
					return r
				}(),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
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
					ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
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
			got, _, err := decodeQueryRequest(tt.args.ctx, tt.args.r, tt.args.svc)
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
	externJSON := `{
		"type": "File",
		"body": [
			{
				"type": "OptionStatement",
				"assignment": {
					"type": "VariableAssignment",
					"id": {
						"type": "Identifier",
						"name": "x"
					},
					"init": {
						"type": "IntegerLiteral",
						"value": "0"
					}
				}
			}
		]
	}`
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
							ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
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
			name: "valid query including extern definition",
			args: args{
				r: httptest.NewRequest("POST", "/", bytes.NewBufferString(`
{
	"extern": `+externJSON+`,
	"query": "from(bucket: \"mybucket\")"
}
`)),
				svc: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{
							ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
					Compiler: lang.FluxCompiler{
						Extern: []byte(externJSON),
						Query:  `from(bucket: "mybucket")`,
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
							ID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
						}, nil
					},
				},
			},
			want: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: func() platform2.ID { s, _ := platform2.IDFromString("deadbeefdeadbeef"); return *s }(),
					Compiler: lang.FluxCompiler{
						Query: `from(bucket: "mybucket")`,
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
	cmpOptions := append(cmpOptions,
		cmpopts.IgnoreFields(lang.ASTCompiler{}, "Now"),
		cmpopts.IgnoreFields(lang.FluxCompiler{}, "Now"),
	)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := decodeProxyQueryRequest(tt.args.ctx, tt.args.r, tt.args.auth, tt.args.svc)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeProxyQueryRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !cmp.Equal(tt.want, got, cmpOptions...) {
				t.Errorf("decodeProxyQueryRequest() -want/+got\n%s", cmp.Diff(tt.want, got, cmpOptions...))
			}
		})
	}
}

func TestProxyRequestToQueryRequest_Compilers(t *testing.T) {
	tests := []struct {
		name string
		pr   query.ProxyRequest
		want QueryRequest
	}{
		{
			name: "flux compiler copied",
			pr: query.ProxyRequest{
				Dialect: &query.NoContentDialect{},
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: `howdy`,
						Now:   time.Unix(45, 45),
					},
				},
			},
			want: QueryRequest{
				Type:            "flux",
				Query:           `howdy`,
				PreferNoContent: true,
				Now:             time.Unix(45, 45),
			},
		},
		{
			name: "AST compiler copied",
			pr: query.ProxyRequest{
				Dialect: &query.NoContentDialect{},
				Request: query.Request{
					Compiler: lang.ASTCompiler{
						Now: time.Unix(45, 45),
						AST: mustMarshal(&ast.Package{}),
					},
				},
			},
			want: QueryRequest{
				Type:            "flux",
				PreferNoContent: true,
				AST:             mustMarshal(&ast.Package{}),
				Now:             time.Unix(45, 45),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got, err := QueryRequestFromProxyRequest(&tt.pr)
			if err != nil {
				t.Error(err)
			} else if !reflect.DeepEqual(*got, tt.want) {
				t.Errorf("QueryRequestFromProxyRequest = %v, want %v", got, tt.want)
			}
		})
	}
}
