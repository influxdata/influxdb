//lint:file-ignore U1000 this error seems to be misreporting
package legacy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/influxql"
	imock "github.com/influxdata/influxdb/v2/influxql/mock"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"go.uber.org/zap/zaptest"
)

var cmpOpts = []cmp.Option{
	// Ignore request ID when comparing headers.
	cmp.Comparer(func(h1, h2 http.Header) bool {
		for k, v1 := range h1 {
			if k == "X-Request-Id" || k == "Request-Id" {
				continue
			}
			if v2, ok := h2[k]; !ok || !cmp.Equal(v1, v2) {
				return false
			}
		}
		for k, v2 := range h2 {
			if k == "X-Request-Id" || k == "Request-Id" {
				continue
			}
			if v1, ok := h1[k]; !ok || !cmp.Equal(v2, v1) {
				return false
			}
		}
		return true
	}),
}

func TestInfluxQLdHandler_HandleQuery(t *testing.T) {
	ctx := context.Background()

	type fields struct {
		OrganizationService platform.OrganizationService
		ProxyQueryService   influxql.ProxyQueryService
	}
	type args struct {
		w *httptest.ResponseRecorder
		r *http.Request
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		context    context.Context
		wantCode   int
		wantHeader http.Header
		wantBody   []byte
	}{
		{
			name: "no token causes http error",
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantCode: http.StatusInternalServerError,
			wantHeader: http.Header{
				"X-Platform-Error-Code": {"internal error"},
				"Content-Type":          {"application/json; charset=utf-8"},
			},
			wantBody: []byte(`{"code":"internal error","message":"authorizer not found on context"}`),
		},
		{
			name:    "inactive authorizer",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Inactive}),
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantCode: http.StatusForbidden,
			wantHeader: http.Header{
				"Content-Type":          {"application/json; charset=utf-8"},
				"X-Platform-Error-Code": {"forbidden"},
			},
			wantBody: []byte(`{"code":"forbidden","message":"insufficient permissions"}`),
		},
		{
			name:    "unknown organization",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Active}),
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return nil, &errors.Error{
							Code: errors.EForbidden,
							Msg:  "nope",
						}
					},
				},
			},
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantCode: http.StatusForbidden,
			wantHeader: http.Header{
				"Content-Type":          {"application/json; charset=utf-8"},
				"X-Platform-Error-Code": {"forbidden"},
			},
			wantBody: []byte(`{"code":"forbidden","message":"nope"}`),
		},
		{
			name:    "bad query",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Active}),
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{}, nil
					},
				},
				ProxyQueryService: &imock.ProxyQueryService{
					QueryF: func(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error) {
						return influxql.Statistics{}, &errors.Error{
							Code: errors.EUnprocessableEntity,
							Msg:  "bad query",
						}
					},
				},
			},
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantCode: http.StatusUnprocessableEntity,
			wantHeader: http.Header{
				"X-Platform-Error-Code": {"unprocessable entity"},
				"Content-Type":          {"application/json; charset=utf-8"},
			},
			wantBody: []byte(`{"code":"unprocessable entity","message":"bad query"}`),
		},
		{
			name:    "query fails during write",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Active}),
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{}, nil
					},
				},
				ProxyQueryService: &imock.ProxyQueryService{
					QueryF: func(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error) {
						_, _ = io.WriteString(w, "fail")
						return influxql.Statistics{}, &errors.Error{
							Code: errors.EInternal,
							Msg:  "during query",
						}
					},
				},
			},
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantBody: []byte("fail"),
			wantCode: http.StatusOK,
			wantHeader: http.Header{
				"Content-Type": {"application/json"},
			},
		},
		{
			name:    "good query unknown accept header",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Active}),
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{}, nil
					},
				},
				ProxyQueryService: &imock.ProxyQueryService{
					QueryF: func(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error) {
						_, err := io.WriteString(w, "good")
						return influxql.Statistics{}, err
					},
				},
			},
			args: args{
				r: WithHeader(httptest.NewRequest("POST", "/query", nil).WithContext(ctx), "Accept", "application/foo"),
				w: httptest.NewRecorder(),
			},
			wantBody: []byte("good"),
			wantCode: http.StatusOK,
			wantHeader: http.Header{
				"Content-Type": {"application/json"},
			},
		},
		{
			name:    "good query",
			context: pcontext.SetAuthorizer(ctx, &platform.Authorization{Status: platform.Active}),
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
						return &platform.Organization{}, nil
					},
				},
				ProxyQueryService: &imock.ProxyQueryService{
					QueryF: func(ctx context.Context, w io.Writer, req *influxql.QueryRequest) (influxql.Statistics, error) {
						_, err := io.WriteString(w, "good")
						return influxql.Statistics{}, err
					},
				},
			},
			args: args{
				r: httptest.NewRequest("POST", "/query", nil).WithContext(ctx),
				w: httptest.NewRecorder(),
			},
			wantBody: []byte("good"),
			wantCode: http.StatusOK,
			wantHeader: http.Header{
				"Content-Type": {"application/json"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &InfluxQLBackend{
				HTTPErrorHandler:      kithttp.ErrorHandler(0),
				OrganizationService:   tt.fields.OrganizationService,
				InfluxqldQueryService: tt.fields.ProxyQueryService,
			}

			h := NewInfluxQLHandler(b, HandlerConfig{})
			h.Logger = zaptest.NewLogger(t)

			if tt.context != nil {
				tt.args.r = tt.args.r.WithContext(tt.context)
			}

			tt.args.r.Header.Add("Content-Type", "application/vnd.influxql")

			h.handleInfluxqldQuery(tt.args.w, tt.args.r)

			if got, want := tt.args.w.Code, tt.wantCode; got != want {
				t.Errorf("HandleQuery() status code = got %d / want %d", got, want)
			}

			if got, want := tt.args.w.Result().Header, tt.wantHeader; !cmp.Equal(got, want, cmpOpts...) {
				t.Errorf("HandleQuery() headers = got(-)/want(+) %s", cmp.Diff(got, want))
			}

			if got, want := tt.args.w.Body.Bytes(), tt.wantBody; !cmp.Equal(got, want) {
				t.Errorf("HandleQuery() body = got(-)/want(+) %s", cmp.Diff(string(got), string(want)))
			}
		})
	}
}

func WithHeader(r *http.Request, key, value string) *http.Request {
	r.Header.Set(key, value)
	return r
}
