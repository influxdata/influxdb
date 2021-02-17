package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/feature"
	tracetesting "github.com/influxdata/influxdb/v2/kit/tracing/testing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	influxmock "github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/query/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	"go.uber.org/zap/zaptest"
)

func TestFluxService_Query(t *testing.T) {
	orgID, err := influxdb.IDFromString("abcdabcdabcdabcd")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		token   string
		ctx     context.Context
		r       *query.ProxyRequest
		status  int
		want    flux.Statistics
		wantW   string
		wantErr bool
	}{
		{
			name:  "query",
			ctx:   context.Background(),
			token: "mytoken",
			r: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: *orgID,
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status: http.StatusOK,
			want:   flux.Statistics{},
			wantW:  "howdy\n",
		},
		{
			name:  "missing org id",
			ctx:   context.Background(),
			token: "mytoken",
			r: &query.ProxyRequest{
				Request: query.Request{
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			wantErr: true,
		},
		{
			name:  "error status",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.ProxyRequest{
				Request: query.Request{
					OrganizationID: *orgID,
					Compiler: lang.FluxCompiler{
						Query: "from()",
					},
				},
				Dialect: csv.DefaultDialect(),
			},
			status:  http.StatusUnauthorized,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if reqID := r.URL.Query().Get(OrgID); reqID == "" {
					if name := r.URL.Query().Get(Org); name == "" {
						// Request must have org or orgID.
						kithttp.ErrorHandler(0).HandleHTTPError(context.TODO(), influxdb.ErrInvalidOrgFilter, w)
						return
					}
				}
				w.WriteHeader(tt.status)
				_, _ = fmt.Fprintln(w, "howdy")
			}))
			defer ts.Close()
			s := &FluxService{
				Addr:  ts.URL,
				Token: tt.token,
			}

			w := &bytes.Buffer{}
			got, err := s.Query(tt.ctx, w, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FluxService.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("FluxService.Query() = -want/+got: %v", diff)
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("FluxService.Query() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestFluxQueryService_Query(t *testing.T) {
	var orgID influxdb.ID
	orgID.DecodeFromString("aaaaaaaaaaaaaaaa")
	tests := []struct {
		name    string
		token   string
		ctx     context.Context
		r       *query.Request
		csv     string
		status  int
		want    string
		wantErr bool
	}{
		{
			name:  "error status",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.Request{
				OrganizationID: orgID,
				Compiler: lang.FluxCompiler{
					Query: "from()",
				},
			},
			status:  http.StatusUnauthorized,
			wantErr: true,
		},
		{
			name:  "returns csv",
			token: "mytoken",
			ctx:   context.Background(),
			r: &query.Request{
				OrganizationID: orgID,
				Compiler: lang.FluxCompiler{
					Query: "from()",
				},
			},
			status: http.StatusOK,
			csv: `#datatype,string,long,dateTime:RFC3339,double,long,string,boolean,string,string,string
#group,false,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,,
,result,table,_time,usage_user,test,mystr,this,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui
`,
			want: toCRLF(`,_result,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui

`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var orgIDStr string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				orgIDStr = r.URL.Query().Get(OrgID)
				w.WriteHeader(tt.status)
				fmt.Fprintln(w, tt.csv)
			}))
			s := &FluxQueryService{
				Addr:  ts.URL,
				Token: tt.token,
			}
			res, err := s.Query(tt.ctx, tt.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("FluxQueryService.Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if res != nil && res.Err() != nil {
				t.Errorf("FluxQueryService.Query() result error = %v", res.Err())
				return
			}
			if tt.wantErr {
				return
			}
			defer res.Release()

			enc := csv.NewMultiResultEncoder(csv.ResultEncoderConfig{
				NoHeader:  true,
				Delimiter: ',',
			})
			b := bytes.Buffer{}
			n, err := enc.Encode(&b, res)
			if err != nil {
				t.Errorf("FluxQueryService.Query() encode error = %v", err)
				return
			}
			if n != int64(len(tt.want)) {
				t.Errorf("FluxQueryService.Query() encode result = %d, want %d", n, len(tt.want))
			}
			if orgIDStr == "" {
				t.Error("FluxQueryService.Query() encoded orgID is empty")
			}
			if got, want := orgIDStr, tt.r.OrganizationID.String(); got != want {
				t.Errorf("FluxQueryService.Query() encoded orgID = %s, want %s", got, want)
			}

			got := b.String()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FluxQueryService.Query() =\n%s\n%s", got, tt.want)
			}
		})
	}
}

func TestFluxHandler_postFluxAST(t *testing.T) {
	tests := []struct {
		name   string
		w      *httptest.ResponseRecorder
		r      *http.Request
		want   string
		status int
	}{
		{
			name: "get ast from()",
			w:    httptest.NewRecorder(),
			r:    httptest.NewRequest("POST", "/api/v2/query/ast", bytes.NewBufferString(`{"query": "from()"}`)),
			want: `{"ast":{"type":"Package","package":"main","files":[{"type":"File","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"metadata":"parser-type=rust","package":null,"imports":null,"body":[{"type":"ExpressionStatement","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"expression":{"type":"CallExpression","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":7},"source":"from()"},"callee":{"type":"Identifier","location":{"start":{"line":1,"column":1},"end":{"line":1,"column":5},"source":"from"},"name":"from"}}}]}]}}
`,
			status: http.StatusOK,
		},
		{
			name:   "error from bad json",
			w:      httptest.NewRecorder(),
			r:      httptest.NewRequest("POST", "/api/v2/query/ast", bytes.NewBufferString(`error!`)),
			want:   `{"code":"invalid","message":"invalid json: invalid character 'e' looking for beginning of value"}`,
			status: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &FluxHandler{
				HTTPErrorHandler:    kithttp.ErrorHandler(0),
				FluxLanguageService: fluxlang.DefaultService,
			}
			h.postFluxAST(tt.w, tt.r)
			if got := tt.w.Body.String(); got != tt.want {
				t.Errorf("http.postFluxAST = got\n%vwant\n%v", got, tt.want)
			}
			if got := tt.w.Code; got != tt.status {
				t.Errorf("http.postFluxAST = got %d\nwant %d", got, tt.status)
			}
		})
	}
}

func TestFluxService_Check(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(HealthHandler))
	defer ts.Close()
	s := &FluxService{
		Addr: ts.URL,
	}
	got := s.Check(context.Background())
	want := check.Response{
		Name:    "influxdb",
		Status:  "pass",
		Message: "ready for queries and writes",
		Checks:  check.Responses{},
	}
	if !cmp.Equal(want, got) {
		t.Errorf("unexpected response -want/+got: " + cmp.Diff(want, got))
	}
}

func TestFluxQueryService_Check(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(HealthHandler))
	defer ts.Close()
	s := &FluxQueryService{
		Addr: ts.URL,
	}
	got := s.Check(context.Background())
	want := check.Response{
		Name:    "influxdb",
		Status:  "pass",
		Message: "ready for queries and writes",
		Checks:  check.Responses{},
	}
	if !cmp.Equal(want, got) {
		t.Errorf("unexpected response -want/+got: " + cmp.Diff(want, got))
	}
}

var crlfPattern = regexp.MustCompile(`\r?\n`)

func toCRLF(data string) string {
	return crlfPattern.ReplaceAllString(data, "\r\n")
}

type noopEventRecorder struct{}

func (noopEventRecorder) Record(context.Context, metric.Event) {}

var _ metric.EventRecorder = noopEventRecorder{}

// Certain error cases must be encoded as influxdb.Error so they can be properly decoded clientside.
func TestFluxHandler_PostQuery_Errors(t *testing.T) {
	defer tracetesting.SetupInMemoryTracing(t.Name())()

	store := NewTestInmemStore(t)
	orgSVC := tenant.NewService(tenant.NewStore(store))
	b := &FluxBackend{
		HTTPErrorHandler:    kithttp.ErrorHandler(0),
		log:                 zaptest.NewLogger(t),
		QueryEventRecorder:  noopEventRecorder{},
		OrganizationService: orgSVC,
		ProxyQueryService: &mock.ProxyQueryService{
			QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
				return flux.Statistics{}, &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "some query error",
				}
			},
		},
		FluxLanguageService: fluxlang.DefaultService,
		Flagger:             feature.DefaultFlagger(),
	}
	h := NewFluxHandler(zaptest.NewLogger(t), b)

	t.Run("missing authorizer", func(t *testing.T) {
		ts := httptest.NewServer(h)
		defer ts.Close()

		resp, err := http.Post(ts.URL+"/api/v2/query", "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}

		defer resp.Body.Close()

		if actual := resp.Header.Get("Trace-Id"); actual == "" {
			t.Error("expected trace ID header")
		}

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("expected unauthorized status, got %d", resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if !strings.Contains(ierr.Msg, "authorization is") {
			t.Fatalf("expected error to mention authorization, got %s", ierr.Msg)
		}
	})

	t.Run("authorizer but syntactically invalid JSON request", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, err := http.NewRequest("POST", "/api/v2/query", strings.NewReader("oops"))
		if err != nil {
			t.Fatal(err)
		}
		authz := &influxdb.Authorization{}
		req = req.WithContext(icontext.SetAuthorizer(req.Context(), authz))

		h.handleQuery(w, req)

		if actual := w.Header().Get("Trace-Id"); actual == "" {
			t.Error("expected trace ID header")
		}

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected bad request status, got %d", w.Code)
		}

		body := w.Body.Bytes()
		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if !strings.Contains(ierr.Msg, "decode request body") {
			t.Fatalf("expected error to mention decoding, got %s", ierr.Msg)
		}
	})

	t.Run("valid request but executing query results in client error", func(t *testing.T) {
		org := influxdb.Organization{Name: t.Name()}
		if err := orgSVC.CreateOrganization(context.Background(), &org); err != nil {
			t.Fatal(err)
		}

		req, err := http.NewRequest("POST", "/api/v2/query?orgID="+org.ID.String(), bytes.NewReader([]byte("buckets()")))
		if err != nil {
			t.Fatal(err)
		}
		authz := &influxdb.Authorization{}
		req = req.WithContext(icontext.SetAuthorizer(req.Context(), authz))
		req.Header.Set("Content-Type", "application/vnd.flux")

		w := httptest.NewRecorder()
		h.handleQuery(w, req)

		if actual := w.Header().Get("Trace-Id"); actual == "" {
			t.Error("expected trace ID header")
		}

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected bad request status, got %d", w.Code)
		}

		body := w.Body.Bytes()
		t.Logf("%s", body)
		var ierr influxdb.Error
		if err := json.Unmarshal(body, &ierr); err != nil {
			t.Logf("failed to json unmarshal into influxdb.error: %q", body)
			t.Fatal(err)
		}

		if got, want := ierr.Code, influxdb.EInvalid; got != want {
			t.Fatalf("unexpected error code -want/+got:\n\t- %v\n\t+ %v", want, got)
		}
		if ierr.Msg != "some query error" {
			t.Fatalf("expected error message to mention 'some query error', got %s", ierr.Err.Error())
		}
	})
}

func TestFluxService_Query_gzip(t *testing.T) {
	// orgService is just to mock out orgs by returning
	// the same org every time.
	orgService := &influxmock.OrganizationService{
		FindOrganizationByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID:   id,
				Name: id.String(),
			}, nil
		},

		FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID:   influxdb.ID(1),
				Name: influxdb.ID(1).String(),
			}, nil
		},
	}

	// queryService is test setup that returns the same CSV for all queries.
	queryService := &mock.ProxyQueryService{
		QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			_, _ = w.Write([]byte(`#datatype,string,long,dateTime:RFC3339,double,long,string,boolean,string,string,string
#group,false,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,,
,result,table,_time,usage_user,test,mystr,this,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui`))
			return flux.Statistics{}, nil
		},
	}

	// authService is yet more test setup that returns an operator auth for any token.
	authService := &influxmock.AuthorizationService{
		FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
			return &influxdb.Authorization{
				ID:          influxdb.ID(1),
				OrgID:       influxdb.ID(1),
				Permissions: influxdb.OperPermissions(),
			}, nil
		},
	}

	fluxBackend := &FluxBackend{
		HTTPErrorHandler:    kithttp.ErrorHandler(0),
		log:                 zaptest.NewLogger(t),
		QueryEventRecorder:  noopEventRecorder{},
		OrganizationService: orgService,
		ProxyQueryService:   queryService,
		FluxLanguageService: fluxlang.DefaultService,
		Flagger:             feature.DefaultFlagger(),
	}

	fluxHandler := NewFluxHandler(zaptest.NewLogger(t), fluxBackend)

	// fluxHandling expects authorization to be on the request context.
	// AuthenticationHandler extracts the token from headers and places
	// the auth on context.
	auth := NewAuthenticationHandler(zaptest.NewLogger(t), kithttp.ErrorHandler(0))
	auth.AuthorizationService = authService
	auth.Handler = fluxHandler
	auth.UserService = &influxmock.UserService{
		FindUserByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
			return &influxdb.User{}, nil
		},
	}

	ts := httptest.NewServer(auth)
	defer ts.Close()

	newFakeRequest := func() *http.Request {
		req, err := http.NewRequest("POST", ts.URL+"/api/v2/query?orgID=0000000000000001", bytes.NewReader([]byte("buckets()")))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/vnd.flux")
		SetToken("not important hard coded test response", req)
		return req
	}

	// disable any gzip compression
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true,
		},
	}

	req := newFakeRequest()
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("unable to POST to server: %v", err)
	}

	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected status code %s", res.Status)
	}

	identityBody, _ := ioutil.ReadAll(res.Body)
	_ = res.Body.Close()

	// now, we try to use gzip
	req = newFakeRequest()
	// If we enable compression, we should get the same response.
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: false,
		},
	}

	res, err = client.Do(req)
	if err != nil {
		t.Fatalf("unable to POST to server: %v", err)
	}

	gzippedBody, _ := ioutil.ReadAll(res.Body)
	_ = res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Errorf("unexpected status code %s", res.Status)
	}

	if string(identityBody) != string(gzippedBody) {
		t.Errorf("unexpected difference in identity and compressed bodies:\n%s\n%s", string(identityBody), string(gzippedBody))
	}
}

func Benchmark_Query_no_gzip(b *testing.B) {
	benchmarkQuery(b, true)
}

func Benchmark_Query_gzip(b *testing.B) {
	benchmarkQuery(b, false)
}

func benchmarkQuery(b *testing.B, disableCompression bool) {
	// orgService is just to mock out orgs by returning
	// the same org every time.
	orgService := &influxmock.OrganizationService{
		FindOrganizationByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID:   id,
				Name: id.String(),
			}, nil
		},

		FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID:   influxdb.ID(1),
				Name: influxdb.ID(1).String(),
			}, nil
		},
	}

	// queryService is test setup that returns the same CSV for all queries.
	queryService := &mock.ProxyQueryService{
		QueryF: func(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
			_, _ = w.Write([]byte(`#datatype,string,long,dateTime:RFC3339,double,long,string,boolean,string,string,string
#group,false,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,,
,result,table,_time,usage_user,test,mystr,this,cpu,host,_measurement
,,0,2018-08-29T13:08:47Z,10.2,10,yay,true,cpu-total,a,cpui`))
			return flux.Statistics{}, nil
		},
	}

	// authService is yet more test setup that returns an operator auth for any token.
	authService := &influxmock.AuthorizationService{
		FindAuthorizationByTokenFn: func(ctx context.Context, token string) (*influxdb.Authorization, error) {
			return &influxdb.Authorization{
				ID:          influxdb.ID(1),
				OrgID:       influxdb.ID(1),
				Permissions: influxdb.OperPermissions(),
			}, nil
		},
	}

	fluxBackend := &FluxBackend{
		HTTPErrorHandler:    kithttp.ErrorHandler(0),
		log:                 zaptest.NewLogger(b),
		QueryEventRecorder:  noopEventRecorder{},
		OrganizationService: orgService,
		ProxyQueryService:   queryService,
		FluxLanguageService: fluxlang.DefaultService,
		Flagger:             feature.DefaultFlagger(),
	}

	fluxHandler := NewFluxHandler(zaptest.NewLogger(b), fluxBackend)

	// fluxHandling expects authorization to be on the request context.
	// AuthenticationHandler extracts the token from headers and places
	// the auth on context.
	auth := NewAuthenticationHandler(zaptest.NewLogger(b), kithttp.ErrorHandler(0))
	auth.AuthorizationService = authService
	auth.Handler = fluxHandler

	ts := httptest.NewServer(auth)
	defer ts.Close()

	newFakeRequest := func() *http.Request {
		req, err := http.NewRequest("POST", ts.URL+"/api/v2/query?orgID=0000000000000001", bytes.NewReader([]byte("buckets()")))
		if err != nil {
			b.Fatal(err)
		}

		req.Header.Set("Content-Type", "application/vnd.flux")
		SetToken("not important hard coded test response", req)
		return req
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: disableCompression,
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := newFakeRequest()

		res, err := client.Do(req)
		if err != nil {
			b.Fatalf("unable to POST to server: %v", err)
		}

		if res.StatusCode != http.StatusOK {
			b.Errorf("unexpected status code %s", res.Status)
		}

		_, _ = ioutil.ReadAll(res.Body)
		_ = res.Body.Close()

	}
}
