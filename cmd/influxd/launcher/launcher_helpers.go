package launcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	nethttp "net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/influxdata/influxdb/v2/query"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// TestLauncher is a test wrapper for launcher.Launcher.
type TestLauncher struct {
	*Launcher

	// Root temporary directory for all data.
	Path string

	// Initialized after calling the Setup() helper.
	User   *platform.User
	Org    *platform.Organization
	Bucket *platform.Bucket
	Auth   *platform.Authorization

	httpClient *httpc.Client

	// Standard in/out/err buffers.
	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewTestLauncher returns a new instance of TestLauncher.
func NewTestLauncher() *TestLauncher {
	l := &TestLauncher{Launcher: NewLauncher()}
	l.Launcher.Stdin = &l.Stdin
	l.Launcher.Stdout = &l.Stdout
	l.Launcher.Stderr = &l.Stderr
	if testing.Verbose() {
		l.Launcher.Stdout = io.MultiWriter(l.Launcher.Stdout, os.Stdout)
		l.Launcher.Stderr = io.MultiWriter(l.Launcher.Stderr, os.Stderr)
	}

	path, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	l.Path = path
	return l
}

// RunTestLauncherOrFail initializes and starts the server.
func RunTestLauncherOrFail(tb testing.TB, ctx context.Context, args ...string) *TestLauncher {
	tb.Helper()
	l := NewTestLauncher()

	if err := l.Run(ctx, args...); err != nil {
		tb.Fatal(err)
	}
	return l
}

// Run executes the program with additional arguments to set paths and ports.
// Passed arguments will overwrite/add to the default ones.
func (tl *TestLauncher) Run(ctx context.Context, args ...string) error {
	largs := make([]string, 0, len(args)+8)
	largs = append(largs, "--bolt-path", filepath.Join(tl.Path, bolt.DefaultFilename))
	largs = append(largs, "--engine-path", filepath.Join(tl.Path, "engine"))
	largs = append(largs, "--http-bind-address", "127.0.0.1:0")
	largs = append(largs, "--log-level", "debug")
	largs = append(largs, args...)
	return tl.Launcher.Run(ctx, largs...)
}

// Shutdown stops the program and cleans up temporary paths.
func (tl *TestLauncher) Shutdown(ctx context.Context) error {
	tl.Cancel()
	tl.Launcher.Shutdown(ctx)
	return os.RemoveAll(tl.Path)
}

// ShutdownOrFail stops the program and cleans up temporary paths. Fail on error.
func (tl *TestLauncher) ShutdownOrFail(tb testing.TB, ctx context.Context) {
	tb.Helper()
	if err := tl.Shutdown(ctx); err != nil {
		tb.Fatal(err)
	}
}

// Setup creates a new user, bucket, org, and auth token.
func (tl *TestLauncher) Setup() error {
	results, err := tl.OnBoard(&platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	})
	if err != nil {
		return err
	}

	tl.User = results.User
	tl.Org = results.Org
	tl.Bucket = results.Bucket
	tl.Auth = results.Auth
	return nil
}

// SetupOrFail creates a new user, bucket, org, and auth token. Fail on error.
func (tl *TestLauncher) SetupOrFail(tb testing.TB) {
	if err := tl.Setup(); err != nil {
		tb.Fatal(err)
	}
}

// OnBoard attempts an on-boarding request.
// The on-boarding status is also reset to allow multiple user/org/buckets to be created.
func (tl *TestLauncher) OnBoard(req *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	res, err := tl.KeyValueService().OnboardInitialUser(context.Background(), req)
	if err != nil {
		return nil, err
	}
	err = tl.KeyValueService().PutOnboardingStatus(context.Background(), false)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// OnBoardOrFail attempts an on-boarding request or fails on error.
// The on-boarding status is also reset to allow multiple user/org/buckets to be created.
func (tl *TestLauncher) OnBoardOrFail(tb testing.TB, req *platform.OnboardingRequest) *platform.OnboardingResults {
	tb.Helper()
	res, err := tl.OnBoard(req)
	if err != nil {
		tb.Fatal(err)
	}
	return res
}

// WriteOrFail attempts a write to the organization and bucket identified by to or fails if there is an error.
func (tl *TestLauncher) WriteOrFail(tb testing.TB, to *platform.OnboardingResults, data string) {
	tb.Helper()
	resp, err := nethttp.DefaultClient.Do(tl.NewHTTPRequestOrFail(tb, "POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", to.Org.ID, to.Bucket.ID), to.Auth.Token, data))
	if err != nil {
		tb.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		tb.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		tb.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		tb.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}
}

// WritePoints attempts a write to the organization and bucket used during setup.
func (tl *TestLauncher) WritePoints(data string) error {
	req, err := tl.NewHTTPRequest(
		"POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", tl.Org.ID, tl.Bucket.ID),
		tl.Auth.Token, data)
	if err != nil {
		return err
	}
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if err := resp.Body.Close(); err != nil {
		return err
	}
	if resp.StatusCode != nethttp.StatusNoContent {
		return fmt.Errorf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}
	return nil
}

// WritePointsOrFail attempts a write to the organization and bucket used during setup or fails if there is an error.
func (tl *TestLauncher) WritePointsOrFail(tb testing.TB, data string) {
	tb.Helper()
	if err := tl.WritePoints(data); err != nil {
		tb.Fatal(err)
	}
}

// MustExecuteQuery executes the provided query panicking if an error is encountered.
// Callers of MustExecuteQuery must call Done on the returned QueryResults.
func (tl *TestLauncher) MustExecuteQuery(query string) *QueryResults {
	results, err := tl.ExecuteQuery(query)
	if err != nil {
		panic(err)
	}
	return results
}

// ExecuteQuery executes the provided query against the ith query node.
// Callers of ExecuteQuery must call Done on the returned QueryResults.
func (tl *TestLauncher) ExecuteQuery(q string) (*QueryResults, error) {
	ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(true, nil))
	ctx, _ = feature.Annotate(ctx, tl.flagger)
	fq, err := tl.QueryController().Query(ctx, &query.Request{
		Authorization:  tl.Auth,
		OrganizationID: tl.Auth.OrgID,
		Compiler: lang.FluxCompiler{
			Query: q,
		}})
	if err != nil {
		return nil, err
	}

	results := make([]flux.Result, 0, 1)
	for res := range fq.Results() {
		results = append(results, res)
	}

	if err := fq.Err(); err != nil {
		fq.Done()
		return nil, err
	}

	return &QueryResults{
		Results: results,
		Query:   fq,
	}, nil
}

// QueryAndConsume queries InfluxDB using the request provided. It uses a function to consume the results obtained.
// It returns the first error encountered when requesting the query, consuming the results, or executing the query.
func (tl *TestLauncher) QueryAndConsume(ctx context.Context, req *query.Request, fn func(r flux.Result) error) error {
	res, err := tl.FluxQueryService().Query(ctx, req)
	if err != nil {
		return err
	}
	// iterate over results to populate res.Err()
	var gotErr error
	for res.More() {
		if err := fn(res.Next()); gotErr == nil {
			gotErr = err
		}
	}
	if gotErr != nil {
		return gotErr
	}
	return res.Err()
}

// QueryAndNopConsume does the same as QueryAndConsume but consumes results with a nop function.
func (tl *TestLauncher) QueryAndNopConsume(ctx context.Context, req *query.Request) error {
	return tl.QueryAndConsume(ctx, req, func(r flux.Result) error {
		return r.Tables().Do(func(table flux.Table) error {
			return nil
		})
	})
}

// FluxQueryOrFail performs a query to the specified organization and returns the results
// or fails if there is an error.
func (tl *TestLauncher) FluxQueryOrFail(tb testing.TB, org *platform.Organization, token string, query string) string {
	tb.Helper()

	b, err := http.SimpleQuery(tl.URL(), query, org.Name, token)
	if err != nil {
		tb.Fatal(err)
	}

	return string(b)
}

// QueryFlux returns the csv response from a flux query.
// It also removes all the \r to make it easier to write tests.
func (tl *TestLauncher) QueryFlux(tb testing.TB, org *platform.Organization, token, query string) string {
	tb.Helper()

	b, err := http.SimpleQuery(tl.URL(), query, org.Name, token)
	if err != nil {
		tb.Fatal(err)
	}

	// remove all \r as well as the extra terminating \n
	b = bytes.ReplaceAll(b, []byte("\r"), nil)
	return string(b[:len(b)-1])
}

// MustNewHTTPRequest returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (tl *TestLauncher) MustNewHTTPRequest(method, rawurl, body string) *nethttp.Request {
	req, err := nethttp.NewRequest(method, tl.URL()+rawurl, strings.NewReader(body))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", "Token "+tl.Auth.Token)
	return req
}

// NewHTTPRequest returns a new nethttp.Request with base URL and auth attached.
func (tl *TestLauncher) NewHTTPRequest(method, rawurl, token string, body string) (*nethttp.Request, error) {
	req, err := nethttp.NewRequest(method, tl.URL()+rawurl, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+token)
	return req, nil
}

// NewHTTPRequestOrFail returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (tl *TestLauncher) NewHTTPRequestOrFail(tb testing.TB, method, rawurl, token string, body string) *nethttp.Request {
	tb.Helper()
	req, err := tl.NewHTTPRequest(method, rawurl, token, body)
	if err != nil {
		tb.Fatal(err)
	}
	return req
}

// Services

func (tl *TestLauncher) FluxService() *http.FluxService {
	return &http.FluxService{Addr: tl.URL(), Token: tl.Auth.Token}
}

func (tl *TestLauncher) FluxQueryService() *http.FluxQueryService {
	return &http.FluxQueryService{Addr: tl.URL(), Token: tl.Auth.Token}
}

func (tl *TestLauncher) BucketService(tb testing.TB) *http.BucketService {
	tb.Helper()
	return &http.BucketService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) CheckService() platform.CheckService {
	return tl.kvService
}

func (tl *TestLauncher) DashboardService(tb testing.TB) *http.DashboardService {
	tb.Helper()
	return &http.DashboardService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) LabelService(tb testing.TB) *http.LabelService {
	tb.Helper()
	return &http.LabelService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) NotificationEndpointService(tb testing.TB) *http.NotificationEndpointService {
	tb.Helper()
	return http.NewNotificationEndpointService(tl.HTTPClient(tb))
}

func (tl *TestLauncher) NotificationRuleService() platform.NotificationRuleStore {
	return tl.kvService
}

func (tl *TestLauncher) OrgService(tb testing.TB) platform.OrganizationService {
	return tl.kvService
}

func (tl *TestLauncher) PkgerService(tb testing.TB) pkger.SVC {
	return &pkger.HTTPRemoteService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) TaskServiceKV() platform.TaskService {
	return tl.kvService
}

func (tl *TestLauncher) TelegrafService(tb testing.TB) *http.TelegrafService {
	tb.Helper()
	return http.NewTelegrafService(tl.HTTPClient(tb))
}

func (tl *TestLauncher) VariableService(tb testing.TB) *http.VariableService {
	tb.Helper()
	return &http.VariableService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) AuthorizationService(tb testing.TB) *http.AuthorizationService {
	return &http.AuthorizationService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) TaskService(tb testing.TB) *http.TaskService {
	return &http.TaskService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) HTTPClient(tb testing.TB) *httpc.Client {
	tb.Helper()

	if tl.httpClient == nil {
		token := ""
		if tl.Auth != nil {
			token = tl.Auth.Token
		}
		client, err := http.NewHTTPClient(tl.URL(), token, false)
		if err != nil {
			tb.Fatal(err)
		}
		tl.httpClient = client
	}
	return tl.httpClient
}

func (tl *TestLauncher) Metrics(tb testing.TB) (metrics map[string]*dto.MetricFamily) {
	req := tl.HTTPClient(tb).
		Get("/metrics").
		RespFn(func(resp *nethttp.Response) error {
			if resp.StatusCode != nethttp.StatusOK {
				return fmt.Errorf("unexpected status code: %d %s", resp.StatusCode, resp.Status)
			}
			defer func() { _ = resp.Body.Close() }()

			var parser expfmt.TextParser
			metrics, _ = parser.TextToMetricFamilies(resp.Body)
			return nil
		})
	if err := req.Do(context.Background()); err != nil {
		tb.Fatal(err)
	}
	return metrics
}

// QueryResult wraps a single flux.Result with some helper methods.
type QueryResult struct {
	t *testing.T
	q flux.Result
}

// HasTableWithCols checks if the desired number of tables and columns exist,
// ignoring any system columns.
//
// If the result is not as expected then the testing.T fails.
func (r *QueryResult) HasTablesWithCols(want []int) {
	r.t.Helper()

	// _start, _stop, _time, _f
	systemCols := 4
	got := []int{}
	if err := r.q.Tables().Do(func(b flux.Table) error {
		got = append(got, len(b.Cols())-systemCols)
		b.Do(func(c flux.ColReader) error { return nil })
		return nil
	}); err != nil {
		r.t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		r.t.Fatalf("got %v, expected %v", got, want)
	}
}

// TablesN returns the number of tables for the result.
func (r *QueryResult) TablesN() int {
	var total int
	r.q.Tables().Do(func(b flux.Table) error {
		total++
		b.Do(func(c flux.ColReader) error { return nil })
		return nil
	})
	return total
}

// QueryResults wraps a set of query results with some helper methods.
type QueryResults struct {
	Results []flux.Result
	Query   flux.Query
}

func (r *QueryResults) Done() {
	r.Query.Done()
}

// First returns the first QueryResult. When there are not exactly 1 table First
// will fail.
func (r *QueryResults) First(t *testing.T) *QueryResult {
	r.HasTableCount(t, 1)
	for _, result := range r.Results {
		return &QueryResult{t: t, q: result}
	}
	return nil
}

// HasTableCount asserts that there are n tables in the result.
func (r *QueryResults) HasTableCount(t *testing.T, n int) {
	if got, exp := len(r.Results), n; got != exp {
		t.Fatalf("result has %d tables, expected %d. Tables: %s", got, exp, r.Names())
	}
}

// Names returns the sorted set of result names for the query results.
func (r *QueryResults) Names() []string {
	if len(r.Results) == 0 {
		return nil
	}
	names := make([]string, len(r.Results), 0)
	for _, r := range r.Results {
		names = append(names, r.Name())
	}
	return names
}

// SortedNames returns the sorted set of table names for the query results.
func (r *QueryResults) SortedNames() []string {
	names := r.Names()
	sort.Strings(names)
	return names
}
