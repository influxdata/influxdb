package launcher

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	nethttp "net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influx-cli/v2/api"
	"github.com/influxdata/influx-cli/v2/clients"
	clibackup "github.com/influxdata/influx-cli/v2/clients/backup"
	clirestore "github.com/influxdata/influx-cli/v2/clients/restore"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	dashboardTransport "github.com/influxdata/influxdb/v2/dashboards/transport"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/sqlite"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// TestLauncher is a test wrapper for launcher.Launcher.
type TestLauncher struct {
	*Launcher

	// Root temporary directory for all data.
	Path string

	// Initialized after calling the Setup() helper.
	User   *influxdb.User
	Org    *influxdb.Organization
	Bucket *influxdb.Bucket
	Auth   *influxdb.Authorization

	httpClient *httpc.Client
	apiClient  *api.APIClient

	// Flag to act as standard server: disk store, no-e2e testing flag
	realServer bool
}

// RunAndSetupNewLauncherOrFail shorcuts the most common pattern used in testing,
// building a new TestLauncher, running it, and setting it up with an initial user.
func RunAndSetupNewLauncherOrFail(ctx context.Context, tb testing.TB, setters ...OptSetter) *TestLauncher {
	tb.Helper()

	l := NewTestLauncher()
	l.RunOrFail(tb, ctx, setters...)
	defer func() {
		// If setup fails, shut down the launcher.
		if tb.Failed() {
			l.Shutdown(ctx)
		}
	}()
	l.SetupOrFail(tb)
	return l
}

// NewTestLauncher returns a new instance of TestLauncher.
func NewTestLauncher() *TestLauncher {
	l := &TestLauncher{Launcher: NewLauncher()}

	path, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	l.Path = path
	return l
}

// NewTestLauncherServer returns a new instance of TestLauncher configured as real server (disk store, no e2e flag).
func NewTestLauncherServer() *TestLauncher {
	l := NewTestLauncher()
	l.realServer = true
	return l
}

// URL returns the URL to connect to the HTTP server.
func (tl *TestLauncher) URL() *url.URL {
	u := url.URL{
		Host:   fmt.Sprintf("127.0.0.1:%d", tl.Launcher.httpPort),
		Scheme: "http",
	}
	if tl.Launcher.tlsEnabled {
		u.Scheme = "https"
	}
	return &u
}

type OptSetter = func(o *InfluxdOpts)

func (tl *TestLauncher) SetFlagger(flagger feature.Flagger) {
	tl.Launcher.flagger = flagger
}

// Run executes the program, failing the test if the launcher fails to start.
func (tl *TestLauncher) RunOrFail(tb testing.TB, ctx context.Context, setters ...OptSetter) {
	if err := tl.Run(tb, ctx, setters...); err != nil {
		tb.Fatal(err)
	}
}

// Run executes the program with additional arguments to set paths and ports.
// Passed arguments will overwrite/add to the default ones.
func (tl *TestLauncher) Run(tb zaptest.TestingT, ctx context.Context, setters ...OptSetter) error {
	opts := NewOpts(viper.New())
	if !tl.realServer {
		opts.StoreType = "memory"
		opts.Testing = true
	}
	opts.TestingAlwaysAllowSetup = true
	opts.BoltPath = filepath.Join(tl.Path, bolt.DefaultFilename)
	opts.SqLitePath = filepath.Join(tl.Path, sqlite.DefaultFilename)
	opts.EnginePath = filepath.Join(tl.Path, "engine")
	opts.HttpBindAddress = "127.0.0.1:0"
	opts.LogLevel = zap.DebugLevel
	opts.ReportingDisabled = true
	opts.ConcurrencyQuota = 32
	opts.QueueSize = 16

	for _, setter := range setters {
		setter(opts)
	}

	// Set up top-level logger to write into the test-case.
	tl.Launcher.log = zaptest.NewLogger(tb, zaptest.Level(opts.LogLevel)).With(zap.String("test_name", tb.Name()))
	return tl.Launcher.run(ctx, opts)
}

// Shutdown stops the program and cleans up temporary paths.
func (tl *TestLauncher) Shutdown(ctx context.Context) error {
	defer os.RemoveAll(tl.Path)
	tl.cancel()
	return tl.Launcher.Shutdown(ctx)
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
	results, err := tl.OnBoard(&influxdb.OnboardingRequest{
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
func (tl *TestLauncher) OnBoard(req *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	return tl.apibackend.OnboardingService.OnboardInitialUser(context.Background(), req)
}

// OnBoardOrFail attempts an on-boarding request or fails on error.
// The on-boarding status is also reset to allow multiple user/org/buckets to be created.
func (tl *TestLauncher) OnBoardOrFail(tb testing.TB, req *influxdb.OnboardingRequest) *influxdb.OnboardingResults {
	tb.Helper()
	res, err := tl.OnBoard(req)
	if err != nil {
		tb.Fatal(err)
	}
	return res
}

// WriteOrFail attempts a write to the organization and bucket identified by to or fails if there is an error.
func (tl *TestLauncher) WriteOrFail(tb testing.TB, to *influxdb.OnboardingResults, data string) {
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
func (tl *TestLauncher) FluxQueryOrFail(tb testing.TB, org *influxdb.Organization, token string, query string) string {
	tb.Helper()

	b, err := http.SimpleQuery(tl.URL(), query, org.Name, token)
	if err != nil {
		tb.Fatal(err)
	}

	return string(b)
}

// QueryFlux returns the csv response from a flux query.
// It also removes all the \r to make it easier to write tests.
func (tl *TestLauncher) QueryFlux(tb testing.TB, org *influxdb.Organization, token, query string) string {
	tb.Helper()

	b, err := http.SimpleQuery(tl.URL(), query, org.Name, token)
	if err != nil {
		tb.Fatal(err)
	}

	// remove all \r as well as the extra terminating \n
	b = bytes.ReplaceAll(b, []byte("\r"), nil)
	return string(b[:len(b)-1])
}

func (tl *TestLauncher) BackupOrFail(tb testing.TB, ctx context.Context, req clibackup.Params) {
	tb.Helper()
	require.NoError(tb, tl.Backup(tb, ctx, req))
}

func (tl *TestLauncher) Backup(tb testing.TB, ctx context.Context, req clibackup.Params) error {
	tb.Helper()
	return tl.BackupService(tb).Backup(ctx, &req)
}

func (tl *TestLauncher) RestoreOrFail(tb testing.TB, ctx context.Context, req clirestore.Params) {
	tb.Helper()
	require.NoError(tb, tl.Restore(tb, ctx, req))
}

func (tl *TestLauncher) Restore(tb testing.TB, ctx context.Context, req clirestore.Params) error {
	tb.Helper()
	return tl.RestoreService(tb).Restore(ctx, &req)
}

// MustNewHTTPRequest returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (tl *TestLauncher) MustNewHTTPRequest(method, rawurl, body string) *nethttp.Request {
	req, err := nethttp.NewRequest(method, tl.URL().String()+rawurl, strings.NewReader(body))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", "Token "+tl.Auth.Token)
	return req
}

// NewHTTPRequest returns a new nethttp.Request with base URL and auth attached.
func (tl *TestLauncher) NewHTTPRequest(method, rawurl, token string, body string) (*nethttp.Request, error) {
	req, err := nethttp.NewRequest(method, tl.URL().String()+rawurl, strings.NewReader(body))
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
	return &http.FluxService{Addr: tl.URL().String(), Token: tl.Auth.Token}
}

func (tl *TestLauncher) FluxQueryService() *http.FluxQueryService {
	return &http.FluxQueryService{Addr: tl.URL().String(), Token: tl.Auth.Token}
}

func (tl *TestLauncher) BucketService(tb testing.TB) *tenant.BucketClientService {
	tb.Helper()
	return &tenant.BucketClientService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) DashboardService(tb testing.TB) influxdb.DashboardService {
	tb.Helper()
	return &dashboardTransport.DashboardService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) LabelService(tb testing.TB) influxdb.LabelService {
	tb.Helper()
	return &label.LabelClientService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) NotificationEndpointService(tb testing.TB) *http.NotificationEndpointService {
	tb.Helper()
	return http.NewNotificationEndpointService(tl.HTTPClient(tb))
}

func (tl *TestLauncher) NotificationRuleService(tb testing.TB) influxdb.NotificationRuleStore {
	tb.Helper()
	return http.NewNotificationRuleService(tl.HTTPClient(tb))
}

func (tl *TestLauncher) OrgService(tb testing.TB) influxdb.OrganizationService {
	tb.Helper()
	return &tenant.OrgClientService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) PkgerService(tb testing.TB) pkger.SVC {
	return &pkger.HTTPRemoteService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) TaskServiceKV(tb testing.TB) taskmodel.TaskService {
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
	tb.Helper()
	return &http.AuthorizationService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) TaskService(tb testing.TB) taskmodel.TaskService {
	tb.Helper()
	return &http.TaskService{Client: tl.HTTPClient(tb)}
}

func (tl *TestLauncher) BackupService(tb testing.TB) *clibackup.Client {
	tb.Helper()
	client := tl.APIClient(tb)
	return &clibackup.Client{
		CLI:       clients.CLI{},
		BackupApi: client.BackupApi,
		HealthApi: client.HealthApi,
	}
}

func (tl *TestLauncher) RestoreService(tb testing.TB) *clirestore.Client {
	tb.Helper()
	client := tl.APIClient(tb)
	return &clirestore.Client{
		CLI:              clients.CLI{},
		HealthApi:        client.HealthApi,
		RestoreApi:       client.RestoreApi,
		BucketsApi:       client.BucketsApi,
		OrganizationsApi: client.OrganizationsApi,
		ApiConfig:        client,
	}
}

func (tl *TestLauncher) ResetHTTPCLient() {
	tl.httpClient = nil
}

func (tl *TestLauncher) HTTPClient(tb testing.TB) *httpc.Client {
	tb.Helper()

	if tl.httpClient == nil {
		token := ""
		if tl.Auth != nil {
			token = tl.Auth.Token
		}
		client, err := http.NewHTTPClient(tl.URL().String(), token, false)
		if err != nil {
			tb.Fatal(err)
		}
		tl.httpClient = client
	}
	return tl.httpClient
}

func (tl *TestLauncher) APIClient(tb testing.TB) *api.APIClient {
	tb.Helper()

	if tl.apiClient == nil {
		params := api.ConfigParams{
			Host: tl.URL(),
		}
		if tl.Auth != nil {
			params.Token = &tl.Auth.Token
		}
		tl.apiClient = api.NewAPIClient(api.NewAPIConfig(params))
	}

	return tl.apiClient
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

func (tl *TestLauncher) NumReads(tb testing.TB, op string) uint64 {
	const metricName = "query_influxdb_source_read_request_duration_seconds"
	mf := tl.Metrics(tb)[metricName]
	if mf != nil {
		fmt.Printf("%v\n", mf)
		for _, m := range mf.Metric {
			for _, label := range m.Label {
				if label.GetName() == "op" && label.GetValue() == op {
					return m.Histogram.GetSampleCount()
				}
			}
		}
	}
	return 0
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
