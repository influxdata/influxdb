package tests_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/lang"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/query"

	_ "github.com/influxdata/flux/functions" // Import the built-in functions
	_ "github.com/influxdata/flux/functions/inputs"
	_ "github.com/influxdata/flux/functions/outputs"
	_ "github.com/influxdata/flux/functions/tests"
	_ "github.com/influxdata/flux/functions/transformations"
	_ "github.com/influxdata/flux/options"             // Import the built-in options
	_ "github.com/influxdata/influxdb/query/functions" // Import the built-in functions
	_ "github.com/influxdata/influxdb/query/functions/inputs"
	_ "github.com/influxdata/influxdb/query/functions/outputs"
	_ "github.com/influxdata/influxdb/query/options" // Import the built-in options
)

// Default context.
var ctx = context.Background()

type AssertionError interface {
	Assertion() bool
}

func init() {
	flux.RegisterBuiltIn("loadTest", loadTestBuiltin)
	flux.FinalizeBuiltIns()
}

var loadTestBuiltin = `
// loadData is a function that's referenced in all the transformation tests.  
// it's registered here so that we can register a different loadData function for 
// each platform/binary.  
testLoadStorageHelper = (csv, bucket, org) => {fromCSV(csv: csv) |> to(bucket: bucket, org: org) return from(bucket: bucket) }
testLoadMem = (csv) => fromCSV(csv: csv)
`

var skipTests = map[string]string{
	// TODO(adam) determine the reason for these test failures.
	"cov":                      "Reason TBD",
	"covariance":               "Reason TBD",
	"cumulative_sum":           "Reason TBD",
	"cumulative_sum_default":   "Reason TBD",
	"cumulative_sum_noop":      "Reason TBD",
	"difference_panic":         "Reason TBD",
	"filter_by_regex_function": "Reason TBD",
	"first":                    "Reason TBD",
	"group_by_irregular":       "Reason TBD",
	"highestAverage":           "Reason TBD",
	"highestMax":               "Reason TBD",
	"histogram":                "Reason TBD",
	"histogram_normalize":      "Reason TBD",
	"histogram_quantile":       "Reason TBD",
	"join":                     "Reason TBD",
	"join_across_measurements": "Reason TBD",
	"key_values":               "Reason TBD",
	"key_values_host_name":     "Reason TBD",
	"last":                     "Reason TBD",
	"lowestAverage":            "Reason TBD",
	"max":                      "Reason TBD",
	"meta_query_fields":        "Reason TBD",
	"meta_query_keys":          "Reason TBD",
	"meta_query_measurements":  "Reason TBD",
	"min":                     "Reason TBD",
	"multiple_range":          "Reason TBD",
	"sample":                  "Reason TBD",
	"selector_preserve_time":  "Reason TBD",
	"shift":                   "Reason TBD",
	"shift_negative_duration": "Reason TBD",
	"show_all_tag_keys":       "Reason TBD",
	"sort":                    "Reason TBD",
	"task_per_line":           "Reason TBD",
	"top":                     "Reason TBD",
	"union":                   "Reason TBD",
	"union_heterogeneous":     "Reason TBD",
	"unique":                  "Reason TBD",

	"string_max":    "error: invalid use of function: *functions.MaxSelector has no implementation for type string (https://github.com/influxdata/platform/issues/224)",
	"null_as_value": "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
	"string_interp": "string interpolation not working as expected in flux (https://github.com/influxdata/platform/issues/404)",
	"to":            "to functions are not supported in the testing framework (https://github.com/influxdata/flux/issues/77)",
	"covariance_missing_column_1": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"covariance_missing_column_2": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"drop_before_rename":          "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"drop_referenced":             "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"yield":                       "yield requires special test case (https://github.com/influxdata/flux/issues/535)",
}

func withEachFluxFile(t testing.TB, fn func(filename string)) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "testdata")
	os.Chdir(path)

	fluxFiles, err := filepath.Glob("*.flux")
	if err != nil {
		t.Fatalf("error searching for Flux files: %s", err)
	}

	for _, fluxFile := range fluxFiles {
		fn(fluxFile)
	}
}

func Test_QueryEndToEnd(t *testing.T) {
	t.Skip("skipping platform end to end tests due to execution sync issues (https://github.com/influxdata/flux/issues/613)")
	l := RunMainOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	withEachFluxFile(t, func(filename string) {
		testName := filename[:len(filename)-len(".flux")]
		reason, skip := skipTests[testName]
		t.Run(filename, func(t *testing.T) {
			if skip {
				t.Skip(reason)
			}
			if qs, err := ioutil.ReadFile(filename); err != nil {
				return
			} else {
				// Query server to ensure write persists.

				b := &platform.Bucket{
					Organization:    "ORG",
					Name:            testName,
					RetentionPeriod: 0,
				}

				s := l.BucketService()
				if err := s.CreateBucket(context.Background(), b); err != nil {
					t.Fatal(err)
				}

				testLoadStorageQuery := fmt.Sprintf("testLoadStorage = (csv) => testLoadStorageHelper(csv:csv, bucket: \"%s\", org: \"%s\") \n", b.Name, b.Organization)

				qy := testLoadStorageQuery + string(qs)
				t.Log(qy)

				req := &query.Request{
					OrganizationID: l.Org.ID,
					Compiler:       lang.FluxCompiler{Query: qy},
				}
				if _, err := l.FluxService().Query(ctx, req); err != nil {
					t.Fatal(err)
				}

				// quirk: our execution engine doesn't guarantee the order of execution for disconnected DAGS
				// so that our function-with-side effects call to `to` may run _after_ the test instead of before.
				// running twice makes sure that `to` happens at least once before we run the test.
				r, err := l.FluxService().Query(ctx, req)
				if err != nil {
					t.Fatal(err)
				}

				for r.More() {
					v := r.Next()
					err := v.Tables().Do(func(tbl flux.Table) error {
						return nil
					})
					if err != nil {
						if assertionErr, ok := err.(AssertionError); ok {
							t.Error(assertionErr)
						} else {
							t.Fatal(err)
						}
					}
				}
				if err := r.Err(); err != nil {
					if assertionErr, ok := err.(AssertionError); ok {
						t.Error(assertionErr)
					} else {
						t.Fatal(err)
					}
				}
			}
		})
	})
}

// Launcher is a test wrapper for main.Launcher.
type Launcher struct {
	*launcher.Launcher

	// Root temporary directory for all data.
	Path string

	// Initialized after calling the Setup() helper.
	User   *platform.User
	Org    *platform.Organization
	Bucket *platform.Bucket
	Auth   *platform.Authorization

	// Standard in/out/err buffers.
	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewLauncher returns a new instance of Launcher.
func NewLauncher() *Launcher {
	l := &Launcher{Launcher: launcher.NewLauncher()}
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

// RunMainOrFail initializes and starts the server.
func RunMainOrFail(tb testing.TB, ctx context.Context, args ...string) *Launcher {
	tb.Helper()
	l := NewLauncher()
	if err := l.Run(ctx, args...); err != nil {
		tb.Fatal(err)
	}
	return l
}

// Run executes the program with additional arguments to set paths and ports.
func (l *Launcher) Run(ctx context.Context, args ...string) error {
	args = append(args, "--bolt-path", filepath.Join(l.Path, "influxd.bolt"))
	args = append(args, "--engine-path", filepath.Join(l.Path, "engine"))
	args = append(args, "--nats-path", filepath.Join(l.Path, "nats"))
	args = append(args, "--http-bind-address", "127.0.0.1:0")
	args = append(args, "--log-level", "debug")
	return l.Launcher.Run(ctx, args...)
}

// Shutdown stops the program and cleans up temporary paths.
func (l *Launcher) Shutdown(ctx context.Context) error {
	l.Cancel()
	l.Launcher.Shutdown(ctx)
	return os.RemoveAll(l.Path)
}

// ShutdownOrFail stops the program and cleans up temporary paths. Fail on error.
func (l *Launcher) ShutdownOrFail(tb testing.TB, ctx context.Context) {
	tb.Helper()
	if err := l.Shutdown(ctx); err != nil {
		tb.Fatal(err)
	}
}

// SetupOrFail creates a new user, bucket, org, and auth token. Fail on error.
func (l *Launcher) SetupOrFail(tb testing.TB) {
	svc := &http.SetupService{Addr: l.URL()}
	results, err := svc.Generate(ctx, &platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	})
	if err != nil {
		tb.Fatal(err)
	}

	l.User = results.User
	l.Org = results.Org
	l.Bucket = results.Bucket
	l.Auth = results.Auth
}

func (l *Launcher) FluxService() *http.FluxQueryService {
	return &http.FluxQueryService{Addr: l.URL(), Token: l.Auth.Token}
}

func (l *Launcher) BucketService() *http.BucketService {
	return &http.BucketService{
		Addr:     l.URL(),
		Token:    l.Auth.Token,
		OpPrefix: bolt.OpPrefix,
	}
}

// MustNewHTTPRequest returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (l *Launcher) MustNewHTTPRequest(method, rawurl, body string) *nethttp.Request {
	req, err := nethttp.NewRequest(method, l.URL()+rawurl, strings.NewReader(body))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", "Token "+l.Auth.Token)
	return req
}
