package testing_test

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	nethttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/stdlib"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/query"

	_ "github.com/influxdata/flux/stdlib"           // Import the built-in functions
	_ "github.com/influxdata/influxdb/query/stdlib" // Import the stdlib
)

// Default context.
var ctx = context.Background()

func init() {
	flux.FinalizeBuiltIns()
}

var skipTests = map[string]string{
	// TODO(adam) determine the reason for these test failures.
	"cov":                      "Reason TBD",
	"covariance":               "Reason TBD",
	"cumulative_sum":           "Reason TBD",
	"cumulative_sum_default":   "Reason TBD",
	"cumulative_sum_noop":      "Reason TBD",
	"difference_panic":         "Reason TBD",
	"drop_non_existent":        "Reason TBD",
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
	"keep_non_existent":        "Reason TBD",
	"key_values":               "Reason TBD",
	"key_values_host_name":     "Reason TBD",
	"last":                     "Reason TBD",
	"lowestAverage":            "Reason TBD",
	"max":                      "Reason TBD",
	"meta_query_fields":        "Reason TBD",
	"meta_query_keys":          "Reason TBD",
	"meta_query_measurements":  "Reason TBD",
	"min":                      "Reason TBD",
	"multiple_range":           "Reason TBD",
	"sample":                   "Reason TBD",
	"selector_preserve_time":   "Reason TBD",
	"shift":                    "Reason TBD",
	"shift_negative_duration":  "Reason TBD",
	"show_all_tag_keys":        "Reason TBD",
	"sort":                     "Reason TBD",
	"task_per_line":            "Reason TBD",
	"top":                      "Reason TBD",
	"union":                    "Reason TBD",
	"union_heterogeneous":      "Reason TBD",
	"unique":                   "Reason TBD",
	"distinct":                 "Reason TBD",

	// it appears these occur when writing the input data.  `to` may not be null safe.
	"fill_bool":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"fill_float":  "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"fill_int":    "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"fill_string": "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"fill_time":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"fill_uint":   "failed to read meta data: panic: interface conversion: interface {} is nil, not uint64",
	"window_null": "failed to read meta data: panic: interface conversion: interface {} is nil, not float64",

	// these may just be missing calls to range() in the tests.  easy to fix in a new PR.
	"group_nulls":      "unbounded test",
	"integral":         "unbounded test",
	"integral_columns": "unbounded test",
	"map":              "unbounded test",

	// the following tests have a difference between the CSV-decoded input table, and the storage-retrieved version of that table
	"columns":              "group key mismatch",
	"count":                "column order mismatch",
	"mean":                 "column order mismatch",
	"percentile_aggregate": "column order mismatch",
	"percentile_tdigest":   "column order mismatch",
	"set":                  "column order mismatch",
	"set_new_column":       "column order mismatch",
	"skew":                 "column order mismatch",
	"spread":               "column order mismatch",
	"stddev":               "column order mismatch",
	"sum":                  "column order mismatch",
	"simple_max":           "_stop missing from expected output",
	"derivative":           "time bounds mismatch (engine uses now() instead of bounds on input table)",
	"percentile":           "time bounds mismatch (engine uses now() instead of bounds on input table)",
	"difference_columns":   "data write/read path loses columns x and y",
	"keys":                 "group key mismatch",
	"pivot_task_test":      "possible group key or column order mismatch",

	// failed to read meta data errors: the CSV encoding is incomplete probably due to data schema errors.  needs more detailed investigation to find root cause of error
	"filter_by_regex":             "failed to read metadata",
	"filter_by_tags":              "failed to read metadata",
	"group":                       "failed to read metadata",
	"group_ungroup":               "failed to read metadata",
	"pivot_mean":                  "failed to read metadata",
	"select_measurement":          "failed to read metadata",
	"select_measurement_field":    "failed to read metadata",
	"histogram_quantile_minvalue": "failed to read meta data: no column with label _measurement exists",
	"increase":                    "failed to read meta data: table has no _value column",

	"string_max":                  "error: invalid use of function: *functions.MaxSelector has no implementation for type string (https://github.com/influxdata/platform/issues/224)",
	"null_as_value":               "null not supported as value in influxql (https://github.com/influxdata/platform/issues/353)",
	"string_interp":               "string interpolation not working as expected in flux (https://github.com/influxdata/platform/issues/404)",
	"to":                          "to functions are not supported in the testing framework (https://github.com/influxdata/flux/issues/77)",
	"covariance_missing_column_1": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"covariance_missing_column_2": "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"drop_before_rename":          "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"drop_referenced":             "need to support known errors in new test framework (https://github.com/influxdata/flux/issues/536)",
	"yield":                       "yield requires special test case (https://github.com/influxdata/flux/issues/535)",
}

func TestFluxEndToEnd(t *testing.T) {
	runEndToEnd(t, stdlib.FluxTestPackages)
}
func BenchmarkFluxEndToEnd(b *testing.B) {
	benchEndToEnd(b, stdlib.FluxTestPackages)
}

func runEndToEnd(t *testing.T, pkgs []*ast.Package) {
	l := RunMainOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)
	for _, pkg := range pkgs {
		pkg := pkg.Copy().(*ast.Package)
		name := pkg.Files[0].Name
		t.Run(name, func(t *testing.T) {
			if reason, ok := skipTests[strings.TrimSuffix(name, ".flux")]; ok {
				t.Skip(reason)
			}
			testFlux(t, l, pkg)
		})
	}
}

func benchEndToEnd(b *testing.B, pkgs []*ast.Package) {
	l := RunMainOrFail(b, ctx)
	l.SetupOrFail(b)
	defer l.ShutdownOrFail(b, ctx)
	for _, pkg := range pkgs {
		pkg := pkg.Copy().(*ast.Package)
		name := pkg.Files[0].Name
		b.Run(name, func(b *testing.B) {
			if reason, ok := skipTests[strings.TrimSuffix(name, ".flux")]; ok {
				b.Skip(reason)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				testFlux(b, l, pkg)
			}
		})
	}
}

var optionsSource = `
import "testing"
import c "csv"

// Options bucket and org are defined dynamically per test

option testing.loadStorage = (csv) => {
	c.from(csv: csv) |> to(bucket: bucket, org: org)
	return from(bucket: bucket)
}
`
var optionsAST *ast.File

func init() {
	pkg := parser.ParseSource(optionsSource)
	if ast.Check(pkg) > 0 {
		panic(ast.GetError(pkg))
	}
	optionsAST = pkg.Files[0]
}

func testFlux(t testing.TB, l *Launcher, pkg *ast.Package) {

	// Query server to ensure write persists.

	b := &platform.Bucket{
		Organization:    "ORG",
		Name:            t.Name(),
		RetentionPeriod: 0,
	}

	s := l.BucketService()
	if err := s.CreateBucket(context.Background(), b); err != nil {
		t.Fatal(err)
	}

	// Define bucket and org options
	bucketOpt := &ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: "bucket"},
			Init: &ast.StringLiteral{Value: b.Name},
		},
	}
	orgOpt := &ast.OptionStatement{
		Assignment: &ast.VariableAssignment{
			ID:   &ast.Identifier{Name: "org"},
			Init: &ast.StringLiteral{Value: b.Organization},
		},
	}
	options := optionsAST.Copy().(*ast.File)
	options.Body = append([]ast.Statement{bucketOpt, orgOpt}, options.Body...)

	// Add options to pkg
	pkg.Files = append(pkg.Files, options)

	// Add testing.inspect call to ensure the data is loaded
	inspectCalls := stdlib.TestingInspectCalls(pkg)
	pkg.Files = append(pkg.Files, inspectCalls)

	req := &query.Request{
		OrganizationID: l.Org.ID,
		Compiler:       lang.ASTCompiler{AST: pkg},
	}
	if r, err := l.FluxService().Query(ctx, req); err != nil {
		t.Fatal(err)
	} else {
		for r.More() {
			v := r.Next()
			if err := v.Tables().Do(func(tbl flux.Table) error {
				return nil
			}); err != nil {
				t.Error(err)
			}
		}
	}

	// quirk: our execution engine doesn't guarantee the order of execution for disconnected DAGS
	// so that our function-with-side effects call to `to` may run _after_ the test instead of before.
	// running twice makes sure that `to` happens at least once before we run the test.
	// this time we use a call to `run` so that the assertion error is triggered
	runCalls := stdlib.TestingRunCalls(pkg)
	pkg.Files[len(pkg.Files)-1] = runCalls
	r, err := l.FluxService().Query(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	for r.More() {
		v := r.Next()
		if err := v.Tables().Do(func(tbl flux.Table) error {
			return nil
		}); err != nil {
			t.Error(err)
		}
	}
	if err := r.Err(); err != nil {
		t.Error(err)
		// Replace the testing.run calls with testing.inspect calls.
		pkg.Files[len(pkg.Files)-1] = inspectCalls
		r, err := l.FluxService().Query(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		defer func() {
			if t.Failed() {
				scanner := bufio.NewScanner(&out)
				for scanner.Scan() {
					t.Log(scanner.Text())
				}
			}
		}()
		for r.More() {
			v := r.Next()
			err := execute.FormatResult(&out, v)
			if err != nil {
				t.Error(err)
			}
		}
		if err := r.Err(); err != nil {
			t.Error(err)
		}
	}
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
	args = append(args, "--protos-path", filepath.Join(l.Path, "protos"))
	args = append(args, "--engine-path", filepath.Join(l.Path, "engine"))
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
