package launcher_test

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

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/http"
	_ "github.com/influxdata/influxdb/query/builtin"
)

// Default context.
var ctx = context.Background()

func TestLauncher_Setup(t *testing.T) {
	l := NewLauncher()
	if err := l.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer l.Shutdown(ctx)

	svc := &http.SetupService{Addr: l.URL()}
	if results, err := svc.Generate(ctx, &platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	}); err != nil {
		t.Fatal(err)
	} else if results.User.ID == 0 {
		t.Fatal("expected user id")
	} else if results.Org.ID == 0 {
		t.Fatal("expected org id")
	} else if results.Bucket.ID == 0 {
		t.Fatal("expected bucket id")
	} else if results.Auth.Token == "" {
		t.Fatal("expected auth token")
	}
}

func TestLauncher_WriteAndQuery(t *testing.T) {
	l := RunLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID), `m,k=v f=100i 946684800000000000`))
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_measurement,k,_field` + "\r\n" +
		`,result,table,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,m,v,f` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	if err != nil {
		t.Fatalf("unexpected error querying server: %v", err)
	}
	if diff := cmp.Diff(string(buf), exp); diff != "" {
		t.Fatal(diff)
	}
}

func TestLauncher_BucketDelete(t *testing.T) {
	l := RunLauncherOrFail(t, ctx)
	l.SetupOrFail(t)
	defer l.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	resp, err := nethttp.DefaultClient.Do(l.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", l.Org.ID, l.Bucket.ID), `m,k=v f=100i 946684800000000000`))
	if err != nil {
		t.Fatal(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_measurement,k,_field` + "\r\n" +
		`,result,table,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,m,v,f` + "\r\n\r\n"

	buf, err := http.SimpleQuery(l.URL(), qs, l.Org.Name, l.Auth.Token)
	if err != nil {
		t.Fatalf("unexpected error querying server: %v", err)
	}
	if diff := cmp.Diff(string(buf), exp); diff != "" {
		t.Fatal(diff)
	}

	// Verify the cardinality in the engine.
	engine := l.Launcher.Engine()
	if got, exp := engine.SeriesCardinality(), int64(1); got != exp {
		t.Fatalf("got %d, exp %d", got, exp)
	}

	// Delete the bucket.
	if resp, err = nethttp.DefaultClient.Do(l.MustNewHTTPRequest("DELETE", fmt.Sprintf("/api/v2/buckets/%s", l.Bucket.ID), "")); err != nil {
		t.Fatal(err)
	}

	if body, err = ioutil.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d, body: %s, headers: %v", resp.StatusCode, body, resp.Header)
	}

	// Verify that the data has been removed from the storage engine.
	if got, exp := engine.SeriesCardinality(), int64(0); got != exp {
		t.Fatalf("after bucket delete got %d, exp %d", got, exp)
	}
}

// Launcher is a test wrapper for launcher.Launcher.
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

// RunLauncherOrFail initializes and starts the server.
func RunLauncherOrFail(tb testing.TB, ctx context.Context, args ...string) *Launcher {
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
	results := l.OnBoardOrFail(tb, &platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	})

	l.User = results.User
	l.Org = results.Org
	l.Bucket = results.Bucket
	l.Auth = results.Auth
}

// OnBoardOrFail attempts an on-boarding request or fails on error.
// The on-boarding status is also reset to allow multiple user/org/buckets to be created.
func (l *Launcher) OnBoardOrFail(tb testing.TB, req *platform.OnboardingRequest) *platform.OnboardingResults {
	tb.Helper()
	res, err := l.KeyValueService().Generate(context.Background(), req)
	if err != nil {
		tb.Fatal(err)
	}

	err = l.KeyValueService().PutOnboardingStatus(context.Background(), false)
	if err != nil {
		tb.Fatal(err)
	}

	return res
}

func (l *Launcher) FluxService() *http.FluxService {
	return &http.FluxService{Addr: l.URL(), Token: l.Auth.Token}
}

func (l *Launcher) BucketService() *http.BucketService {
	return &http.BucketService{Addr: l.URL(), Token: l.Auth.Token}
}

func (l *Launcher) AuthorizationService() *http.AuthorizationService {
	return &http.AuthorizationService{Addr: l.URL(), Token: l.Auth.Token}
}

func (l *Launcher) TaskService() *http.TaskService {
	return &http.TaskService{Addr: l.URL(), Token: l.Auth.Token}
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

// MustNewHTTPRequest returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (l *Launcher) NewHTTPRequestOrFail(tb testing.TB, method, rawurl, token string, body string) *nethttp.Request {
	tb.Helper()
	req, err := nethttp.NewRequest(method, l.URL()+rawurl, strings.NewReader(body))
	if err != nil {
		tb.Fatal(err)
	}

	req.Header.Set("Authorization", "Token "+token)
	return req
}
