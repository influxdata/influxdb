package main_test

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
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influxd"
	"github.com/influxdata/platform/http"
)

// Default context.
var ctx = context.Background()

func TestMain_Setup(t *testing.T) {
	m := NewMain()
	if err := m.Run(ctx); err != nil {
		t.Fatal(err)
	}
	defer m.Shutdown(ctx)

	svc := &http.SetupService{Addr: m.URL()}
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

func TestMain_WriteAndQuery(t *testing.T) {
	m := RunMainOrFail(t, ctx)
	m.SetupOrFail(t)
	defer m.ShutdownOrFail(t, ctx)

	// Execute single write against the server.
	if resp, err := nethttp.DefaultClient.Do(m.MustNewHTTPRequest("POST", fmt.Sprintf("/api/v2/write?org=%s&bucket=%s", m.Org.ID, m.Bucket.ID), `m,k=v f=100i 946684800000000000`)); err != nil {
		t.Fatal(err)
	} else if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	} else if resp.StatusCode != nethttp.StatusNoContent {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	// Query server to ensure write persists.
	qs := `from(bucket:"BUCKET") |> range(start:2000-01-01T00:00:00Z,stop:2000-01-02T00:00:00Z)`
	exp := `,result,table,_start,_stop,_time,_value,_field,_measurement,k` + "\r\n" +
		`,result,table,2000-01-01T00:00:00Z,2000-01-02T00:00:00Z,2000-01-01T00:00:00Z,100,f,m,v` + "\r\n\r\n"

	var buf bytes.Buffer
	req := (http.QueryRequest{Query: qs, Org: m.Org}).WithDefaults()
	if preq, err := req.ProxyRequest(); err != nil {
		t.Fatal(err)
	} else if _, err := m.FluxService().Query(ctx, &buf, preq); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(buf.String(), exp); diff != "" {
		t.Fatal(diff)
	}
}

// Main is a test wrapper for main.Main.
type Main struct {
	*main.Main

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

// NewMain returns a new instance of Main.
func NewMain() *Main {
	m := &Main{Main: main.NewMain()}
	m.Main.Stdin = &m.Stdin
	m.Main.Stdout = &m.Stdout
	m.Main.Stderr = &m.Stderr
	if testing.Verbose() {
		m.Main.Stdout = io.MultiWriter(m.Main.Stdout, os.Stdout)
		m.Main.Stderr = io.MultiWriter(m.Main.Stderr, os.Stderr)
	}

	path, err := ioutil.TempDir("", "")
	if err != nil {
		panic(err)
	}
	m.Path = path
	return m
}

// RunMainOrFail initializes and starts the server.
func RunMainOrFail(tb testing.TB, ctx context.Context, args ...string) *Main {
	tb.Helper()
	m := NewMain()
	if err := m.Run(ctx, args...); err != nil {
		tb.Fatal(err)
	}
	return m
}

// Run executes the program with additional arguments to set paths and ports.
func (m *Main) Run(ctx context.Context, args ...string) error {
	args = append(args, "--bolt-path", filepath.Join(m.Path, "influxd.bolt"))
	args = append(args, "--engine-path", filepath.Join(m.Path, "engine"))
	args = append(args, "--nats-path", filepath.Join(m.Path, "nats"))
	args = append(args, "--http-bind-address", "127.0.0.1:0")
	args = append(args, "--log-level", "debug")
	return m.Main.Run(ctx, args...)
}

// Shutdown stops the program and cleans up temporary paths.
func (m *Main) Shutdown(ctx context.Context) error {
	m.Main.Shutdown(ctx)
	return os.RemoveAll(m.Path)
}

// ShutdownOrFail stops the program and cleans up temporary paths. Fail on error.
func (m *Main) ShutdownOrFail(tb testing.TB, ctx context.Context) {
	tb.Helper()
	if err := m.Shutdown(ctx); err != nil {
		tb.Fatal(err)
	}
}

// SetupOrFail creates a new user, bucket, org, and auth token. Fail on error.
func (m *Main) SetupOrFail(tb testing.TB) {
	svc := &http.SetupService{Addr: m.URL()}
	results, err := svc.Generate(ctx, &platform.OnboardingRequest{
		User:     "USER",
		Password: "PASSWORD",
		Org:      "ORG",
		Bucket:   "BUCKET",
	})
	if err != nil {
		tb.Fatal(err)
	}

	m.User = results.User
	m.Org = results.Org
	m.Bucket = results.Bucket
	m.Auth = results.Auth
}

func (m *Main) FluxService() *http.FluxService {
	return &http.FluxService{Addr: m.URL(), Token: m.Auth.Token}
}

// MustNewHTTPRequest returns a new nethttp.Request with base URL and auth attached. Fail on error.
func (m *Main) MustNewHTTPRequest(method, rawurl, body string) *nethttp.Request {
	req, err := nethttp.NewRequest(method, m.URL()+rawurl, strings.NewReader(body))
	if err != nil {
		panic(err)
	}

	req.Header.Set("Authorization", "Token "+m.Auth.Token)
	return req
}
