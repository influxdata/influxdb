package check

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestEmptyCheck(t *testing.T) {
	c := NewCheck()
	resp := c.CheckReady(context.Background())
	if len(resp.Checks) > 0 {
		t.Errorf("no checks added but %d returned", len(resp.Checks))
	}

	if resp.Name != "Ready" {
		t.Errorf("expected: \"Ready\", got: %q", resp.Name)
	}

	if resp.Status != StatusPass {
		t.Errorf("expected: %q, got: %q", StatusPass, resp.Status)
	}
}

func TestAddHealthCheck(t *testing.T) {
	h := NewCheck()
	h.AddHealthCheck(Named("awesome", ErrCheck(func() error {
		return nil
	})))
	r := h.CheckHealth(context.Background())
	if r.Status != StatusPass {
		t.Error("Health should fail because one of the check is unhealthy")
	}

	if len(r.Checks) != 1 {
		t.Fatalf("check not in results: %+v", r.Checks)
	}

	v := r.Checks[0]
	if v.Status != StatusPass {
		t.Errorf("the added check should be pass not %q.", v.Status)
	}
}

func TestAddUnHealthyCheck(t *testing.T) {
	h := NewCheck()
	h.AddHealthCheck(Named("failure", ErrCheck(func() error {
		return errors.New("Oops! I am sorry")
	})))
	r := h.CheckHealth(context.Background())
	if r.Status != StatusFail {
		t.Error("Health should fail because one of the check is unhealthy")
	}

	if len(r.Checks) != 1 {
		t.Fatal("check not in results")
	}

	v := r.Checks[0]
	if v.Status != StatusFail {
		t.Errorf("the added check should be fail not %s.", v.Status)
	}
	if v.Message != "Oops! I am sorry" {
		t.Errorf(
			"the error should be 'Oops! I am sorry' not  %s.",
			v.Message,
		)
	}
}

func buildCheckWithServer() (*Check, *httptest.Server) {
	c := NewCheck()
	return c, httptest.NewServer(c)
}

type mockCheck struct {
	status Status
	name   string
}

func (m mockCheck) Check(_ context.Context) Response {
	return Response{
		Name:   m.name,
		Status: m.status,
	}
}

func mockPass(name string) Checker {
	return mockCheck{status: StatusPass, name: name}
}

func mockFail(name string) Checker {
	return mockCheck{status: StatusFail, name: name}
}

func respBuilder(body io.ReadCloser) (*Response, error) {
	defer body.Close()
	d := json.NewDecoder(body)
	r := &Response{}
	return r, d.Decode(r)
}

func TestBasicHTTPHandler(t *testing.T) {
	_, ts := buildCheckWithServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}

	actual, err := respBuilder(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	expected := &Response{
		Name:   "Ready",
		Status: StatusPass,
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("unexpected response. expected %v, actual %v", expected, actual)
	}
}

func TestHealthSorting(t *testing.T) {
	c, ts := buildCheckWithServer()
	defer ts.Close()

	c.AddHealthCheck(mockPass("a"))
	c.AddHealthCheck(mockPass("c"))
	c.AddHealthCheck(mockPass("b"))
	c.AddHealthCheck(mockFail("k"))
	c.AddHealthCheck(mockFail("b"))

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := respBuilder(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Response{
		Name:   "Health",
		Status: "fail",
		Checks: Responses{
			Response{Name: "b", Status: "fail"},
			Response{Name: "k", Status: "fail"},
			Response{Name: "a", Status: "pass"},
			Response{Name: "b", Status: "pass"},
			Response{Name: "c", Status: "pass"},
		},
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("unexpected response. expected %v, actual %v", expected, actual)
	}
}

func TestNoCrossOver(t *testing.T) {
	c, ts := buildCheckWithServer()
	defer ts.Close()

	c.AddHealthCheck(mockPass("a"))
	c.AddHealthCheck(mockPass("c"))
	c.AddReadyCheck(mockPass("b"))
	c.AddReadyCheck(mockFail("k"))
	c.AddHealthCheck(mockFail("b"))

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := respBuilder(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	expected := &Response{
		Name:   "Health",
		Status: "fail",
		Checks: Responses{
			Response{Name: "b", Status: "fail"},
			Response{Name: "a", Status: "pass"},
			Response{Name: "c", Status: "pass"},
		},
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("unexpected response. expected %v, actual %v", expected, actual)
	}

	resp, err = http.Get(ts.URL + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	actual, err = respBuilder(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	expected = &Response{
		Name:   "Ready",
		Status: "fail",
		Checks: Responses{
			Response{Name: "k", Status: "fail"},
			Response{Name: "b", Status: "pass"},
		},
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("unexpected response. expected %v, actual %v", expected, actual)
	}
}

func TestPassthrough(t *testing.T) {
	c, ts := buildCheckWithServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 404 {
		t.Fatalf("failed to error when no passthrough is present, status: %d", resp.StatusCode)
	}

	used := false
	s := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		used = true
		w.Write([]byte("hi"))
	})

	c.SetPassthrough(s)

	resp, err = http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("bad response code from passthrough, status: %d", resp.StatusCode)
	}

	if !used {
		t.Fatal("passthrough server not used")
	}
}

func ExampleNewCheck() {
	// Run the default healthcheck. it always return 200. It is good if you
	// have a service without any dependency
	h := NewCheck()
	h.CheckHealth(context.Background())
}

func ExampleCheck_CheckHealth() {
	h := NewCheck()
	h.AddHealthCheck(Named("google", CheckerFunc(func(ctx context.Context) Response {
		var r net.Resolver
		_, err := r.LookupHost(ctx, "google.com")
		if err != nil {
			return Error(err)
		}
		return Pass()
	})))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.CheckHealth(ctx)
}

func ExampleCheck_ServeHTTP() {
	c := NewCheck()
	http.ListenAndServe(":6060", c)
}

func ExampleCheck_SetPassthrough() {
	c := NewCheck()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello friends!"))
	})

	c.SetPassthrough(http.DefaultServeMux)
	http.ListenAndServe(":6060", c)
}
