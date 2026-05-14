package check

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	h.AddNamedHealthCheck(Named("awesome", ErrCheck(func() error {
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
	h.AddNamedHealthCheck(Named("failure", ErrCheck(func() error {
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

func TestCheckReadyEmpty(t *testing.T) {
	c := NewCheck()
	actual := c.CheckReady(context.Background())
	expected := Response{
		Name:   "Ready",
		Status: StatusPass,
		Checks: Responses{},
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("unexpected response. expected %v, actual %v", expected, actual)
	}
}

func TestHealthSorting(t *testing.T) {
	c := NewCheck()

	c.AddHealthCheck(mockPass("a"))
	c.AddHealthCheck(mockPass("c"))
	c.AddHealthCheck(mockPass("b"))
	c.AddHealthCheck(mockFail("k"))
	c.AddHealthCheck(mockFail("b"))

	actual := c.CheckHealth(context.Background())

	expected := Response{
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
	c := NewCheck()

	c.AddHealthCheck(mockPass("a"))
	c.AddHealthCheck(mockPass("c"))
	c.AddReadyCheck(mockPass("b"))
	c.AddReadyCheck(mockFail("k"))
	c.AddHealthCheck(mockFail("b"))

	actualHealth := c.CheckHealth(context.Background())

	expectedHealth := Response{
		Name:   "Health",
		Status: "fail",
		Checks: Responses{
			Response{Name: "b", Status: "fail"},
			Response{Name: "a", Status: "pass"},
			Response{Name: "c", Status: "pass"},
		},
	}

	if !reflect.DeepEqual(expectedHealth, actualHealth) {
		t.Errorf("unexpected response. expected %v, actual %v", expectedHealth, actualHealth)
	}

	actualReady := c.CheckReady(context.Background())

	expectedReady := Response{
		Name:   "Ready",
		Status: "fail",
		Checks: Responses{
			Response{Name: "k", Status: "fail"},
			Response{Name: "b", Status: "pass"},
		},
	}

	if !reflect.DeepEqual(expectedReady, actualReady) {
		t.Errorf("unexpected response. expected %v, actual %v", expectedReady, actualReady)
	}
}

func TestReadyCheckNames(t *testing.T) {
	c := NewCheck()

	// Empty Check returns an empty (non-nil) slice.
	names := c.ReadyCheckNames()
	require.NotNil(t, names)
	require.Empty(t, names)

	c.AddNamedReadyCheck(Named("a", mockPass("a")))
	c.AddReadyCheck(mockPass("")) // anonymous: not a NamedChecker
	c.AddNamedReadyCheck(Named("c", mockPass("c")))

	names = c.ReadyCheckNames()
	require.Equal(t, []string{"a", "", "c"}, names)

	// The returned slice must be a copy: mutating it does not affect
	// subsequent calls.
	names[0] = "MUTATED"
	require.Equal(t, []string{"a", "", "c"}, c.ReadyCheckNames())
}

func TestAddHealthCheck_NamedCheckerDispatch(t *testing.T) {
	// mockPass("") returns Response.Name == "". If AddHealthCheck honors
	// the NamedChecker interface, Named() stamps "alpha" onto the
	// response. If it doesn't, the response name would stay empty.
	c := NewCheck()
	c.AddHealthCheck(Named("alpha", mockPass("")))

	resp := c.CheckHealth(context.Background())
	require.Len(t, resp.Checks, 1)
	require.Equal(t, "alpha", resp.Checks[0].Name)
}

func TestAddReadyCheck_NamedCheckerDispatch(t *testing.T) {
	// ReadyCheckNames is the clearest signal: the recorded name is the
	// side effect of dispatching to AddNamedReadyCheck.
	c := NewCheck()
	c.AddReadyCheck(Named("beta", mockPass("")))

	require.Equal(t, []string{"beta"}, c.ReadyCheckNames())

	resp := c.CheckReady(context.Background())
	require.Len(t, resp.Checks, 1)
	require.Equal(t, "beta", resp.Checks[0].Name)
}

func ExampleNewCheck() {
	// Run the default healthcheck. it always return 200. It is good if you
	// have a service without any dependency
	h := NewCheck()
	h.CheckHealth(context.Background())
}

func ExampleCheck_CheckHealth() {
	h := NewCheck()
	h.AddNamedHealthCheck(Named("google", CheckerFunc(func(ctx context.Context) Response {
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
