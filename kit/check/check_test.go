package check

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEmptyCheck(t *testing.T) {
	c := NewCheck()
	resp := c.CheckReady(context.Background())
	require.Empty(t, resp.Checks(), "no checks added")
	require.Equal(t, NameReady, resp.Name())
	require.Equal(t, StatusPass, resp.Status())
}

func TestAddHealthCheck(t *testing.T) {
	h := NewCheck()
	h.AddNamedHealthCheck(Named("awesome", ErrCheck(func() error {
		return nil
	})))
	r := h.CheckHealth(context.Background())
	require.Equal(t, StatusPass, r.Status())
	require.Len(t, r.Checks(), 1)
	require.Equal(t, StatusPass, r.Checks()[0].Status())
}

func TestAddUnHealthyCheck(t *testing.T) {
	h := NewCheck()
	h.AddNamedHealthCheck(Named("failure", ErrCheck(func() error {
		return errors.New("Oops! I am sorry")
	})))
	r := h.CheckHealth(context.Background())
	require.Equal(t, StatusFail, r.Status())
	require.Len(t, r.Checks(), 1)
	require.Equal(t, StatusFail, r.Checks()[0].Status())
	require.Equal(t, "Oops! I am sorry", r.Checks()[0].Message())
}

type mockCheck struct {
	status Status
	name   string
}

func (m mockCheck) Check(_ context.Context) Response {
	return NewBasicResponse(m.name, m.status, "", nil)
}

func mockPass(name string) Checker {
	return mockCheck{status: StatusPass, name: name}
}

func mockFail(name string) Checker {
	return mockCheck{status: StatusFail, name: name}
}

// assertResponseEqual compares the wire-visible fields of two Response
// values. Used in place of reflect.DeepEqual because Response is an
// interface; concrete impls may differ even when their derived fields
// match.
func assertResponseEqual(t *testing.T, want, got Response) {
	t.Helper()
	require.Equal(t, want.Name(), got.Name(), "name")
	require.Equal(t, want.Status(), got.Status(), "status")
	require.Equal(t, want.Message(), got.Message(), "message")
	require.Equal(t, len(want.Checks()), len(got.Checks()), "checks length")
	for i := range want.Checks() {
		require.Equal(t, want.Checks()[i].Name(), got.Checks()[i].Name(), "checks[%d].name", i)
		require.Equal(t, want.Checks()[i].Status(), got.Checks()[i].Status(), "checks[%d].status", i)
		require.Equal(t, want.Checks()[i].Message(), got.Checks()[i].Message(), "checks[%d].message", i)
	}
}

func TestCheckReadyEmpty(t *testing.T) {
	c := NewCheck()
	actual := c.CheckReady(context.Background())
	expected := NewBasicResponse(NameReady, StatusPass, "", Responses{})
	assertResponseEqual(t, expected, actual)
}

func TestHealthSorting(t *testing.T) {
	c := NewCheck()

	c.AddHealthCheck(mockPass("a"))
	c.AddHealthCheck(mockPass("c"))
	c.AddHealthCheck(mockPass("b"))
	c.AddHealthCheck(mockFail("k"))
	c.AddHealthCheck(mockFail("b"))

	actual := c.CheckHealth(context.Background())

	expected := NewBasicResponse(NameHealth, StatusFail, "", Responses{
		NamedFail("b", ""),
		NamedFail("k", ""),
		NamedPass("a"),
		NamedPass("b"),
		NamedPass("c"),
	})
	assertResponseEqual(t, expected, actual)
}

func TestNoCrossOver(t *testing.T) {
	const (
		nameA = "a"
		nameB = "b"
		nameC = "c"
		nameK = "k"
	)
	c := NewCheck()

	c.AddHealthCheck(mockPass(nameA))
	c.AddHealthCheck(mockPass(nameC))
	c.AddNamedReadyCheck(Named(nameB, mockPass(nameB)))
	c.AddNamedReadyCheck(Named(nameK, mockFail(nameK)))
	c.AddHealthCheck(mockFail(nameB))

	actualHealth := c.CheckHealth(context.Background())
	expectedHealth := NewBasicResponse(NameHealth, StatusFail, "", Responses{
		NamedFail(nameB, ""),
		NamedPass(nameA),
		NamedPass(nameC),
	})
	assertResponseEqual(t, expectedHealth, actualHealth)

	actualReady := c.CheckReady(context.Background())
	expectedReady := NewBasicResponse(NameReady, StatusFail, "", Responses{
		NamedFail(nameK, ""),
		NamedPass(nameB),
	})
	assertResponseEqual(t, expectedReady, actualReady)
}

func TestReadyCheckNames(t *testing.T) {
	const (
		nameA = "a"
		nameB = "b"
		nameC = "c"
	)
	c := NewCheck()

	names := c.ReadyCheckNames()
	require.NotNil(t, names)
	require.Empty(t, names)

	c.AddNamedReadyCheck(Named(nameA, mockPass(nameA)))
	c.AddNamedReadyCheck(Named(nameB, mockPass(nameB)))
	c.AddNamedReadyCheck(Named(nameC, mockPass(nameC)))

	want := []string{nameA, nameB, nameC}
	names = c.ReadyCheckNames()
	require.Equal(t, want, names)

	names[0] = "MUTATED"
	require.Equal(t, want, c.ReadyCheckNames())
}

func TestAddHealthCheck_NamedCheckerDispatch(t *testing.T) {
	// mockPass("") returns an empty name. Named("alpha", ...) must override
	// it so the registered response carries "alpha".
	c := NewCheck()
	c.AddHealthCheck(Named("alpha", mockPass("")))

	resp := c.CheckHealth(context.Background())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, "alpha", resp.Checks()[0].Name())
}

func ExampleNewCheck() {
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
