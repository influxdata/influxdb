package check

import (
	"context"
	"errors"
	"fmt"
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

func TestAddNamedHealthCheck(t *testing.T) {
	h := NewCheck()
	require.NoError(t, h.AddNamedHealthCheck(Named("awesome", ErrCheck(func() error {
		return nil
	}))))
	r := h.CheckHealth(context.Background())
	require.Equal(t, StatusPass, r.Status())
	require.Len(t, r.Checks(), 1)
	require.Equal(t, StatusPass, r.Checks()[0].Status())
}

func TestAddUnHealthyCheck(t *testing.T) {
	h := NewCheck()
	require.NoError(t, h.AddNamedHealthCheck(Named("failure", ErrCheck(func() error {
		return errors.New("Oops! I am sorry")
	}))))
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

func (m mockCheck) CheckName() string { return m.name }

func (m mockCheck) Check(_ context.Context) Response {
	return NewBasicResponse(m.name, m.status, "", nil)
}

func mockPass(name string) NamedChecker {
	return mockCheck{status: StatusPass, name: name}
}

func mockFail(name string) NamedChecker {
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

	require.NoError(t, c.AddNamedHealthCheck(mockPass("a")))
	require.NoError(t, c.AddNamedHealthCheck(mockPass("c")))
	require.NoError(t, c.AddNamedHealthCheck(mockPass("b")))
	require.NoError(t, c.AddNamedHealthCheck(mockFail("k")))
	require.NoError(t, c.AddNamedHealthCheck(mockFail("b2")))

	actual := c.CheckHealth(context.Background())

	expected := NewBasicResponse(NameHealth, StatusFail, "", Responses{
		NamedFail("b2", ""),
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

	require.NoError(t, c.AddNamedHealthCheck(mockPass(nameA)))
	require.NoError(t, c.AddNamedHealthCheck(mockPass(nameC)))
	require.NoError(t, c.AddNamedReadyCheck(Named(nameB, mockPass(nameB))))
	require.NoError(t, c.AddNamedReadyCheck(Named(nameK, mockFail(nameK))))
	require.NoError(t, c.AddNamedHealthCheck(mockFail(nameB)))

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

	require.NoError(t, c.AddNamedReadyCheck(Named(nameA, mockPass(nameA))))
	require.NoError(t, c.AddNamedReadyCheck(Named(nameB, mockPass(nameB))))
	require.NoError(t, c.AddNamedReadyCheck(Named(nameC, mockPass(nameC))))

	want := []string{nameA, nameB, nameC}
	names = c.ReadyCheckNames()
	require.Equal(t, want, names)

	names[0] = "MUTATED"
	require.Equal(t, want, c.ReadyCheckNames())
}

func TestNamed_StampsRegistrationName(t *testing.T) {
	// mockPass("") stamps an empty name. Named("alpha", ...) must override
	// it so the registered response carries "alpha".
	c := NewCheck()
	require.NoError(t, c.AddNamedHealthCheck(Named("alpha", mockPass(""))))

	resp := c.CheckHealth(context.Background())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, "alpha", resp.Checks()[0].Name())
}

// namedErrCheck is a check named name whose failure message is msg, so a test
// can tell which of two same-named registrations is being served.
func namedErrCheck(name, msg string) NamedChecker {
	return Named(name, ErrCheck(func() error { return errors.New(msg) }))
}

func TestAddNamedHealthCheck_DuplicateRejected(t *testing.T) {
	c := NewCheck()
	require.NoError(t, c.AddNamedHealthCheck(namedErrCheck("kv", "first")))

	err := c.AddNamedHealthCheck(namedErrCheck("kv", "second"))
	require.ErrorIs(t, err, ErrDuplicateCheckName)
	require.ErrorContains(t, err, `health check "kv"`)

	// The first registration survives, not merely one of them.
	resp := c.CheckHealth(context.Background())
	require.Len(t, resp.Checks(), 1)
	require.Equal(t, "kv", resp.Checks()[0].Name())
	require.Equal(t, "first", resp.Checks()[0].Message())
}

func TestAddNamedReadyCheck_DuplicateRejected(t *testing.T) {
	c := NewCheck()
	require.NoError(t, c.AddNamedReadyCheck(namedErrCheck("engine", "first")))
	require.NoError(t, c.AddNamedReadyCheck(namedErrCheck("kv", "other")))
	before := c.ReadyCheckNames()

	err := c.AddNamedReadyCheck(namedErrCheck("engine", "second"))
	require.ErrorIs(t, err, ErrDuplicateCheckName)
	require.ErrorContains(t, err, `ready check "engine"`)
	require.Equal(t, before, c.ReadyCheckNames())

	resp := c.CheckReady(context.Background())
	require.Len(t, resp.Checks(), 2)
	byName := make(map[string]Response)
	for _, sub := range resp.Checks() {
		byName[sub.Name()] = sub
	}
	require.Equal(t, "first", byName["engine"].Message())
}

func TestAddNamedCheck_SameNameAcrossSets(t *testing.T) {
	// The launcher registers a failed subsystem's name in both sets; the
	// sets are independent namespaces.
	c := NewCheck()
	require.NoError(t, c.AddNamedHealthCheck(mockPass("api")))
	require.NoError(t, c.AddNamedReadyCheck(mockPass("api")))
	require.Len(t, c.CheckHealth(context.Background()).Checks(), 1)
	require.Equal(t, []string{"api"}, c.ReadyCheckNames())
}

func TestAddNamedCheck_EmptyNameRejected(t *testing.T) {
	c := NewCheck()
	require.ErrorIs(t, c.AddNamedHealthCheck(mockPass("")), ErrEmptyCheckName)
	require.ErrorIs(t, c.AddNamedReadyCheck(mockPass("")), ErrEmptyCheckName)
	require.Empty(t, c.CheckHealth(context.Background()).Checks())
	require.Empty(t, c.CheckReady(context.Background()).Checks())
	require.Empty(t, c.ReadyCheckNames())
}

func ExampleNewCheck() {
	h := NewCheck()
	h.CheckHealth(context.Background())
}

func ExampleCheck_CheckHealth() {
	h := NewCheck()
	err := h.AddNamedHealthCheck(Named("google", CheckerFunc(func(ctx context.Context) Response {
		var r net.Resolver
		_, err := r.LookupHost(ctx, "google.com")
		if err != nil {
			return Error(err)
		}
		return Pass()
	})))
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.CheckHealth(ctx)
}
