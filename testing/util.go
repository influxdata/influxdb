package testing

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func()) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	require.NoError(t, err, "unable to create temporary boltdb file")
	require.NoError(t, f.Close())

	path := f.Name()
	s := bolt.NewKVStore(zaptest.NewLogger(t), path, bolt.WithNoSync)
	require.NoError(t, s.Open(context.Background()))

	// apply all kv migrations
	require.NoError(t, all.Up(context.Background(), zaptest.NewLogger(t), s))

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close
}

func NewTestInmemStore(t *testing.T) kv.SchemaStore {
	s := inmem.NewKVStore()
	// apply all kv migrations
	require.NoError(t, all.Up(context.Background(), zaptest.NewLogger(t), s))
	return s
}

// TODO(goller): remove opPrefix argument
func diffPlatformErrors(name string, actual, expected error, opPrefix string, t *testing.T) {
	t.Helper()
	ErrorsEqual(t, actual, expected)
}

// ErrorsEqual checks to see if the provided errors are equivalent.
func ErrorsEqual(t *testing.T, actual, expected error) {
	t.Helper()
	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		t.Errorf("unexpected error %s", actual.Error())
	}

	if expected != nil && actual == nil {
		t.Errorf("expected error %s but received nil", expected.Error())
	}

	if errors.ErrorCode(expected) != errors.ErrorCode(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error code %q but received %q", errors.ErrorCode(expected), errors.ErrorCode(actual))
	}

	if errors.ErrorMessage(expected) != errors.ErrorMessage(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error message %q but received %q", errors.ErrorMessage(expected), errors.ErrorMessage(actual))
	}
}

func idPtr(id platform.ID) *platform.ID {
	return &id
}

func strPtr(s string) *string {
	return &s
}
func boolPtr(b bool) *bool {
	return &b
}

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) platform.ID {
	id, err := platform.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}

// MustIDBase16Ptr is an helper to ensure a correct ID ptr ref is built during testing.
func MustIDBase16Ptr(s string) *platform.ID {
	id := MustIDBase16(s)
	return &id
}

func MustCreateOrgs(ctx context.Context, svc influxdb.OrganizationService, os ...*influxdb.Organization) {
	for _, o := range os {
		if err := svc.CreateOrganization(ctx, o); err != nil {
			panic(err)
		}
	}
}

func MustCreateUsers(ctx context.Context, svc influxdb.UserService, us ...*influxdb.User) {
	for _, u := range us {
		if err := svc.CreateUser(ctx, u); err != nil {
			panic(err)
		}
	}
}

func MustNewPermissionAtID(id platform.ID, a influxdb.Action, rt influxdb.ResourceType, orgID platform.ID) *influxdb.Permission {
	perm, err := influxdb.NewPermissionAtID(id, a, rt, orgID)
	if err != nil {
		panic(err)
	}
	return perm
}

func influxErrsEqual(t *testing.T, expected *errors.Error, actual error) {
	t.Helper()

	if expected != nil {
		require.Error(t, actual)
	}

	if actual == nil {
		return
	}

	if expected == nil {
		require.NoError(t, actual)
		return
	}
	iErr, ok := actual.(*errors.Error)
	require.True(t, ok)
	assert.Equal(t, expected.Code, iErr.Code)
	assert.Truef(t, strings.HasPrefix(iErr.Error(), expected.Error()), "expected: %s got err: %s", expected.Error(), actual.Error())
}
