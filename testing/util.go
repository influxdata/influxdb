package testing

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

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

	if platform.ErrorCode(expected) != platform.ErrorCode(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error code %q but received %q", platform.ErrorCode(expected), platform.ErrorCode(actual))
	}

	if platform.ErrorMessage(expected) != platform.ErrorMessage(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error message %q but received %q", platform.ErrorMessage(expected), platform.ErrorMessage(actual))
	}
}

// FloatPtr takes the ref of a float number.
func FloatPtr(f float64) *float64 {
	p := new(float64)
	*p = f
	return p
}

func idPtr(id platform.ID) *platform.ID {
	return &id
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

func MustCreateOrgs(ctx context.Context, svc *kv.Service, os ...*platform.Organization) {
	for _, o := range os {
		if err := svc.CreateOrganization(ctx, o); err != nil {
			panic(err)
		}
	}
}

func MustCreateLabels(ctx context.Context, svc *kv.Service, labels ...*platform.Label) {
	for _, l := range labels {
		if err := svc.CreateLabel(ctx, l); err != nil {
			panic(err)
		}
	}
}

func MustCreateUsers(ctx context.Context, svc *kv.Service, us ...*platform.User) {
	for _, u := range us {
		if err := svc.CreateUser(ctx, u); err != nil {
			panic(err)
		}
	}
}

func MustCreateMappings(ctx context.Context, svc *kv.Service, ms ...*platform.UserResourceMapping) {
	for _, m := range ms {
		if err := svc.CreateUserResourceMapping(ctx, m); err != nil {
			panic(err)
		}
	}
}

func MustMakeUsersOrgOwner(ctx context.Context, svc *kv.Service, oid platform.ID, uids ...platform.ID) {
	ms := make([]*platform.UserResourceMapping, len(uids))
	for i, uid := range uids {
		ms[i] = &platform.UserResourceMapping{
			UserID:       uid,
			UserType:     platform.Owner,
			ResourceType: platform.OrgsResourceType,
			ResourceID:   oid,
		}
	}
	MustCreateMappings(ctx, svc, ms...)
}

func MustMakeUsersOrgMember(ctx context.Context, svc *kv.Service, oid platform.ID, uids ...platform.ID) {
	ms := make([]*platform.UserResourceMapping, len(uids))
	for i, uid := range uids {
		ms[i] = &platform.UserResourceMapping{
			UserID:       uid,
			UserType:     platform.Member,
			ResourceType: platform.OrgsResourceType,
			ResourceID:   oid,
		}
	}
	MustCreateMappings(ctx, svc, ms...)
}

func MustNewPermissionAtID(id platform.ID, a platform.Action, rt platform.ResourceType, orgID platform.ID) *platform.Permission {
	perm, err := platform.NewPermissionAtID(id, a, rt, orgID)
	if err != nil {
		panic(err)
	}
	return perm
}
