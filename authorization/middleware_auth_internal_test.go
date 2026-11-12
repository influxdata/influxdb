package authorization

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
)

// notAllowed is the message of the error returned for a permission p that the
// caller may not grant.
func notAllowed(p influxdb.Permission) string {
	return fmt.Sprintf("permission %s is not allowed: %s is unauthorized", p, p)
}

// permission returns a permission with the given action, type, ID and org. A nil
// id or org leaves that field unset.
func permission(a influxdb.Action, rt influxdb.ResourceType, id, org *platform.ID) influxdb.Permission {
	return influxdb.Permission{Action: a, Resource: influxdb.Resource{Type: rt, ID: id, OrgID: org}}
}

func idPtr(id platform.ID) *platform.ID { return &id }

func TestVerifyUserAndOrgPermissions(t *testing.T) {
	org := idPtr(10)
	userByIDWithOrg := permission(influxdb.WriteAction, influxdb.UsersResourceType, idPtr(1), org)
	orgByIDWithOrg := permission(influxdb.ReadAction, influxdb.OrgsResourceType, idPtr(10), org)

	tests := []struct {
		name string
		ps   []influxdb.Permission
		// wantErr is the error message, or "" if the permissions are accepted.
		wantErr string
	}{
		{name: "no permissions"},
		{name: "user by ID with an organization", ps: []influxdb.Permission{userByIDWithOrg}, wantErr: notAllowed(userByIDWithOrg)},
		{name: "organization by ID with an organization", ps: []influxdb.Permission{orgByIDWithOrg}, wantErr: notAllowed(orgByIDWithOrg)},
		{
			name: "user by ID with no organization",
			ps:   []influxdb.Permission{permission(influxdb.WriteAction, influxdb.UsersResourceType, idPtr(1), nil)},
		},
		{
			name: "organization by ID with no organization",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.OrgsResourceType, idPtr(10), nil)},
		},
		{
			name: "users across an organization",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.UsersResourceType, nil, org)},
		},
		{
			name: "another resource type by ID with an organization",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(100), org)},
		},
		{
			name: "the first refused permission of several is named",
			ps: []influxdb.Permission{
				permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(100), org),
				userByIDWithOrg,
				orgByIDWithOrg,
			},
			wantErr: notAllowed(userByIDWithOrg),
		},
		{
			name: "a refused permission after one with no ID",
			ps: []influxdb.Permission{
				permission(influxdb.ReadAction, influxdb.BucketsResourceType, nil, org),
				orgByIDWithOrg,
			},
			wantErr: notAllowed(orgByIDWithOrg),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyUserAndOrgPermissions(tt.ps)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Equal(t, errors.EForbidden, errors.ErrorCode(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAuthedAuthorizationService_verifyBucketPermissions(t *testing.T) {
	const (
		orgID     = platform.ID(10)
		bucketID  = platform.ID(100)
		missingID = platform.ID(200)
		failingID = platform.ID(300)
	)
	inOrg := permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(bucketID), idPtr(orgID))
	missing := permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(missingID), idPtr(orgID))
	failing := permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(failingID), idPtr(orgID))

	tests := []struct {
		name string
		ps   []influxdb.Permission
		// wantLookups is how many bucket lookups the check makes.
		wantLookups int
		// wantErr and wantCode are the error's message and code, or "" if the
		// permissions are accepted.
		wantErr  string
		wantCode string
	}{
		{name: "bucket in the organization", ps: []influxdb.Permission{inOrg}, wantLookups: 1},
		{
			name:        "bucket that does not exist",
			ps:          []influxdb.Permission{missing},
			wantLookups: 1,
			wantErr:     notAllowed(missing),
			wantCode:    errors.EForbidden,
		},
		{
			name:        "bucket lookup fails",
			ps:          []influxdb.Permission{failing},
			wantLookups: 1,
			wantErr:     "bucket lookup failed",
			wantCode:    errors.EInternal,
		},
		{
			name:        "a later permission names a bucket that does not exist",
			ps:          []influxdb.Permission{inOrg, missing},
			wantLookups: 2,
			wantErr:     notAllowed(missing),
			wantCode:    errors.EForbidden,
		},
		{
			name: "bucket by ID with no organization is not looked up",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(missingID), nil)},
		},
		{
			name: "buckets across an organization are not looked up",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.BucketsResourceType, nil, idPtr(orgID))},
		},
		{
			name: "another resource type is not looked up",
			ps:   []influxdb.Permission{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(missingID), idPtr(orgID))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookups := 0
			s := &AuthedAuthorizationService{ts: &tenantService{
				FindBucketByIDFn: func(_ context.Context, id platform.ID) (*influxdb.Bucket, error) {
					lookups++
					switch id {
					case bucketID:
						return &influxdb.Bucket{ID: id, OrgID: orgID, Name: "b1"}, nil
					case failingID:
						return nil, &errors.Error{Code: errors.EInternal, Msg: "bucket lookup failed"}
					}
					return nil, &errors.Error{Code: errors.ENotFound, Msg: "bucket not found"}
				},
			}}

			err := s.verifyBucketPermissions(context.Background(), tt.ps)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Equal(t, tt.wantCode, errors.ErrorCode(err))
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantLookups, lookups)
		})
	}
}

func TestVerifyPermissionsWithoutOrg(t *testing.T) {
	org := idPtr(10)
	requested := permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), nil)

	tests := []struct {
		name string
		// ctx returns the request context, with the caller's authorizer.
		ctx       func() context.Context
		requested []influxdb.Permission
		// wantErr and wantCode are the error's message and code, or "" if the
		// permissions are accepted.
		wantErr  string
		wantCode string
	}{
		{
			name:      "held by ID with no organization",
			ctx:       holding(permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), nil)),
			requested: []influxdb.Permission{requested},
		},
		{
			name:      "held for every resource of the type",
			ctx:       holding(permission(influxdb.ReadAction, influxdb.DashboardsResourceType, nil, nil)),
			requested: []influxdb.Permission{requested},
		},
		{
			name:      "held only by ID with an organization",
			ctx:       holding(permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), org)),
			requested: []influxdb.Permission{requested},
			wantErr:   notAllowed(requested),
			wantCode:  errors.EForbidden,
		},
		{
			name:      "held with no organization, but for another action",
			ctx:       holding(permission(influxdb.WriteAction, influxdb.DashboardsResourceType, idPtr(100), nil)),
			requested: []influxdb.Permission{requested},
			wantErr:   notAllowed(requested),
			wantCode:  errors.EForbidden,
		},
		{
			name:      "a requested permission with an organization isn't checked",
			ctx:       holding(),
			requested: []influxdb.Permission{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), org)},
		},
		{
			name:      "a requested permission with no ID isn't checked",
			ctx:       holding(),
			requested: []influxdb.Permission{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, nil, nil)},
		},
		{
			name:      "no authorizer in the context",
			ctx:       context.Background,
			requested: []influxdb.Permission{requested},
			wantErr:   "authorizer not found on context",
			wantCode:  errors.EInternal,
		},
		{
			name: "inactive token",
			ctx: func() context.Context {
				return icontext.SetAuthorizer(context.Background(), &influxdb.Authorization{
					Status:      influxdb.Inactive,
					Permissions: []influxdb.Permission{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, nil, nil)},
				})
			},
			requested: []influxdb.Permission{requested},
			wantErr:   "token is inactive",
			wantCode:  errors.EUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyPermissionsWithoutOrg(tt.ctx(), tt.requested)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				require.Equal(t, tt.wantCode, errors.ErrorCode(err))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// holding returns a function giving a context whose authorizer holds ps.
func holding(ps ...influxdb.Permission) func() context.Context {
	return func() context.Context {
		return icontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, ps))
	}
}

func TestHeldWithoutOrg(t *testing.T) {
	org := idPtr(10)
	p := permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), nil)

	tests := []struct {
		name string
		held influxdb.PermissionSet
		want bool
	}{
		{name: "nothing held"},
		{
			name: "held by ID with no organization",
			held: influxdb.PermissionSet{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), nil)},
			want: true,
		},
		{
			name: "held by ID with an organization",
			held: influxdb.PermissionSet{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), org)},
		},
		{
			name: "held with no organization for another ID",
			held: influxdb.PermissionSet{permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(200), nil)},
		},
		{
			name: "one of several held permissions matches",
			held: influxdb.PermissionSet{
				permission(influxdb.ReadAction, influxdb.DashboardsResourceType, idPtr(100), org),
				permission(influxdb.ReadAction, influxdb.DashboardsResourceType, nil, nil),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, heldWithoutOrg(tt.held, p))
		})
	}
}

func TestErrPermissionNotAllowed(t *testing.T) {
	p := permission(influxdb.ReadAction, influxdb.BucketsResourceType, idPtr(100), idPtr(10))

	err := errPermissionNotAllowed(p)
	require.EqualError(t, err, notAllowed(p))
	require.Equal(t, errors.EForbidden, errors.ErrorCode(err))

	// It wraps the error authorizer.IsAllowed gives for a permission the caller
	// doesn't hold.
	outer, ok := err.(*errors.Error)
	require.True(t, ok)
	require.EqualError(t, outer.Err, fmt.Sprintf("%s is unauthorized", p))
	require.Equal(t, errors.EUnauthorized, errors.ErrorCode(outer.Err))
}
