package authorization_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

var authorizationCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Authorization) []*influxdb.Authorization {
		out := append([]*influxdb.Authorization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestAuthorizationService_ReadAuthorization(t *testing.T) {
	type args struct {
		permissions []influxdb.Permission
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "authorized to access id",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
				authorizations: []*influxdb.Authorization{
					{
						ID:     10,
						UserID: 1,
						OrgID:  1,
					},
				},
			},
		},
		{
			name: "unauthorized to access id - wrong org",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(2),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:orgs/0000000000000001/authorizations/000000000000000a is unauthorized",
					Code: errors.EUnauthorized,
				},
				authorizations: []*influxdb.Authorization{},
			},
		},
		{
			name: "unauthorized to access id - wrong user",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.ReadAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:users/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
				authorizations: []*influxdb.Authorization{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock.AuthorizationService{}
			m.FindAuthorizationByIDFn = func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     id,
					UserID: 1,
					OrgID:  1,
				}, nil
			}
			m.FindAuthorizationByTokenFn = func(ctx context.Context, t string) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     10,
					UserID: 1,
					OrgID:  1,
				}, nil
			}
			m.FindAuthorizationsFn = func(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
				return []*influxdb.Authorization{
					{
						ID:     10,
						UserID: 1,
						OrgID:  1,
					},
				}, 1, nil
			}
			// set up tenant service
			ctx := context.Background()
			st := inmem.NewKVStore()
			if err := all.Up(ctx, zaptest.NewLogger(t), st); err != nil {
				t.Fatal(err)
			}

			store := tenant.NewStore(st)
			ts := tenant.NewService(store)
			s := authorization.NewAuthedAuthorizationService(m, ts)

			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			t.Run("find authorization by id", func(t *testing.T) {
				_, err := s.FindAuthorizationByID(ctx, 10)
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})
			t.Run("find authorization by token", func(t *testing.T) {
				_, err := s.FindAuthorizationByToken(ctx, "10")
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

			t.Run("find authorizations", func(t *testing.T) {
				as, _, err := s.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
				influxdbtesting.ErrorsEqual(t, err, nil)

				if diff := cmp.Diff(as, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
					t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
				}
			})
		})
	}
}

func TestAuthorizationService_WriteAuthorization(t *testing.T) {
	type args struct {
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "authorized to write authorization",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to write authorization - wrong org",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(2),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000001/authorizations/000000000000000a is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to write authorization - wrong user",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:users/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock.AuthorizationService{}
			m.FindAuthorizationByIDFn = func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     id,
					UserID: 1,
					OrgID:  1,
				}, nil
			}
			m.CreateAuthorizationFn = func(ctx context.Context, a *influxdb.Authorization) error {
				return nil
			}
			m.DeleteAuthorizationFn = func(ctx context.Context, id platform.ID) error {
				return nil
			}
			m.UpdateAuthorizationFn = func(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
				return nil, nil
			}
			// set up tenant service
			ctx := context.Background()
			st := inmem.NewKVStore()
			if err := all.Up(ctx, zaptest.NewLogger(t), st); err != nil {
				t.Fatal(err)
			}

			store := tenant.NewStore(st)
			ts := tenant.NewService(store)
			s := authorization.NewAuthedAuthorizationService(m, ts)

			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			t.Run("update authorization", func(t *testing.T) {
				_, err := s.UpdateAuthorization(ctx, 10, &influxdb.AuthorizationUpdate{Status: influxdb.Active.Ptr()})
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

			t.Run("delete authorization", func(t *testing.T) {
				err := s.DeleteAuthorization(ctx, 10)
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

		})
	}
}

func TestAuthorizationService_CreateAuthorization(t *testing.T) {
	type args struct {
		permissions []influxdb.Permission
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "authorized to write authorization",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to write authorization - wrong org",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(2),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(1),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:orgs/0000000000000001/authorizations is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
		{
			name: "unauthorized to write authorization - wrong user",
			args: args{
				permissions: []influxdb.Permission{
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type:  influxdb.AuthorizationsResourceType,
							OrgID: influxdbtesting.IDPtr(1),
						},
					},
					{
						Action: influxdb.WriteAction,
						Resource: influxdb.Resource{
							Type: influxdb.UsersResourceType,
							ID:   influxdbtesting.IDPtr(2),
						},
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:users/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock.AuthorizationService{}
			m.FindAuthorizationByIDFn = func(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     id,
					UserID: 1,
					OrgID:  1,
				}, nil
			}
			m.CreateAuthorizationFn = func(ctx context.Context, a *influxdb.Authorization) error {
				return nil
			}
			m.DeleteAuthorizationFn = func(ctx context.Context, id platform.ID) error {
				return nil
			}
			m.UpdateAuthorizationFn = func(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
				return nil, nil
			}
			// set up tenant service
			st := inmem.NewKVStore()
			ctx := context.Background()
			if err := all.Up(ctx, zaptest.NewLogger(t), st); err != nil {
				t.Fatal(err)
			}

			store := tenant.NewStore(st)
			ts := tenant.NewService(store)
			s := authorization.NewAuthedAuthorizationService(m, ts)

			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, tt.args.permissions))

			err := s.CreateAuthorization(ctx, &influxdb.Authorization{OrgID: 1, UserID: 1})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestAuthorizationService_CreateAuthorization_BucketPermissions(t *testing.T) {
	ctx := context.Background()
	st := inmem.NewKVStore()
	require.NoError(t, all.Up(ctx, zaptest.NewLogger(t), st))
	ts := tenant.NewService(tenant.NewStore(st))

	org := &influxdb.Organization{Name: "org"}
	require.NoError(t, ts.CreateOrganization(ctx, org))
	bucket := &influxdb.Bucket{OrgID: org.ID, Name: "bucket"}
	require.NoError(t, ts.CreateBucket(ctx, bucket))

	const userID = platform.ID(1)
	missingID := platform.ID(0x0bad0bad0bad0bad)
	_, err := ts.FindBucketByID(ctx, missingID)
	require.EqualErrorf(t, err, "bucket not found", "setup: %s must not be a bucket", missingID)

	bucketPerm := func(id platform.ID, orgID *platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   influxdb.ReadAction,
			Resource: influxdb.Resource{Type: influxdb.BucketsResourceType, ID: &id, OrgID: orgID},
		}
	}

	// The caller may create authorizations in the org for itself, and holds
	// read on every bucket, so authorizer.VerifyPermissions accepts each bucket
	// permission below.
	caller := append(influxdb.OwnerPermissions(org.ID), influxdb.MePermissions(userID)...)
	caller = append(caller, influxdb.Permission{
		Action:   influxdb.ReadAction,
		Resource: influxdb.Resource{Type: influxdb.BucketsResourceType},
	})

	// createWith runs CreateAuthorization in org as the caller, using the given
	// tenant service, and reports whether the request reached the underlying
	// service.
	createWith := func(t *testing.T, tenants authorization.TenantService, ps ...influxdb.Permission) (bool, error) {
		t.Helper()
		var created bool
		m := &mock.AuthorizationService{
			CreateAuthorizationFn: func(context.Context, *influxdb.Authorization) error {
				created = true
				return nil
			},
		}
		s := authorization.NewAuthedAuthorizationService(m, tenants)
		actx := influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, caller))
		err := s.CreateAuthorization(actx, &influxdb.Authorization{OrgID: org.ID, UserID: userID, Permissions: ps})
		return created, err
	}
	create := func(t *testing.T, ps ...influxdb.Permission) (bool, error) {
		t.Helper()
		return createWith(t, ts, ps...)
	}

	t.Run("bucket in the organization", func(t *testing.T) {
		created, err := create(t, bucketPerm(bucket.ID, &org.ID))
		require.NoError(t, err)
		require.True(t, created)
	})

	t.Run("bucket that does not exist", func(t *testing.T) {
		p := bucketPerm(missingID, &org.ID)
		created, err := create(t, p)
		require.EqualError(t, err, notAllowedError(p))
		require.Equal(t, errors.EForbidden, errors.ErrorCode(err))
		require.False(t, created)

		// That's the error authorizer.VerifyPermissions gives a caller that
		// doesn't hold p.
		noPerms := influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, nil))
		require.EqualError(t, authorizer.VerifyPermissions(noPerms, []influxdb.Permission{p}), notAllowedError(p))
	})

	t.Run("bucket permission without an organization", func(t *testing.T) {
		created, err := create(t, bucketPerm(missingID, nil))
		require.NoError(t, err)
		require.True(t, created)
	})

	t.Run("later permission names a bucket that does not exist", func(t *testing.T) {
		created, err := create(t, bucketPerm(bucket.ID, &org.ID), bucketPerm(missingID, &org.ID))
		require.EqualError(t, err, notAllowedError(bucketPerm(missingID, &org.ID)))
		require.Equal(t, errors.EForbidden, errors.ErrorCode(err))
		require.False(t, created)
	})

	t.Run("permission for another resource type", func(t *testing.T) {
		// The ID belongs to a dashboard, so it isn't looked up as a bucket.
		created, err := create(t, influxdb.Permission{
			Action:   influxdb.ReadAction,
			Resource: influxdb.Resource{Type: influxdb.DashboardsResourceType, ID: &missingID, OrgID: &org.ID},
		})
		require.NoError(t, err)
		require.True(t, created)
	})

	t.Run("bucket lookup fails", func(t *testing.T) {
		lookupErr := &errors.Error{Code: errors.EInternal, Msg: "bucket lookup failed"}
		created, err := createWith(t, failingBucketLookup{TenantService: ts, err: lookupErr}, bucketPerm(bucket.ID, &org.ID))
		require.EqualError(t, err, "bucket lookup failed")
		require.Equal(t, errors.EInternal, errors.ErrorCode(err))
		require.False(t, created)
	})
}

// notAllowedError is the error message authorizer.VerifyPermissions gives a
// caller that doesn't hold permission p.
func notAllowedError(p influxdb.Permission) string {
	return fmt.Sprintf("permission %s is not allowed: %s is unauthorized", p, p)
}

// TestAuthorizationService_CreateAuthorization_PermissionWithoutOrg creates a
// permission that names a resource by ID with no organization. That's wider than
// the same permission with an organization, so the caller must hold it without an
// organization too.
func TestAuthorizationService_CreateAuthorization_PermissionWithoutOrg(t *testing.T) {
	const (
		orgID  = platform.ID(10)
		userID = platform.ID(1)
		resID  = platform.ID(100)
	)

	perm := func(rt influxdb.ResourceType, id, org *platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   influxdb.ReadAction,
			Resource: influxdb.Resource{Type: rt, ID: id, OrgID: org},
		}
	}

	for _, rt := range []influxdb.ResourceType{influxdb.BucketsResourceType, influxdb.DashboardsResourceType} {
		t.Run(string(rt), func(t *testing.T) {
			tests := []struct {
				name    string
				held    influxdb.Permission
				allowed bool
			}{
				{
					name:    "held by ID without an organization",
					held:    perm(rt, influxdbtesting.IDPtr(resID), nil),
					allowed: true,
				},
				{
					name:    "held for every resource of the type",
					held:    perm(rt, nil, nil),
					allowed: true,
				},
				{
					name: "held by ID with an organization",
					held: perm(rt, influxdbtesting.IDPtr(resID), influxdbtesting.IDPtr(orgID)),
				},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					var created bool
					m := &mock.AuthorizationService{
						CreateAuthorizationFn: func(context.Context, *influxdb.Authorization) error {
							created = true
							return nil
						},
					}
					caller := append(influxdb.OwnerPermissions(orgID), influxdb.MePermissions(userID)...)
					caller = append(caller, tt.held)
					ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, caller))

					requested := perm(rt, influxdbtesting.IDPtr(resID), nil)
					// No tenant service: this check doesn't look anything up.
					s := authorization.NewAuthedAuthorizationService(m, nil)
					err := s.CreateAuthorization(ctx, &influxdb.Authorization{
						OrgID:       orgID,
						UserID:      userID,
						Permissions: []influxdb.Permission{requested},
					})

					if tt.allowed {
						require.NoError(t, err)
						require.True(t, created)
						return
					}

					// The error a caller that doesn't hold the permission gets.
					require.EqualError(t, err, notAllowedError(requested))
					require.Equal(t, errors.EForbidden, errors.ErrorCode(err))
					require.False(t, created)
				})
			}
		})
	}
}

// TestAuthorizationService_CreateAuthorization_UserAndOrgPermissionWithOrg
// creates permissions that name a user or an organization by ID. Users and
// organizations don't belong to an organization, so a permission that names one
// by ID can't have one.
func TestAuthorizationService_CreateAuthorization_UserAndOrgPermissionWithOrg(t *testing.T) {
	const (
		orgID  = platform.ID(10)
		userID = platform.ID(1)
	)

	perm := func(a influxdb.Action, rt influxdb.ResourceType, id, org *platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   a,
			Resource: influxdb.Resource{Type: rt, ID: id, OrgID: org},
		}
	}
	allAccess := allAccessPermissions(orgID, userID)

	userWithOrg := perm(influxdb.WriteAction, influxdb.UsersResourceType, influxdbtesting.IDPtr(userID), influxdbtesting.IDPtr(orgID))
	orgWithOrg := perm(influxdb.ReadAction, influxdb.OrgsResourceType, influxdbtesting.IDPtr(orgID), influxdbtesting.IDPtr(orgID))

	tests := []struct {
		name      string
		requested []influxdb.Permission
		allowed   bool
		// refused is the permission named in the error when allowed is false.
		refused influxdb.Permission
	}{
		{
			name:      "user by ID with an organization",
			requested: []influxdb.Permission{userWithOrg},
			refused:   userWithOrg,
		},
		{
			name:      "organization by ID with an organization",
			requested: []influxdb.Permission{orgWithOrg},
			refused:   orgWithOrg,
		},
		{
			name: "one permission of several names a user by ID with an organization",
			requested: []influxdb.Permission{
				perm(influxdb.ReadAction, influxdb.BucketsResourceType, nil, influxdbtesting.IDPtr(orgID)),
				userWithOrg,
			},
			refused: userWithOrg,
		},
		{
			name:      "users across the organization",
			requested: []influxdb.Permission{perm(influxdb.ReadAction, influxdb.UsersResourceType, nil, influxdbtesting.IDPtr(orgID))},
			allowed:   true,
		},
		{
			name:      "all-access permissions",
			requested: allAccess,
			allowed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var created bool
			m := &mock.AuthorizationService{
				CreateAuthorizationFn: func(context.Context, *influxdb.Authorization) error {
					created = true
					return nil
				},
			}
			caller := append(influxdb.OwnerPermissions(orgID), influxdb.MePermissions(userID)...)
			ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, caller))

			// No tenant service: this check doesn't look anything up.
			s := authorization.NewAuthedAuthorizationService(m, nil)
			err := s.CreateAuthorization(ctx, &influxdb.Authorization{
				OrgID:       orgID,
				UserID:      userID,
				Permissions: tt.requested,
			})

			if tt.allowed {
				require.NoError(t, err)
				require.True(t, created)
				return
			}

			// The error a caller that doesn't hold the permission gets.
			require.EqualError(t, err, notAllowedError(tt.refused))
			require.Equal(t, errors.EForbidden, errors.ErrorCode(err))
			require.False(t, created)
		})
	}
}

// allAccessPermissions returns the permissions the CLI and UI give an all-access
// token: the organization and the user by ID with no organization, and every
// other type across the organization.
func allAccessPermissions(orgID, userID platform.ID) []influxdb.Permission {
	perm := func(a influxdb.Action, rt influxdb.ResourceType, id, org *platform.ID) influxdb.Permission {
		return influxdb.Permission{
			Action:   a,
			Resource: influxdb.Resource{Type: rt, ID: id, OrgID: org},
		}
	}
	actions := []influxdb.Action{influxdb.ReadAction, influxdb.WriteAction}

	var ps []influxdb.Permission
	for _, rt := range influxdb.AllResourceTypes {
		switch rt {
		case influxdb.InstanceResourceType:
			continue
		case influxdb.OrgsResourceType:
			ps = append(ps, perm(influxdb.ReadAction, rt, influxdbtesting.IDPtr(orgID), nil))
		case influxdb.UsersResourceType:
			for _, a := range actions {
				ps = append(ps, perm(a, rt, influxdbtesting.IDPtr(userID), nil))
			}
		default:
			for _, a := range actions {
				ps = append(ps, perm(a, rt, nil, influxdbtesting.IDPtr(orgID)))
			}
		}
	}
	return ps
}

// TestAuthorizationService_CreateAuthorization_FromAllAccess creates a token
// with the all-access permissions, using a token that has them.
func TestAuthorizationService_CreateAuthorization_FromAllAccess(t *testing.T) {
	const (
		orgID  = platform.ID(10)
		userID = platform.ID(1)
	)

	var created bool
	m := &mock.AuthorizationService{
		CreateAuthorizationFn: func(context.Context, *influxdb.Authorization) error {
			created = true
			return nil
		},
	}
	allAccess := allAccessPermissions(orgID, userID)
	ctx := influxdbcontext.SetAuthorizer(context.Background(), mock.NewMockAuthorizer(false, allAccess))

	// No tenant service: no permission here names a bucket by ID.
	s := authorization.NewAuthedAuthorizationService(m, nil)
	err := s.CreateAuthorization(ctx, &influxdb.Authorization{
		OrgID:       orgID,
		UserID:      userID,
		Permissions: allAccess,
	})
	require.NoError(t, err)
	require.True(t, created)
}

// failingBucketLookup is a tenant service whose bucket lookups fail with err.
type failingBucketLookup struct {
	authorization.TenantService
	err error
}

func (f failingBucketLookup) FindBucketByID(context.Context, platform.ID) (*influxdb.Bucket, error) {
	return nil, f.err
}
