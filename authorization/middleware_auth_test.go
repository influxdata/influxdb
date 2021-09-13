package authorization_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorization"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
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
