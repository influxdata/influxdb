package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/mock"
	influxdbtesting "github.com/influxdata/influxdb/testing"
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
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
	}
	type args struct {
		permission influxdb.Permission
		id         influxdb.ID
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to access id",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{
							ID:     id,
							UserID: 1,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
				id: 1,
			},
			wants: wants{
				err: nil,
				authorizations: []*influxdb.Authorization{
					{
						ID:     10,
						UserID: 1,
					},
				},
			},
		},
		{
			name: "unauthorized to access id",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					FindAuthorizationByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
						return &influxdb.Authorization{
							ID:     id,
							UserID: 1,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.BucketsResourceType,
						ID:   influxdbtesting.IDPtr(20),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "read:users/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
				authorizations: []*influxdb.Authorization{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock.AuthorizationService{}
			m.FindAuthorizationByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     id,
					UserID: 1,
				}, nil
			}
			m.FindAuthorizationByTokenFn = func(ctx context.Context, t string) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     10,
					UserID: 1,
				}, nil
			}
			m.FindAuthorizationsFn = func(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
				return []*influxdb.Authorization{
					{
						ID:     10,
						UserID: 1,
					},
				}, 1, nil
			}
			s := authorizer.NewAuthorizationService(m)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

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
	type fields struct {
		AuthorizationService influxdb.AuthorizationService
	}
	type args struct {
		permission influxdb.Permission
		userID     influxdb.ID
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to create authorization",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					CreateAuthorizationFn: func(ctx context.Context, b *influxdb.Authorization) error {
						return nil
					},
				},
			},
			args: args{
				userID: 1,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create authorization",
			fields: fields{
				AuthorizationService: &mock.AuthorizationService{
					CreateAuthorizationFn: func(ctx context.Context, b *influxdb.Authorization) error {
						return nil
					},
				},
			},
			args: args{
				userID: 1,
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(10),
					},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Msg:  "write:users/0000000000000001 is unauthorized",
					Code: influxdb.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mock.AuthorizationService{}
			m.FindAuthorizationByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
				return &influxdb.Authorization{
					ID:     id,
					UserID: 1,
				}, nil
			}
			m.CreateAuthorizationFn = func(ctx context.Context, a *influxdb.Authorization) error {
				return nil
			}
			m.DeleteAuthorizationFn = func(ctx context.Context, id influxdb.ID) error {
				return nil
			}
			m.SetAuthorizationStatusFn = func(ctx context.Context, id influxdb.ID, s influxdb.Status) error {
				return nil
			}
			s := authorizer.NewAuthorizationService(m)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, &Authorizer{[]influxdb.Permission{tt.args.permission}})

			t.Run("create authorization", func(t *testing.T) {
				err := s.CreateAuthorization(ctx, &influxdb.Authorization{UserID: tt.args.userID})
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

			t.Run("set authorization status", func(t *testing.T) {
				err := s.SetAuthorizationStatus(ctx, 10, influxdb.Active)
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

			t.Run("delete authorization", func(t *testing.T) {
				err := s.DeleteAuthorization(ctx, 10)
				influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
			})

		})
	}
}
