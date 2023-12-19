package authorizer_test

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/authorizer"
	influxdbcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
)

var userCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.User) []*influxdb.User {
		out := append([]*influxdb.User(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

func TestUserService_FindUserByID(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		permission influxdb.Permission
		id         platform.ID
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
			name: "authorized to access id",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID: id,
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
			},
		},
		{
			name: "unauthorized to access id",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID: id,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
				id: 1,
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:users/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindUserByID(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestUserService_FindUser(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		permission influxdb.Permission
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
			name: "authorized to access user",
			fields: fields{
				UserService: &mock.UserService{
					FindUserFn: func(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
						return &influxdb.User{
							ID: 1,
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
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to access user",
			fields: fields{
				UserService: &mock.UserService{
					FindUserFn: func(ctx context.Context, filter influxdb.UserFilter) (*influxdb.User, error) {
						return &influxdb.User{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "read:users/0000000000000001 is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.FindUser(ctx, influxdb.UserFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestUserService_FindUsers(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		permission influxdb.Permission
	}
	type wants struct {
		err   error
		users []*influxdb.User
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "authorized to see all users",
			fields: fields{
				UserService: &mock.UserService{
					FindUsersFn: func(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
						return []*influxdb.User{
							{
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
					},
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID: 1,
					},
					{
						ID: 2,
					},
					{
						ID: 3,
					},
				},
			},
		},
		{
			name: "authorized to access a single user",
			fields: fields{
				UserService: &mock.UserService{
					FindUsersFn: func(ctx context.Context, filter influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
						return []*influxdb.User{
							{
								ID: 1,
							},
							{
								ID: 2,
							},
							{
								ID: 3,
							},
						}, 3, nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(2),
					},
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID: 2,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			users, _, err := s.FindUsers(ctx, influxdb.UserFilter{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)

			if diff := cmp.Diff(users, tt.wants.users, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func TestUserService_UpdateUser(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		id         platform.ID
		permission influxdb.Permission
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
			name: "authorized to update user",
			fields: fields{
				UserService: &mock.UserService{
					UpdateUserFn: func(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
						return &influxdb.User{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
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
			name: "unauthorized to update user",
			fields: fields{
				UserService: &mock.UserService{
					UpdateUserFn: func(ctx context.Context, id platform.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
						return &influxdb.User{
							ID: 1,
						}, nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(1),
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
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			_, err := s.UpdateUser(ctx, tt.args.id, influxdb.UserUpdate{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestUserService_DeleteUser(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		id         platform.ID
		permission influxdb.Permission
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
			name: "authorized to delete user",
			fields: fields{
				UserService: &mock.UserService{
					DeleteUserFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
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
			name: "unauthorized to delete user",
			fields: fields{
				UserService: &mock.UserService{
					DeleteUserFn: func(ctx context.Context, id platform.ID) error {
						return nil
					},
				},
			},
			args: args{
				id: 1,
				permission: influxdb.Permission{
					Action: "read",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(1),
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
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.DeleteUser(ctx, tt.args.id)
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}

func TestUserService_CreateUser(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		permission influxdb.Permission
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
			name: "authorized to create user",
			fields: fields{
				UserService: &mock.UserService{
					CreateUserFn: func(ctx context.Context, o *influxdb.User) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
					},
				},
			},
			wants: wants{
				err: nil,
			},
		},
		{
			name: "unauthorized to create user",
			fields: fields{
				UserService: &mock.UserService{
					CreateUserFn: func(ctx context.Context, o *influxdb.User) error {
						return nil
					},
				},
			},
			args: args{
				permission: influxdb.Permission{
					Action: "write",
					Resource: influxdb.Resource{
						Type: influxdb.UsersResourceType,
						ID:   influxdbtesting.IDPtr(1),
					},
				},
			},
			wants: wants{
				err: &errors.Error{
					Msg:  "write:users is unauthorized",
					Code: errors.EUnauthorized,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := authorizer.NewUserService(tt.fields.UserService)

			ctx := context.Background()
			ctx = influxdbcontext.SetAuthorizer(ctx, mock.NewMockAuthorizer(false, []influxdb.Permission{tt.args.permission}))

			err := s.CreateUser(ctx, &influxdb.User{})
			influxdbtesting.ErrorsEqual(t, err, tt.wants.err)
		})
	}
}
