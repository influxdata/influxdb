package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	userOneID   = "020f755c3c082000"
	userTwoID   = "020f755c3c082001"
	userThreeID = "020f755c3c082002"
)

var userCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Comparer(func(x, y *influxdb.User) bool {
		if x == nil && y == nil {
			return true
		}
		if x != nil && y == nil || y != nil && x == nil {
			return false
		}
		return x.Name == y.Name && x.OAuthID == y.OAuthID && x.Status == y.Status
	}),
	cmp.Transformer("Sort", func(in []*influxdb.User) []*influxdb.User {
		out := append([]*influxdb.User(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// UserFields will include the IDGenerator, and users
type UserFields struct {
	IDGenerator platform.IDGenerator
	Users       []*influxdb.User
}

// UserService tests all the service functions.
func UserService(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateUser",
			fn:   CreateUser,
		},
		{
			name: "FindUserByID",
			fn:   FindUserByID,
		},
		{
			name: "FindUsers",
			fn:   FindUsers,
		},
		{
			name: "DeleteUser",
			fn:   DeleteUser,
		},
		{
			name: "FindUser",
			fn:   FindUser,
		},
		{
			name: "UpdateUser",
			fn:   UpdateUser,
		},
		{
			name: "UpdateUser_IndexHygiene",
			fn:   UpdateUser_IndexHygiene,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// CreateUser testing
func CreateUser(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		user *influxdb.User
	}
	type wants struct {
		err   error
		users []*influxdb.User
	}

	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "create users with empty set",
			fields: UserFields{
				IDGenerator: mock.NewIDGenerator(userOneID, t),
				Users:       []*influxdb.User{},
			},
			args: args{
				user: &influxdb.User{
					Name:   "name1",
					Status: influxdb.Active,
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						Name:   "name1",
						ID:     MustIDBase16(userOneID),
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "basic create user",
			fields: UserFields{
				IDGenerator: mock.NewIDGenerator(userTwoID, t),
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				user: &influxdb.User{
					Name:   "user2",
					Status: influxdb.Active,
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "names should be unique",
			fields: UserFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(userOneID)
					},
				},
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				user: &influxdb.User{
					Name: "user1",
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
				},
				err: &errors.Error{
					Code: errors.EConflict,
					Op:   influxdb.OpCreateUser,
					Msg:  "user with name user1 already exists",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateUser(ctx, tt.args.user)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			// Delete only created users - ie., having a not nil ID
			if tt.args.user.ID.Valid() {
				defer s.DeleteUser(ctx, tt.args.user.ID)
			}

			users, _, err := s.FindUsers(ctx, influxdb.UserFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve users: %v", err)
			}
			if diff := cmp.Diff(users, tt.wants.users, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindUserByID testing
func FindUserByID(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err  error
		user *influxdb.User
	}

	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "basic find user by id",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				id: MustIDBase16(userTwoID),
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userTwoID),
					Name:   "user2",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "find user by id not exists",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpFindUserByID,
					Msg:  "user not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			user, err := s.FindUserByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("user is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindUsers testing
func FindUsers(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID          platform.ID
		name        string
		findOptions influxdb.FindOptions
	}

	type wants struct {
		users []*influxdb.User
		err   error
	}
	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "find all users",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "find user by id",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				ID: MustIDBase16(userTwoID),
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "find user by name",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "find user by id not exists",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				ID: MustIDBase16(threeID),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpFindUsers,
					Msg:  "user not found",
				},
			},
		},
		{
			name: "find user by name not exists",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "no_exist",
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpFindUsers,
					Msg:  "user not found",
				},
			},
		},
		{
			name: "find all users by offset and limit",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userThreeID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					Offset: 1,
					Limit:  1,
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "find all users by after and limit",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userThreeID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					After: MustIDBase16Ptr(userOneID),
					Limit: 2,
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userThreeID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "find all users by descending",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userThreeID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					Offset:     1,
					Descending: true,
				},
			},
			wants: wants{
				users: []*influxdb.User{
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "def",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := influxdb.UserFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			users, _, err := s.FindUsers(ctx, filter, tt.args.findOptions)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(users, tt.wants.users, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteUser testing
func DeleteUser(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err   error
		users []*influxdb.User
	}

	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "delete users using exist id",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						Name:   "orgA",
						ID:     MustIDBase16(userOneID),
						Status: influxdb.Active,
					},
					{
						Name:   "orgB",
						ID:     MustIDBase16(userTwoID),
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				ID: MustIDBase16(userOneID),
			},
			wants: wants{
				users: []*influxdb.User{
					{
						Name:   "orgB",
						ID:     MustIDBase16(userTwoID),
						Status: influxdb.Active,
					},
				},
			},
		},
		{
			name: "delete users using id that does not exist",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						Name:   "orgA",
						ID:     MustIDBase16(userOneID),
						Status: influxdb.Active,
					},
					{
						Name:   "orgB",
						ID:     MustIDBase16(userTwoID),
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				ID: MustIDBase16(userThreeID),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpDeleteUser,
					Msg:  "user not found",
				},
				users: []*influxdb.User{
					{
						Name:   "orgA",
						ID:     MustIDBase16(userOneID),
						Status: influxdb.Active,
					},
					{
						Name:   "orgB",
						ID:     MustIDBase16(userTwoID),
						Status: influxdb.Active,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteUser(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			filter := influxdb.UserFilter{}
			users, _, err := s.FindUsers(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve users: %v", err)
			}
			if diff := cmp.Diff(users, tt.wants.users, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindUser testing
func FindUser(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.UserFilter
	}

	type wants struct {
		user *influxdb.User
		err  error
	}

	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "find user by name",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				filter: influxdb.UserFilter{
					Name: func(s string) *string { return &s }("abc"),
				},
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "abc",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "find existing user by its id",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				filter: influxdb.UserFilter{
					ID: func(id platform.ID) *platform.ID { return &id }(MustIDBase16(userOneID)),
				},
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "abc",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "user with name does not exist",
			fields: UserFields{
				Users: []*influxdb.User{},
			},
			args: args{
				filter: influxdb.UserFilter{
					Name: func(s string) *string { return &s }("abc"),
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "user not found",
					Op:   influxdb.OpFindUser,
				},
			},
		},
		{
			name: "user with id does not exist",
			fields: UserFields{
				Users: []*influxdb.User{},
			},
			args: args{
				filter: influxdb.UserFilter{
					ID: func(id platform.ID) *platform.ID { return &id }(MustIDBase16(userOneID)),
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "user not found",
					Op:   influxdb.OpFindUser,
				},
			},
		},
		{
			name: "filter with both name and ID prefers ID",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "abc",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "xyz",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				filter: influxdb.UserFilter{
					ID:   func(id platform.ID) *platform.ID { return &id }(MustIDBase16(userOneID)),
					Name: func(s string) *string { return &s }("xyz"),
				},
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "abc",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "filter with no name nor id returns error",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				filter: influxdb.UserFilter{},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "user not found",
					Op:   influxdb.OpFindUser,
				},
			},
		},
		{
			name: "filter both name and non-existent id returns no user",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				filter: influxdb.UserFilter{
					ID:   func(id platform.ID) *platform.ID { return &id }(MustIDBase16(userOneID)),
					Name: func(s string) *string { return &s }("xyz"),
				},
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Msg:  "user not found",
					Op:   influxdb.OpFindUser,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			user, err := s.FindUser(ctx, tt.args.filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateUser testing
func UpdateUser(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {
	type args struct {
		name   string
		id     platform.ID
		status string
	}
	type wants struct {
		err  error
		user *influxdb.User
	}

	tests := []struct {
		name   string
		fields UserFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				id:   MustIDBase16(userOneID),
				name: "changed",
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "changed",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "update name to same name",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				id:   MustIDBase16(userOneID),
				name: "user1",
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "user1",
					Status: influxdb.Active,
				},
			},
		},
		{
			name: "update status",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:     MustIDBase16(userOneID),
						Name:   "user1",
						Status: influxdb.Active,
					},
					{
						ID:     MustIDBase16(userTwoID),
						Name:   "user2",
						Status: influxdb.Active,
					},
				},
			},
			args: args{
				id:     MustIDBase16(userOneID),
				status: "inactive",
			},
			wants: wants{
				user: &influxdb.User{
					ID:     MustIDBase16(userOneID),
					Name:   "user1",
					Status: "inactive",
				},
			},
		},
		{
			name: "update name with id not exists",
			fields: UserFields{
				Users: []*influxdb.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "user1",
					},
					{
						ID:   MustIDBase16(userTwoID),
						Name: "user2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(threeID),
				name: "changed",
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.ENotFound,
					Op:   influxdb.OpUpdateUser,
					Msg:  "user not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := influxdb.UserUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			switch tt.args.status {
			case "inactive":
				status := influxdb.Inactive
				upd.Status = &status
			case "active":
				status := influxdb.Inactive
				upd.Status = &status
			}

			user, err := s.UpdateUser(ctx, tt.args.id, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("user is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func UpdateUser_IndexHygiene(
	init func(UserFields, *testing.T) (influxdb.UserService, string, func()),
	t *testing.T,
) {

	oldUserName := "user1"
	users := UserFields{
		Users: []*influxdb.User{
			{
				ID:     MustIDBase16(userOneID),
				Name:   oldUserName,
				Status: "active",
			},
		},
	}
	s, _, done := init(users, t)
	defer done()

	newUserName := "user1Updated"
	upd := influxdb.UserUpdate{
		Name: &newUserName,
	}

	ctx := context.Background()
	_, err := s.UpdateUser(ctx, MustIDBase16(userOneID), upd)
	if err != nil {
		t.Error(err)
	}

	// Ensure we can find the user with the new name.
	_, nerr := s.FindUser(ctx, influxdb.UserFilter{
		Name: &newUserName,
	})
	if nerr != nil {
		t.Error("unexpected error when finding user by name", nerr)
	}

	// Ensure we cannot find a user with the old name. The index used when
	// searching by name should have been cleared out by the UpdateUser
	// operation.
	_, oerr := s.FindUser(ctx, influxdb.UserFilter{
		Name: &oldUserName,
	})
	ErrorsEqual(t, oerr, &errors.Error{
		Code: errors.ENotFound,
		Op:   influxdb.OpFindUser,
		Msg:  "user not found",
	})
}
