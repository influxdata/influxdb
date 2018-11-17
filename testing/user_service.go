package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
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
	cmp.Transformer("Sort", func(in []*platform.User) []*platform.User {
		out := append([]*platform.User(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// UserFields will include the IDGenerator, and users
type UserFields struct {
	IDGenerator platform.IDGenerator
	Users       []*platform.User
}

// UserService tests all the service functions.
func UserService(
	init func(UserFields, *testing.T) (platform.UserService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(UserFields, *testing.T) (platform.UserService, func()),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateUser testing
func CreateUser(
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		user *platform.User
	}
	type wants struct {
		err   error
		users []*platform.User
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
				Users:       []*platform.User{},
			},
			args: args{
				user: &platform.User{
					Name: "name1",
				},
			},
			wants: wants{
				users: []*platform.User{
					{
						Name: "name1",
						ID:   MustIDBase16(userOneID),
					},
				},
			},
		},
		{
			name: "basic create user",
			fields: UserFields{
				IDGenerator: mock.NewIDGenerator(userTwoID, t),
				Users: []*platform.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "user1",
					},
				},
			},
			args: args{
				user: &platform.User{
					Name: "user2",
				},
			},
			wants: wants{
				users: []*platform.User{
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
		},
		{
			name: "names should be unique",
			fields: UserFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return MustIDBase16(userOneID)
					},
				},
				Users: []*platform.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "user1",
					},
				},
			},
			args: args{
				user: &platform.User{
					Name: "user1",
				},
			},
			wants: wants{
				users: []*platform.User{
					{
						ID:   MustIDBase16(userOneID),
						Name: "user1",
					},
				},
				err: fmt.Errorf("user with name user1 already exists"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.CreateUser(ctx, tt.args.user)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			// Delete only created users - ie., having a not nil ID
			if tt.args.user.ID.Valid() {
				defer s.DeleteUser(ctx, tt.args.user.ID)
			}

			users, _, err := s.FindUsers(ctx, platform.UserFilter{})
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
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err  error
		user *platform.User
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
				Users: []*platform.User{
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
				id: MustIDBase16(userTwoID),
			},
			wants: wants{
				user: &platform.User{
					ID:   MustIDBase16(userTwoID),
					Name: "user2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			user, err := s.FindUserByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("user is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindUsers testing
func FindUsers(
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		ID   platform.ID
		name string
	}

	type wants struct {
		users []*platform.User
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
				Users: []*platform.User{
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
			args: args{},
			wants: wants{
				users: []*platform.User{
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
		},
		{
			name: "find user by id",
			fields: UserFields{
				Users: []*platform.User{
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
				ID: MustIDBase16(userTwoID),
			},
			wants: wants{
				users: []*platform.User{
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find user by name",
			fields: UserFields{
				Users: []*platform.User{
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
				name: "xyz",
			},
			wants: wants{
				users: []*platform.User{
					{
						ID:   MustIDBase16(userTwoID),
						Name: "xyz",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			filter := platform.UserFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			users, _, err := s.FindUsers(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(users, tt.wants.users, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteUser testing
func DeleteUser(
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err   error
		users []*platform.User
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
				Users: []*platform.User{
					{
						Name: "orgA",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(userTwoID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(userOneID),
			},
			wants: wants{
				users: []*platform.User{
					{
						Name: "orgB",
						ID:   MustIDBase16(userTwoID),
					},
				},
			},
		},
		{
			name: "delete users using id that does not exist",
			fields: UserFields{
				Users: []*platform.User{
					{
						Name: "orgA",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(userTwoID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(userThreeID),
			},
			wants: wants{
				err: fmt.Errorf("<not found> user not found"),
				users: []*platform.User{
					{
						Name: "orgA",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(userTwoID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteUser(ctx, tt.args.ID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.UserFilter{}
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
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		name string
	}

	type wants struct {
		user *platform.User
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
				Users: []*platform.User{
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
				name: "abc",
			},
			wants: wants{
				user: &platform.User{
					ID:   MustIDBase16(userOneID),
					Name: "abc",
				},
			},
		},
		{
			name: "user does not exist",
			fields: UserFields{
				Users: []*platform.User{},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				err: fmt.Errorf("<not found> user not found"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			filter := platform.UserFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			user, err := s.FindUser(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("users are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateUser testing
func UpdateUser(
	init func(UserFields, *testing.T) (platform.UserService, func()),
	t *testing.T,
) {
	type args struct {
		name string
		id   platform.ID
	}
	type wants struct {
		err  error
		user *platform.User
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
				Users: []*platform.User{
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
				id:   MustIDBase16(userOneID),
				name: "changed",
			},
			wants: wants{
				user: &platform.User{
					ID:   MustIDBase16(userOneID),
					Name: "changed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			upd := platform.UserUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			user, err := s.UpdateUser(ctx, tt.args.id, upd)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(user, tt.wants.user, userCmpOptions...); diff != "" {
				t.Errorf("user is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
