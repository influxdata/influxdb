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
						ID:   idFromString(t, userOneID),
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
						ID:   idFromString(t, userOneID),
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
						ID:   idFromString(t, userOneID),
						Name: "user1",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "user2",
					},
				},
			},
		},
		{
			name: "names should not be unique",
			fields: UserFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return idFromString(t, userOneID)
					},
				},
				Users: []*platform.User{
					{
						ID:   idFromString(t, userOneID),
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
						ID:   idFromString(t, userOneID),
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
			defer s.DeleteUser(ctx, tt.args.user.ID)

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
						ID:   idFromString(t, userOneID),
						Name: "user1",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "user2",
					},
				},
			},
			args: args{
				id: idFromString(t, userTwoID),
			},
			wants: wants{
				user: &platform.User{
					ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userOneID),
						Name: "abc",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				users: []*platform.User{
					{
						ID:   idFromString(t, userOneID),
						Name: "abc",
					},
					{
						ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userOneID),
						Name: "abc",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				ID: idFromString(t, userTwoID),
			},
			wants: wants{
				users: []*platform.User{
					{
						ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userOneID),
						Name: "abc",
					},
					{
						ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userTwoID),
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
			if tt.args.ID != nil {
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "orgB",
						ID:   idFromString(t, userTwoID),
					},
				},
			},
			args: args{
				ID: idFromString(t, userOneID),
			},
			wants: wants{
				users: []*platform.User{
					{
						Name: "orgB",
						ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "orgB",
						ID:   idFromString(t, userTwoID),
					},
				},
			},
			args: args{
				ID: idFromString(t, userThreeID),
			},
			wants: wants{
				err: fmt.Errorf("user not found"),
				users: []*platform.User{
					{
						Name: "orgA",
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "orgB",
						ID:   idFromString(t, userTwoID),
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
						ID:   idFromString(t, userOneID),
						Name: "abc",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				user: &platform.User{
					ID:   idFromString(t, userOneID),
					Name: "abc",
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
						ID:   idFromString(t, userOneID),
						Name: "user1",
					},
					{
						ID:   idFromString(t, userTwoID),
						Name: "user2",
					},
				},
			},
			args: args{
				id:   idFromString(t, userOneID),
				name: "changed",
			},
			wants: wants{
				user: &platform.User{
					ID:   idFromString(t, userOneID),
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
