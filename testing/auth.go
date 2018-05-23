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

var authorizationCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Authorization) []*platform.Authorization {
		out := append([]*platform.Authorization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// AuthorizationFields will include the IDGenerator, and authorizations
type AuthorizationFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	Authorizations []*platform.Authorization
	Users          []*platform.User
}

// CreateAuthorization testing
func CreateAuthorization(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, func()),
	t *testing.T,
) {
	type args struct {
		authorization *platform.Authorization
		user          string
	}
	type wants struct {
		err            error
		authorizations []*platform.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "create authorizations with empty set",
			fields: AuthorizationFields{
				IDGenerator: mock.NewIDGenerator("id1"),
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Authorizations: []*platform.Authorization{},
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					User: "cooluser",
					Permissions: []platform.Permission{
						platform.CreateUserPermission,
						platform.DeleteUserPermission,
					},
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("id1"),
						UserID: platform.ID("user1"),
						Token:  "rand",
						User:   "cooluser",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
				},
			},
		},
		{
			name: "basic create authorization",
			fields: AuthorizationFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "supersecret",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					User: "regularuser",
					Permissions: []platform.Permission{
						platform.CreateUserPermission,
					},
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						User:   "cooluser",
						Token:  "supersecret",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						User:   "regularuser",
						Token:  "rand",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
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
			err := s.CreateAuthorization(ctx, tt.args.authorization)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteAuthorization(ctx, tt.args.authorization.ID)

			authorizations, _, err := s.FindAuthorizations(ctx, platform.AuthorizationFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve authorizations: %v", err)
			}
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizationByID testing
func FindAuthorizationByID(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err           error
		authorization *platform.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic find authorization by id",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				id: platform.ID("2"),
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:     platform.ID("2"),
					UserID: platform.ID("user2"),
					User:   "regularuser",
					Token:  "rand2",
					Permissions: []platform.Permission{
						platform.CreateUserPermission,
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

			authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizationByToken testing
func FindAuthorizationByToken(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, func()),
	t *testing.T,
) {
	type args struct {
		token string
	}
	type wants struct {
		err           error
		authorization *platform.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic find authorization by token",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("0"),
						UserID: platform.ID("user1"),
						Token:  "rand0",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     platform.ID("3"),
						UserID: platform.ID("user1"),
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				token: "rand1",
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:     platform.ID("1"),
					UserID: platform.ID("user1"),
					User:   "cooluser",
					Token:  "rand1",
					Permissions: []platform.Permission{
						platform.CreateUserPermission,
						platform.DeleteUserPermission,
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

			authorization, err := s.FindAuthorizationByToken(ctx, tt.args.token)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizations testing
func FindAuthorizations(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, func()),
	t *testing.T,
) {
	type args struct {
		ID     string
		UserID string
		token  string
	}

	type wants struct {
		authorizations []*platform.Authorization
		err            error
	}
	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "find all authorizations",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						User:   "cooluser",
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						User:   "regularuser",
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
		},
		{
			name: "find authorization by user id",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     platform.ID("3"),
						UserID: platform.ID("user1"),
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				UserID: "user1",
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						User:   "cooluser",
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("3"),
						UserID: platform.ID("user1"),
						User:   "cooluser",
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
				},
			},
		},
		{
			name: "find authorization by token",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("0"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     platform.ID("3"),
						UserID: platform.ID("user1"),
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				token: "rand2",
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						User:   "regularuser",
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
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

			filter := platform.AuthorizationFilter{}
			if tt.args.ID != "" {
				id := platform.ID(tt.args.ID)
				filter.ID = &id
			}
			if tt.args.UserID != "" {
				id := platform.ID(tt.args.UserID)
				filter.UserID = &id
			}
			if tt.args.token != "" {
				filter.Token = &tt.args.token
			}

			authorizations, _, err := s.FindAuthorizations(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteAuthorization testing
func DeleteAuthorization(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err            error
		authorizations []*platform.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		args   args
		wants  wants
	}{
		{
			name: "delete authorizations using exist id",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: "1",
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						User:   "regularuser",
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
		},
		{
			name: "delete authorizations using id that does not exist",
			fields: AuthorizationFields{
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   platform.ID("user1"),
					},
					{
						Name: "regularuser",
						ID:   platform.ID("user2"),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: "123",
			},
			wants: wants{
				err: fmt.Errorf("authorization not found"),
				authorizations: []*platform.Authorization{
					{
						ID:     platform.ID("1"),
						UserID: platform.ID("user1"),
						User:   "cooluser",
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     platform.ID("2"),
						UserID: platform.ID("user2"),
						User:   "regularuser",
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
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
			err := s.DeleteAuthorization(ctx, platform.ID(tt.args.ID))
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.AuthorizationFilter{}
			authorizations, _, err := s.FindAuthorizations(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve authorizations: %v", err)
			}
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
