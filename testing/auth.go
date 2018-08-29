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
	authZeroID  = "020f755c3c081000"
	authOneID   = "020f755c3c082000"
	authTwoID   = "020f755c3c082001"
	authThreeID = "020f755c3c082002"
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
				IDGenerator: mock.NewIDGenerator(authOneID, t),
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Authorizations: []*platform.Authorization{},
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   idFromString(t, userOneID),
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
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand",
						Status: platform.Active,
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
				IDGenerator: mock.NewIDGenerator(authTwoID, t),
				TokenGenerator: &mock.TokenGenerator{
					TokenFn: func() (string, error) {
						return "rand", nil
					},
				},
				Users: []*platform.User{
					{
						Name: "cooluser",
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
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
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						User:   "cooluser",
						Status: platform.Active,
						Token:  "supersecret",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Status: platform.Active,
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				id: idFromString(t, authTwoID),
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:     idFromString(t, authTwoID),
					UserID: idFromString(t, userTwoID),
					User:   "regularuser",
					Status: platform.Active,
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Status: platform.Inactive,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authZeroID),
						UserID: idFromString(t, userOneID),
						Token:  "rand0",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     idFromString(t, authThreeID),
						UserID: idFromString(t, userOneID),
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
					ID:     idFromString(t, authOneID),
					UserID: idFromString(t, userOneID),
					Status: platform.Inactive,
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
		ID     platform.ID
		UserID platform.ID
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Status: platform.Active,
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
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						User:   "cooluser",
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						User:   "regularuser",
						Token:  "rand2",
						Status: platform.Active,
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     idFromString(t, authThreeID),
						UserID: idFromString(t, userOneID),
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				UserID: idFromString(t, userOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						User:   "cooluser",
						Status: platform.Active,
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authThreeID),
						UserID: idFromString(t, userOneID),
						User:   "cooluser",
						Status: platform.Active,
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authZeroID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     idFromString(t, authThreeID),
						UserID: idFromString(t, userOneID),
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
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						User:   "regularuser",
						Token:  "rand2",
						Status: platform.Active,
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
			if tt.args.ID != nil {
				filter.ID = &tt.args.ID
			}
			if tt.args.UserID != nil {
				filter.UserID = &tt.args.UserID
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
		ID platform.ID
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: idFromString(t, authOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						User:   "regularuser",
						Status: platform.Active,
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
						ID:   idFromString(t, userOneID),
					},
					{
						Name: "regularuser",
						ID:   idFromString(t, userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: idFromString(t, authThreeID),
			},
			wants: wants{
				err: fmt.Errorf("authorization not found"),
				authorizations: []*platform.Authorization{
					{
						ID:     idFromString(t, authOneID),
						UserID: idFromString(t, userOneID),
						User:   "cooluser",
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     idFromString(t, authTwoID),
						UserID: idFromString(t, userTwoID),
						User:   "regularuser",
						Token:  "rand2",
						Status: platform.Active,
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
