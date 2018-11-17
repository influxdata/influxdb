package testing

import (
	"bytes"
	"context"
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

// AuthorizationService tests all the service functions.
func AuthorizationService(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateAuthorization",
			fn:   CreateAuthorization,
		},
		{
			name: "FindAuthorizationByID",
			fn:   FindAuthorizationByID,
		},
		{
			name: "FindAuthorizationByToken",
			fn:   FindAuthorizationByToken,
		},
		{
			name: "FindAuthorizations",
			fn:   FindAuthorizations,
		},
		{
			name: "DeleteAuthorization",
			fn:   DeleteAuthorization,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateAuthorization testing
func CreateAuthorization(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		authorization *platform.Authorization
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
						ID:   MustIDBase16(userOneID),
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					ID:   MustIDBase16(authOneID),
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
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
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
					ID:   MustIDBase16(authTwoID),
					Permissions: []platform.Permission{
						platform.CreateUserPermission,
					},
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						User:   "cooluser",
						Status: platform.Active,
						Token:  "supersecret",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						User:   "regularuser",
						Token:  "rand",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
		},
		{
			name: "missing id authorization",
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
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
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						User:   "cooluser",
						Status: platform.Active,
						Token:  "supersecret",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						User:   "regularuser",
						Token:  "rand",
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.CreateAuthorization(ctx, tt.args.authorization)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			diffPlatformErrors(tt.name, tt.wants.err, err, opPrefix, t)

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			// if tt.args.authorization.ID != nil {
			defer s.DeleteAuthorization(ctx, tt.args.authorization.ID)
			// }

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
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:     MustIDBase16(authTwoID),
					UserID: MustIDBase16(userTwoID),
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizationByToken testing
func FindAuthorizationByToken(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Status: platform.Inactive,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authZeroID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand0",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authThreeID),
						UserID: MustIDBase16(userOneID),
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
					ID:     MustIDBase16(authOneID),
					UserID: MustIDBase16(userOneID),
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			authorization, err := s.FindAuthorizationByToken(ctx, tt.args.token)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindAuthorizations testing
func FindAuthorizations(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
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
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						User:   "cooluser",
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authThreeID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand3",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						User:   "cooluser",
						Status: platform.Active,
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authThreeID),
						UserID: MustIDBase16(userOneID),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authZeroID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authThreeID),
						UserID: MustIDBase16(userOneID),
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
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			filter := platform.AuthorizationFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.UserID.Valid() {
				filter.UserID = &tt.args.UserID
			}
			if tt.args.token != "" {
				filter.Token = &tt.args.token
			}

			authorizations, _, err := s.FindAuthorizations(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)
			if diff := cmp.Diff(authorizations, tt.wants.authorizations, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteAuthorization testing
func DeleteAuthorization(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(authOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
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
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						Token:  "rand1",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
						Token:  "rand2",
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
						},
					},
				},
			},
			args: args{
				ID: MustIDBase16(authThreeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Msg:  "authorization not found",
					Op:   platform.OpDeleteAuthorization,
				},
				authorizations: []*platform.Authorization{
					{
						ID:     MustIDBase16(authOneID),
						UserID: MustIDBase16(userOneID),
						User:   "cooluser",
						Token:  "rand1",
						Status: platform.Active,
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
						},
					},
					{
						ID:     MustIDBase16(authTwoID),
						UserID: MustIDBase16(userTwoID),
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
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteAuthorization(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

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
