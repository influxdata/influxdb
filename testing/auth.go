package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
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
	Orgs           []*platform.Organization
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
			name: "UpdateAuthorizationStatus",
			fn:   UpdateAuthorizationStatus,
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
				},
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					OrgID:       MustIDBase16(orgOneID),
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Description: "new auth",
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand",
						Status:      platform.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
						Description: "new auth",
					},
				},
			},
		},
		{
			name: "if auth ID supplied it is ignored",
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
				},
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					ID:          platform.ID(1), // Should be ignored.
					UserID:      MustIDBase16(userOneID),
					OrgID:       MustIDBase16(orgOneID),
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand",
						Status:      platform.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "providing a non existing user is invalid",
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
				},
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					OrgID:       MustIDBase16(orgOneID),
					UserID:      MustIDBase16(userTwoID),
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Description: "auth with non-existent user",
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
				err: platform.ErrUnableToCreateToken,
			},
		},
		{
			name: "providing a non existing org is invalid",
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
				},
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
			},
			args: args{
				authorization: &platform.Authorization{
					OrgID:       MustIDBase16(orgTwoID),
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(MustIDBase16(orgTwoID)),
					Description: "auth with non-existent org",
				},
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
				err: platform.ErrUnableToCreateToken,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateAuthorization(ctx, tt.args.authorization)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

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
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Status:      platform.Active,
					Token:       "rand2",
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
				t.Errorf("authorization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateAuthorizationStatus testing
func UpdateAuthorizationStatus(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id     platform.ID
		status platform.Status
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
			name: "regular update",
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      platform.Inactive,
						OrgID:       MustIDBase16(orgTwoID),
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				id:     MustIDBase16(authTwoID),
				status: platform.Inactive,
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Token:       "rand2",
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Status:      platform.Inactive,
				},
			},
		},
		{
			name: "update with id not found",
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      platform.Inactive,
						OrgID:       MustIDBase16(orgTwoID),
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				id:     MustIDBase16(authThreeID),
				status: platform.Inactive,
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpSetAuthorizationStatus,
					Msg:  "authorization not found",
				},
			},
		},
		{
			name: "update with unknown status",
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      platform.Inactive,
						OrgID:       MustIDBase16(orgTwoID),
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				id:     MustIDBase16(authTwoID),
				status: platform.Status("unknown"),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInvalid,
					Op:   platform.OpSetAuthorizationStatus,
					Msg:  "unknown authorization status",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			err := s.SetAuthorizationStatus(ctx, tt.args.id, tt.args.status)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if tt.wants.err == nil {
				authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
				if err != nil {
					t.Errorf("%s failed, got error %s", tt.name, err.Error())
				}
				if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      platform.Inactive,
						OrgID:       MustIDBase16(orgTwoID),
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				token: "rand1",
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:          MustIDBase16(authOneID),
					UserID:      MustIDBase16(userOneID),
					OrgID:       MustIDBase16(orgTwoID),
					Status:      platform.Inactive,
					Token:       "rand1",
					Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      platform.Active,
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      platform.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      platform.Active,
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand3",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				token: "rand2",
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      platform.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				ID: MustIDBase16(authOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
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
				Orgs: []*platform.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						OrgID:       MustIDBase16(orgOneID),
						UserID:      MustIDBase16(userTwoID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
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
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      platform.Active,
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      platform.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
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

func allUsersPermission(orgID platform.ID) []platform.Permission {
	return []platform.Permission{
		{Action: platform.WriteAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
		{Action: platform.ReadAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
	}
}

func createUsersPermission(orgID platform.ID) []platform.Permission {
	return []platform.Permission{
		{Action: platform.WriteAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
	}
}

func deleteUsersPermission(orgID platform.ID) []platform.Permission {
	return []platform.Permission{
		{Action: platform.WriteAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
	}
}
