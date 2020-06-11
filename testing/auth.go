package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	authZeroID  = "020f755c3c081000"
	authOneID   = "020f755c3c082000"
	authTwoID   = "020f755c3c082001"
	authThreeID = "020f755c3c082002"
)

var authorizationCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(influxdb.Authorization{}, "ID", "Token", "CreatedAt", "UpdatedAt"),
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

type AuthTestOpts struct {
	WithoutFindByToken bool
}

// WithoutFindByToken allows the Find By Token test case to be skipped when we are testing the http server,
// since finding by token is not supported by the HTTP API
func WithoutFindByToken() AuthTestOpts {
	return AuthTestOpts{
		WithoutFindByToken: true,
	}
}

// AuthorizationFields will include the IDGenerator, and authorizations
type AuthorizationFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	TimeGenerator  platform.TimeGenerator
	Authorizations []*platform.Authorization
	Users          []*platform.User
	Orgs           []*platform.Organization
}

// AuthorizationService tests all the service functions.
func AuthorizationService(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
	t *testing.T,
	opts ...AuthTestOpts) {
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
			name: "UpdateAuthorization",
			fn:   UpdateAuthorization,
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
		if tt.name == "FindAuthorizationByToken" && len(opts) > 0 && opts[0].WithoutFindByToken {
			continue
		}
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
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						CRUDLog: platform.CRUDLog{
							CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
							UpdatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "providing a non existing user is invalid",
			fields: AuthorizationFields{
				IDGenerator: mock.NewIDGenerator(authTwoID, t),
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
				},
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
	type wants struct {
		err            error
		authorizations []*platform.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      "active",
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      "active",
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

			for i := range tt.fields.Authorizations {
				authorization, err := s.FindAuthorizationByID(ctx, tt.fields.Authorizations[i].ID)
				diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

				if diff := cmp.Diff(authorization, tt.wants.authorizations[i], authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
			}

		})
	}
}

func stringPtr(s string) *string {
	return &s
}

// UpdateAuthorization testing
func UpdateAuthorization(
	init func(AuthorizationFields, *testing.T) (platform.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id  platform.ID
		upd *platform.AuthorizationUpdate
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
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
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
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
				upd: &platform.AuthorizationUpdate{
					Status:      platform.Inactive.Ptr(),
					Description: stringPtr("desc1"),
				},
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Token:       "rand2",
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Status:      platform.Inactive,
					Description: "desc1",
					CRUDLog: platform.CRUDLog{
						UpdatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					},
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
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
				id: MustIDBase16(authThreeID),
				upd: &platform.AuthorizationUpdate{
					Status: platform.Inactive.Ptr(),
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateAuthorization,
					Msg:  "authorization not found",
				},
			},
		},
		{
			name: "update with unknown status",
			fields: AuthorizationFields{
				TimeGenerator: &mock.TimeGenerator{
					FakeValue: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
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
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
					},
				},
			},
			args: args{
				id: MustIDBase16(authTwoID),
				upd: &platform.AuthorizationUpdate{
					Status: platform.Status("unknown").Ptr(),
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInvalid,
					Op:   platform.OpUpdateAuthorization,
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

			updatedAuth, err := s.UpdateAuthorization(ctx, tt.args.id, tt.args.upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if tt.wants.err == nil {
				authorization, err := s.FindAuthorizationByID(ctx, tt.args.id)
				if err != nil {
					t.Errorf("%s failed, got error %s", tt.name, err.Error())
				}
				if diff := cmp.Diff(authorization, tt.wants.authorization, authorizationCmpOptions...); diff != "" {
					t.Errorf("authorization is different -got/+want\ndiff %s", diff)
				}
				if diff := cmp.Diff(authorization, updatedAuth, authorizationCmpOptions...); diff != "" {
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
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authZeroID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand0",
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userTwoID)),
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
					Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
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
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand4",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
			args: args{
				token: "rand2",
			},
			wants: wants{
				authorization: &platform.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Token:       "rand2",
					Status:      platform.Active,
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
		OrgID  platform.ID
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
			name: "find authorization by org id",
			fields: AuthorizationFields{
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
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      platform.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
				},
			},
			args: args{
				OrgID: MustIDBase16(orgOneID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "find authorization by org id and user id",
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
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      platform.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      platform.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userTwoID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      platform.Active,
						Token:       "rand4",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userTwoID)),
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
				OrgID:  MustIDBase16(orgTwoID),
			},
			wants: wants{
				authorizations: []*platform.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      platform.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID), MustIDBase16(userOneID)),
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
			if tt.args.OrgID.Valid() {
				filter.OrgID = &tt.args.OrgID
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID), MustIDBase16(userOneID)),
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

func allUsersPermission(orgID platform.ID, userID platform.ID) []platform.Permission {
	return []platform.Permission{
		{Action: platform.WriteAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
		{Action: platform.ReadAction, Resource: platform.Resource{Type: platform.UsersResourceType, OrgID: &orgID}},
		{Action: platform.WriteAction, Resource: platform.Resource{Type: platform.UsersResourceType, ID: &userID}},
		{Action: platform.ReadAction, Resource: platform.Resource{Type: platform.UsersResourceType, ID: &userID}},
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
