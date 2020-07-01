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
	cmp.Transformer("Sort", func(in []*influxdb.Authorization) []*influxdb.Authorization {
		out := append([]*influxdb.Authorization(nil), in...) // Copy input to avoid mutating it
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
	IDGenerator    influxdb.IDGenerator
	TokenGenerator influxdb.TokenGenerator
	TimeGenerator  influxdb.TimeGenerator
	Authorizations []*influxdb.Authorization
	Users          []*influxdb.User
	Orgs           []*influxdb.Organization
}

// AuthorizationService tests all the service functions.
func AuthorizationService(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
	opts ...AuthTestOpts) {
	tests := []struct {
		name string
		fn   func(init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
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
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// CreateAuthorization testing
func CreateAuthorization(
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		authorization *influxdb.Authorization
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				authorization: &influxdb.Authorization{
					OrgID:       MustIDBase16(orgOneID),
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Description: "new auth",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand",
						Status:      influxdb.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
						Description: "new auth",
						CRUDLog: influxdb.CRUDLog{
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				authorization: &influxdb.Authorization{
					OrgID:       MustIDBase16(orgOneID),
					UserID:      MustIDBase16(userTwoID),
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Description: "auth with non-existent user",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
				err: influxdb.ErrUnableToCreateToken,
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				authorization: &influxdb.Authorization{
					OrgID:       MustIDBase16(orgTwoID),
					UserID:      MustIDBase16(userOneID),
					Permissions: createUsersPermission(MustIDBase16(orgTwoID)),
					Description: "auth with non-existent org",
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "supersecret",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
						Description: "already existing auth",
					},
				},
				err: influxdb.ErrUnableToCreateToken,
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

			authorizations, _, err := s.FindAuthorizations(ctx, influxdb.AuthorizationFilter{})
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
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
	}

	tests := []struct {
		name   string
		fields AuthorizationFields
		wants  wants
	}{
		{
			name: "basic find authorization by id",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      "active",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
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
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id  influxdb.ID
		upd *influxdb.AuthorizationUpdate
	}
	type wants struct {
		err           error
		authorization *influxdb.Authorization
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
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
				id: MustIDBase16(authTwoID),
				upd: &influxdb.AuthorizationUpdate{
					Status:      influxdb.Inactive.Ptr(),
					Description: stringPtr("desc1"),
				},
			},
			wants: wants{
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Token:       "rand2",
					Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					Status:      influxdb.Inactive,
					Description: "desc1",
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update with id not found",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
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
				id: MustIDBase16(authThreeID),
				upd: &influxdb.AuthorizationUpdate{
					Status: influxdb.Inactive.Ptr(),
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpUpdateAuthorization,
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
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
				id: MustIDBase16(authTwoID),
				upd: &influxdb.AuthorizationUpdate{
					Status: influxdb.Status("unknown").Ptr(),
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Op:   influxdb.OpUpdateAuthorization,
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
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		token string
	}
	type wants struct {
		err           error
		authorization *influxdb.Authorization
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Inactive,
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
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authOneID),
					UserID:      MustIDBase16(userOneID),
					OrgID:       MustIDBase16(orgTwoID),
					Status:      influxdb.Inactive,
					Token:       "rand1",
					Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
				},
			},
		},
		{
			name: "find authorization by token",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
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
				authorization: &influxdb.Authorization{
					ID:          MustIDBase16(authTwoID),
					UserID:      MustIDBase16(userTwoID),
					OrgID:       MustIDBase16(orgOneID),
					Token:       "rand2",
					Status:      influxdb.Active,
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
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID     influxdb.ID
		UserID influxdb.ID
		OrgID  influxdb.ID
		token  string
	}

	type wants struct {
		authorizations []*influxdb.Authorization
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      influxdb.Active,
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      influxdb.Active,
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "find authorization by user id",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand1",
						Status:      influxdb.Active,
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
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "find authorization by org id",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
				},
			},
			args: args{
				OrgID: MustIDBase16(orgOneID),
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: deleteUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "find authorization by org id and user id",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "o2",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand1",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand3",
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authThreeID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      influxdb.Active,
						Token:       "rand4",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
					},
				},
			},
			args: args{
				UserID: MustIDBase16(userOneID),
				OrgID:  MustIDBase16(orgTwoID),
			},
			wants: wants{
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userOneID),
						OrgID:       MustIDBase16(orgTwoID),
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: allUsersPermission(MustIDBase16(orgTwoID)),
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

			filter := influxdb.AuthorizationFilter{}
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
	init func(AuthorizationFields, *testing.T) (influxdb.AuthorizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID influxdb.ID
	}
	type wants struct {
		err            error
		authorizations []*influxdb.Authorization
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
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Status:      influxdb.Active,
						Token:       "rand2",
						Permissions: createUsersPermission(MustIDBase16(orgOneID)),
					},
				},
			},
		},
		{
			name: "delete authorizations using id that does not exist",
			fields: AuthorizationFields{
				Users: []*influxdb.User{
					{
						Name: "cooluser",
						ID:   MustIDBase16(userOneID),
					},
					{
						Name: "regularuser",
						ID:   MustIDBase16(userTwoID),
					},
				},
				Orgs: []*influxdb.Organization{
					{
						Name: "o1",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Authorizations: []*influxdb.Authorization{
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
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "authorization not found",
					Op:   influxdb.OpDeleteAuthorization,
				},
				authorizations: []*influxdb.Authorization{
					{
						ID:          MustIDBase16(authOneID),
						UserID:      MustIDBase16(userOneID),
						Token:       "rand1",
						Status:      influxdb.Active,
						OrgID:       MustIDBase16(orgOneID),
						Permissions: allUsersPermission(MustIDBase16(orgOneID)),
					},
					{
						ID:          MustIDBase16(authTwoID),
						UserID:      MustIDBase16(userTwoID),
						OrgID:       MustIDBase16(orgOneID),
						Token:       "rand2",
						Status:      influxdb.Active,
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

			filter := influxdb.AuthorizationFilter{}
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

func allUsersPermission(orgID influxdb.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
		{Action: influxdb.ReadAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}

func createUsersPermission(orgID influxdb.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}

func deleteUsersPermission(orgID influxdb.ID) []influxdb.Permission {
	return []influxdb.Permission{
		{Action: influxdb.WriteAction, Resource: influxdb.Resource{Type: influxdb.UsersResourceType, OrgID: &orgID}},
	}
}
