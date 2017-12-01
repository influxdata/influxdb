package organizations_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/organizations"
)

// IgnoreFields is used because ID cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var userCmpOptions = cmp.Options{
	cmpopts.IgnoreFields(chronograf.User{}, "ID"),
	cmpopts.EquateEmpty(),
}

func TestUsersStore_Get(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	type args struct {
		ctx    context.Context
		usr    *chronograf.User
		userID uint64
		orgID  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Get user with no role in organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1234,
							Name:     "billietta",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Organization: "1338",
									Name:         "The HillBilliettas",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx:    context.Background(),
				userID: 1234,
				orgID:  "1336",
			},
			wantErr: true,
		},
		{
			name: "Get user no organization set",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1234,
							Name:     "billietta",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Organization: "1338",
									Name:         "The HillBilliettas",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				userID: 1234,
				ctx:    context.Background(),
			},
			wantErr: true,
		},
		{
			name: "Get user scoped to an organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1234,
							Name:     "billietta",
							Provider: "google",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Organization: "1338",
									Name:         "The HillBilliettas",
								},
								{
									Organization: "1336",
									Name:         "The BillHilliettos",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx:    context.Background(),
				userID: 1234,
				orgID:  "1336",
			},
			want: &chronograf.User{
				Name:     "billietta",
				Provider: "google",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Organization: "1336",
						Name:         "The BillHilliettos",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.args.orgID)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.orgID)
		got, err := s.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.userID})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, userCmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestUsersStore_Add(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	type args struct {
		ctx      context.Context
		u        *chronograf.User
		orgID    string
		uInitial *chronograf.User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Add new user - no org",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					ID:       1234,
					Name:     "docbrown",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1336",
							Name:         "editor",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Add new user",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, chronograf.ErrUserNotFound
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					ID:       1234,
					Name:     "docbrown",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1336",
							Name:         "editor",
						},
					},
				},
				orgID: "1336",
			},
			want: &chronograf.User{
				ID:       1234,
				Name:     "docbrown",
				Provider: "github",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Organization: "1336",
						Name:         "editor",
					},
				},
			},
		},
		{
			name: "Add non-new user without Role",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1234,
							Name:     "docbrown",
							Provider: "github",
							Scheme:   "oauth2",
							Roles:    []chronograf.Role{},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					ID:       1234,
					Name:     "docbrown",
					Provider: "github",
					Scheme:   "oauth2",
					Roles:    []chronograf.Role{},
				},
				orgID: "1336",
			},
			want: &chronograf.User{
				Name:     "docbrown",
				Provider: "github",
				Scheme:   "oauth2",
				Roles:    []chronograf.Role{},
			},
		},
		{
			name: "Add non-new user with Role",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:       1234,
							Name:     "docbrown",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Organization: "1337",
									Name:         "editor",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					ID:       1234,
					Name:     "docbrown",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1336",
							Name:         "admin",
						},
					},
				},
				orgID: "1336",
			},
			want: &chronograf.User{
				Name:     "docbrown",
				Provider: "github",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Organization: "1337",
						Name:         "editor",
					},
					{
						Organization: "1336",
						Name:         "admin",
					},
				},
			},
		},
		{
			name: "Has invalid Role: missing Organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, nil
					},
				},
			},
			args: args{
				ctx:   context.Background(),
				orgID: "1338",
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "editor",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Has invalid Role: missing Name",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
						return u, nil
					},
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, nil
					},
				},
			},
			args: args{
				ctx:   context.Background(),
				orgID: "1337",
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1337",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Has invalid Organization",
			fields: fields{
				UsersStore: &mocks.UsersStore{},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						chronograf.Role{},
					},
				},
				orgID: "1337",
			},
			wantErr: true,
		},
		{
			name: "Organization does not match orgID",
			fields: fields{
				UsersStore: &mocks.UsersStore{},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1338",
							Name:         "editor",
						},
					},
				},
				orgID: "1337",
			},
			wantErr: true,
		},
		{
			name: "Role Name not specified",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1337",
						},
					},
				},
				orgID: "1337",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.orgID)
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.args.orgID)

		got, err := s.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got == nil && tt.want == nil {
			continue
		}
	}
}

func TestUsersStore_Delete(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	type args struct {
		ctx   context.Context
		user  *chronograf.User
		orgID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		wantRaw *chronograf.User
	}{
		{
			name: "No such user",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					//AddF: func(ctx context.Context, u *chronograf.User) (*chronograf.User, error) {
					//	return u, nil
					//},
					//UpdateF: func(ctx context.Context, u *chronograf.User) error {
					//	return nil
					//},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, chronograf.ErrUserNotFound
					},
				},
			},
			args: args{
				ctx: context.Background(),
				user: &chronograf.User{
					ID: 10,
				},
				orgID: "1336",
			},
			wantErr: true,
		},
		{
			name: "Derlete user",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							ID:   1234,
							Name: "noone",
							Roles: []chronograf.Role{
								{
									Organization: "1338",
									Name:         "The BillHilliettas",
								},
								{
									Organization: "1336",
									Name:         "The HillBilliettas",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				user: &chronograf.User{
					ID:   1234,
					Name: "noone",
					Roles: []chronograf.Role{
						{
							Organization: "1338",
							Name:         "The BillHilliettas",
						},
						{
							Organization: "1336",
							Name:         "The HillBilliettas",
						},
					},
				},
				orgID: "1336",
			},
		},
	}
	for _, tt := range tests {
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.orgID)
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.args.orgID)
		if err := s.Delete(tt.args.ctx, tt.args.user); (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestUsersStore_Update(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	type args struct {
		ctx        context.Context
		usr        *chronograf.User
		roles      []chronograf.Role
		superAdmin bool
		orgID      string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.User
		wantRaw *chronograf.User
		wantErr bool
	}{
		{
			name: "No such user",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return nil, chronograf.ErrUserNotFound
					},
				},
			},
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					ID: 10,
				},
				orgID: "1338",
			},
			wantErr: true,
		},
		{
			name: "Update user role",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							Name:     "bobetta",
							Provider: "github",
							Scheme:   "oauth2",
							Roles: []chronograf.Role{
								{
									Organization: "1337",
									Name:         "viewer",
								},
								{
									Organization: "1338",
									Name:         "editor",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "bobetta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles:    []chronograf.Role{},
				},
				roles: []chronograf.Role{
					{
						Organization: "1338",
						Name:         "editor",
					},
				},
				orgID: "1338",
			},
			want: &chronograf.User{
				Name:     "bobetta",
				Provider: "github",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Organization: "1338",
						Name:         "editor",
					},
				},
			},
		},
		{
			name: "Update user super admin",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					UpdateF: func(ctx context.Context, u *chronograf.User) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.UserQuery) (*chronograf.User, error) {
						return &chronograf.User{
							Name:       "bobetta",
							Provider:   "github",
							Scheme:     "oauth2",
							SuperAdmin: false,
							Roles: []chronograf.Role{
								{
									Organization: "1337",
									Name:         "viewer",
								},
								{
									Organization: "1338",
									Name:         "editor",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "bobetta",
					Provider: "github",
					Scheme:   "oauth2",
					Roles:    []chronograf.Role{},
				},
				superAdmin: true,
				orgID:      "1338",
			},
			want: &chronograf.User{
				Name:       "bobetta",
				Provider:   "github",
				Scheme:     "oauth2",
				SuperAdmin: true,
			},
		},
	}
	for _, tt := range tests {
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.orgID)
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.args.orgID)

		if tt.args.roles != nil {
			tt.args.usr.Roles = tt.args.roles
		}

		if tt.args.superAdmin {
			tt.args.usr.SuperAdmin = tt.args.superAdmin
		}

		if err := s.Update(tt.args.ctx, tt.args.usr); (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		// for the empty test
		if tt.want == nil {
			continue
		}

		if diff := cmp.Diff(tt.args.usr, tt.want, userCmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}

	}
}

func TestUsersStore_All(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	tests := []struct {
		name    string
		fields  fields
		ctx     context.Context
		want    []chronograf.User
		wantRaw []chronograf.User
		orgID   string
		wantErr bool
	}{
		{
			name: "No users",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "howdy",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "viewer",
									},
									{
										Organization: "1336",
										Name:         "viewer",
									},
								},
							},
							{
								Name:     "doody2",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1337",
										Name:         "editor",
									},
								},
							},
							{
								Name:     "doody",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "editor",
									},
								},
							},
						}, nil
					},
				},
			},
			ctx:   context.Background(),
			orgID: "2330",
		},
		{
			name:  "get all users",
			orgID: "1338",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "howdy",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "viewer",
									},
									{
										Organization: "1336",
										Name:         "viewer",
									},
								},
							},
							{
								Name:     "doody2",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1337",
										Name:         "editor",
									},
								},
							},
							{
								Name:     "doody",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "editor",
									},
								},
							},
						}, nil
					},
				},
			},
			ctx: context.Background(),
			want: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1338",
							Name:         "viewer",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Organization: "1338",
							Name:         "editor",
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		tt.ctx = context.WithValue(tt.ctx, organizations.ContextKey, tt.orgID)
		for _, u := range tt.wantRaw {
			tt.fields.UsersStore.Add(tt.ctx, &u)
		}
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.orgID)
		gots, err := s.All(tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(gots, tt.want, userCmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.All():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestUsersStore_Num(t *testing.T) {
	type fields struct {
		UsersStore chronograf.UsersStore
	}
	tests := []struct {
		name    string
		fields  fields
		ctx     context.Context
		orgID   string
		want    int
		wantErr bool
	}{
		{
			name: "No users",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "howdy",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "viewer",
									},
									{
										Organization: "1336",
										Name:         "viewer",
									},
								},
							},
							{
								Name:     "doody2",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1337",
										Name:         "editor",
									},
								},
							},
							{
								Name:     "doody",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "editor",
									},
								},
							},
						}, nil
					},
				},
			},
			ctx:   context.Background(),
			orgID: "2330",
		},
		{
			name:  "get all users",
			orgID: "1338",
			fields: fields{
				UsersStore: &mocks.UsersStore{
					AllF: func(ctx context.Context) ([]chronograf.User, error) {
						return []chronograf.User{
							{
								Name:     "howdy",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "viewer",
									},
									{
										Organization: "1336",
										Name:         "viewer",
									},
								},
							},
							{
								Name:     "doody2",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1337",
										Name:         "editor",
									},
								},
							},
							{
								Name:     "doody",
								Provider: "github",
								Scheme:   "oauth2",
								Roles: []chronograf.Role{
									{
										Organization: "1338",
										Name:         "editor",
									},
								},
							},
						}, nil
					},
				},
			},
			ctx:  context.Background(),
			want: 2,
		},
	}
	for _, tt := range tests {
		tt.ctx = context.WithValue(tt.ctx, organizations.ContextKey, tt.orgID)
		s := organizations.NewUsersStore(tt.fields.UsersStore, tt.orgID)
		got, err := s.Num(tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Num() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != tt.want {
			t.Errorf("%q. UsersStore.Num() = %d. want %d", tt.name, got, tt.want)
		}
	}
}
