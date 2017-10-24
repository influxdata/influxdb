package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
)

func TestOrganizationUsersStore_Get(t *testing.T) {
	type args struct {
		ctx   context.Context
		usr   *chronograf.User
		orgID string
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.User
		wantErr  bool
		addFirst bool
	}{
		{
			name: "Get user with no role in organization",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "Google",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "The HillBilliettas",
						},
					},
				},
				orgID: "1336",
			},
			wantErr:  true,
			addFirst: true,
		},
		{
			name: "Get user no organization set",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "Google",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "The HillBilliettas",
						},
					},
				},
			},
			wantErr:  true,
			addFirst: true,
		},
		{
			name: "Get user scoped to an organization",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "Google",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "The HillBilliettas",
						},
						{
							OrganizationID: "1336",
							Name:           "The BillHilliettos",
						},
					},
				},
				orgID: "1336",
			},
			want: &chronograf.User{
				Name:     "billietta",
				Provider: "Google",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					{
						OrganizationID: "1336",
						Name:           "The BillHilliettos",
					},
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.orgID)
		if tt.addFirst {
			tt.args.usr, err = client.UsersStore.Add(tt.args.ctx, tt.args.usr)
			if err != nil {
				t.Fatal(err)
			}
		}
		s := client.OrganizationUsersStore
		got, err := s.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.usr.ID})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationUsersStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationUsersStore_Add(t *testing.T) {
	type args struct {
		ctx   context.Context
		u     *chronograf.User
		orgID string
	}
	tests := []struct {
		name    string
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Add new user - no org",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "docbrown",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1336",
							Name:           "Editor",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Add new user",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "docbrown",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1336",
							Name:           "Editor",
						},
					},
				},
				orgID: "1336",
			},
			want: &chronograf.User{
				Name:     "docbrown",
				Provider: "GitHub",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					{
						OrganizationID: "1336",
						Name:           "Editor",
					},
				},
			},
		},
		{
			name: "Has invalid Role: missing OrganizationID",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							Name: "Editor",
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Has invalid Role: missing Name",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "henrietta",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1337",
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.orgID)
		s := client.OrganizationUsersStore
		got, err := s.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationUsersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got == nil && tt.want == nil {
			continue
		}
		got, err = client.UsersStore.Get(tt.args.ctx, chronograf.UserQuery{ID: &got.ID})
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationUsersStore_Delete(t *testing.T) {
	type args struct {
		ctx   context.Context
		user  *chronograf.User
		orgID string
	}
	tests := []struct {
		name        string
		args        args
		addFirst    bool
		wantErr     bool
		wantRawUser *chronograf.User
	}{
		{
			name: "No such user",
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
			name: "Delete new user",
			args: args{
				ctx: context.Background(),
				user: &chronograf.User{
					Name: "noone",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "The BillHilliettas",
						},
						{
							OrganizationID: "1336",
							Name:           "The HillBilliettas",
						},
					},
				},
				orgID: "1336",
			},
			addFirst: true,
			wantRawUser: &chronograf.User{
				Name: "noone",
				Roles: []chronograf.Role{
					{
						OrganizationID: "1338",
						Name:           "The BillHilliettas",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.orgID)
		if tt.addFirst {
			tt.args.user, _ = client.UsersStore.Add(tt.args.ctx, tt.args.user)
		}
		s := client.OrganizationUsersStore
		if err := s.Delete(tt.args.ctx, tt.args.user); (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationUsersStore.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if u, err := s.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.user.ID}); err == nil {
			t.Errorf("%q. Expected error retrieving deleted user, got user %v", tt.name, u)
		}
		rawUser, _ := client.UsersStore.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.user.ID})
		if diff := cmp.Diff(rawUser, tt.wantRawUser, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.Delete():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationUsersStore_Update(t *testing.T) {
	type args struct {
		ctx   context.Context
		usr   *chronograf.User
		roles []chronograf.Role
		orgID string
	}
	tests := []struct {
		name     string
		args     args
		addFirst bool
		want     *chronograf.User
		wantRaw  *chronograf.User
		wantErr  bool
	}{
		{
			name: "No such user",
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
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "bobetta",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Viewer",
						},
						{
							OrganizationID: "1337",
							Name:           "Viewer",
						},
					},
				},
				roles: []chronograf.Role{
					{
						OrganizationID: "1338",
						Name:           "Editor",
					},
				},
				orgID: "1338",
			},
			want: &chronograf.User{
				Name:     "bobetta",
				Provider: "GitHub",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					{
						OrganizationID: "1338",
						Name:           "Editor",
					},
				},
			},
			wantRaw: &chronograf.User{
				Name:     "bobetta",
				Provider: "GitHub",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					{
						OrganizationID: "1337",
						Name:           "Viewer",
					},
					{
						OrganizationID: "1338",
						Name:           "Editor",
					},
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.orgID)
		if tt.addFirst {
			tt.args.usr, err = client.UsersStore.Add(tt.args.ctx, tt.args.usr)
			if err != nil {
				t.Fatal(err)
			}
		}
		s := client.OrganizationUsersStore

		if tt.args.roles != nil {
			tt.args.usr.Roles = tt.args.roles
		}

		if err := s.Update(tt.args.ctx, tt.args.usr); (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationUsersStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		// for the empty test
		if tt.want == nil {
			continue
		}

		got, err := s.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.usr.ID})
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
		gotRaw, err := client.UsersStore.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.usr.ID})
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		if diff := cmp.Diff(gotRaw, tt.wantRaw, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationUsersStore_All(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		want     []chronograf.User
		wantRaw  []chronograf.User
		orgID    string
		addFirst bool
		wantErr  bool
	}{
		{
			name: "No users",
			ctx:  context.Background(),
			wantRaw: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Viewer",
						},
						{
							OrganizationID: "1336",
							Name:           "Viewer",
						},
					},
				},
				{
					Name:     "doody2",
					Provider: "GitHub2",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1337",
							Name:           "Editor",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Editor",
						},
					},
				},
			},
			orgID: "2330",
		},
		{
			name:  "get all users",
			orgID: "1338",
			ctx:   context.Background(),
			want: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Viewer",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Editor",
						},
					},
				},
			},
			wantRaw: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Viewer",
						},
						{
							OrganizationID: "1336",
							Name:           "Viewer",
						},
					},
				},
				{
					Name:     "doody2",
					Provider: "GitHub2",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1337",
							Name:           "Editor",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						{
							OrganizationID: "1338",
							Name:           "Editor",
						},
					},
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Open(context.TODO()); err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		tt.ctx = context.WithValue(tt.ctx, "organizationID", tt.orgID)
		if tt.addFirst {
			for _, u := range tt.wantRaw {
				client.UsersStore.Add(tt.ctx, &u)
			}
		}
		s := client.OrganizationUsersStore
		gots, err := s.All(tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationUsersStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(gots, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationUsersStore.All():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
