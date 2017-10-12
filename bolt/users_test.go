package bolt_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
)

func TestUsersStore_Get(t *testing.T) {
	type args struct {
		ctx context.Context
		ID  string
	}
	tests := []struct {
		name    string
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "User not found",
			args: args{
				ctx: context.Background(),
				ID:  "1337",
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

		s := client.UsersStore
		got, err := s.Get(tt.args.ctx, tt.args.ID)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. UsersStore.Get() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestUsersStore_Add(t *testing.T) {
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name    string
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Add new user",
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:     "docbrown",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						chronograf.DefaultUserRoles[chronograf.EditorRole],
					},
				},
			},
			want: &chronograf.User{
				Name:     "docbrown",
				Provider: "GitHub",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					chronograf.DefaultUserRoles[chronograf.EditorRole],
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
		s := client.UsersStore
		got, err := s.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}

		got, err = s.Get(tt.args.ctx, fmt.Sprintf("%d", got.ID))
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		usersEqual(t, tt.name, "Add", got, tt.want)
	}
}

func TestUsersStore_Delete(t *testing.T) {
	type args struct {
		ctx  context.Context
		user *chronograf.User
	}
	tests := []struct {
		name     string
		args     args
		addFirst bool
		wantErr  bool
	}{
		{
			name: "No such user",
			args: args{
				ctx: context.Background(),
				user: &chronograf.User{
					ID: 10,
				},
			},
			wantErr: true,
		},
		{
			name: "Delete new user",
			args: args{
				ctx: context.Background(),
				user: &chronograf.User{
					Name: "noone",
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
		s := client.UsersStore

		if tt.addFirst {
			tt.args.user, _ = s.Add(tt.args.ctx, tt.args.user)
		}
		if err := s.Delete(tt.args.ctx, tt.args.user); (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestUsersStore_Update(t *testing.T) {
	type args struct {
		ctx      context.Context
		usr      *chronograf.User
		roles    []chronograf.Role
		provider string
		scheme   string
		// TODO: add name
	}
	tests := []struct {
		name     string
		args     args
		addFirst bool
		want     *chronograf.User
		wantErr  bool
	}{
		{
			name: "No such user",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					ID: 10,
				},
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
						chronograf.DefaultUserRoles[chronograf.ViewerRole],
					},
				},
				roles: []chronograf.Role{
					chronograf.DefaultUserRoles[chronograf.EditorRole],
				},
			},
			want: &chronograf.User{
				Name:     "bobetta",
				Provider: "GitHub",
				Scheme:   "OAuth2",
				Roles: []chronograf.Role{
					chronograf.DefaultUserRoles[chronograf.EditorRole],
				},
			},
			addFirst: true,
		},
		{
			name: "Update user provider and scheme",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "bobetta",
					Provider: "GitHub",
					Scheme:   "OAuth2",
				},
				provider: "Google",
				scheme:   "LDAP",
			},
			want: &chronograf.User{
				Name:     "bobetta",
				Provider: "Google",
				Scheme:   "LDAP",
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
		s := client.UsersStore

		if tt.addFirst {
			tt.args.usr, err = s.Add(tt.args.ctx, tt.args.usr)
			if err != nil {
				t.Fatal(err)
			}
		}

		if tt.args.roles != nil {
			tt.args.usr.Roles = tt.args.roles
		}

		if tt.args.provider != "" {
			tt.args.usr.Provider = tt.args.provider
		}

		if tt.args.scheme != "" {
			tt.args.usr.Scheme = tt.args.scheme
		}

		if err := s.Update(tt.args.ctx, tt.args.usr); (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		// for the empty test
		if tt.want == nil {
			continue
		}

		got, err := s.Get(tt.args.ctx, fmt.Sprintf("%d", tt.args.usr.ID))
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		usersEqual(t, tt.name, "Update", got, tt.want)
	}
}

func TestUsersStore_All(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		want     []chronograf.User
		addFirst bool
		wantErr  bool
	}{
		{
			name: "No users",
		},
		{
			name: "Update new user",
			want: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						chronograf.DefaultUserRoles[chronograf.ViewerRole],
					},
				},
				{
					Name:     "doody",
					Provider: "GitHub",
					Scheme:   "OAuth2",
					Roles: []chronograf.Role{
						chronograf.DefaultUserRoles[chronograf.EditorRole],
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
		s := client.UsersStore

		if tt.addFirst {
			for _, u := range tt.want {
				s.Add(tt.ctx, &u)
			}
		}
		gots, err := s.All(tt.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			usersEqual(t, tt.name, "All", &got, &tt.want[i])
		}
	}
}

func usersEqual(t *testing.T, name, method string, u1, u2 *chronograf.User) {
	if u1.Name != u2.Name {
		t.Errorf("%q. UsersStore.%s() .Name:\ngot %v, want %v", name, method, u1.Name, u2.Name)
	}
	if u1.Provider != u2.Provider {
		t.Errorf("%q. UsersStore.%s() .Provider:\ngot %v, want %v", name, method, u1.Provider, u2.Provider)
	}
	if u1.Scheme != u2.Scheme {
		t.Errorf("%q. UsersStore.%s() .Scheme:\ngot %v, want %v", name, method, u1.Scheme, u2.Scheme)
	}
	if len(u1.Roles) != len(u2.Roles) {
		t.Errorf("%q. UsersStore.%s() .Roles:\ngot %v, want %v", name, method, u1.Roles, u2.Roles)
		return
	}
	if len(u1.Roles) > 0 && len(u2.Roles) > 0 {
		for i, gotRole := range u1.Roles {
			wantRole := u2.Roles[i]
			if wantRole.Name != gotRole.Name {
				t.Errorf("%q. UsersStore.%s() .Roles[%d]:\ngot %v, want %v", name, method, i, gotRole, wantRole)
			}
		}
	}
}
