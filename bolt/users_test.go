package bolt_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
)

// IgnoreFields is used because ID is created by BoltDB and cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var cmpOptions = cmp.Options{
	cmpopts.IgnoreFields(chronograf.User{}, "ID"),
	cmpopts.EquateEmpty(),
}

func TestUsersStore_GetWithID(t *testing.T) {
	type args struct {
		ctx context.Context
		usr *chronograf.User
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.User
		wantErr  bool
		addFirst bool
	}{
		{
			name: "User not found",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					ID: 1337,
				},
			},
			wantErr: true,
		},
		{
			name: "Get user",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "google",
					Scheme:   "oauth2",
				},
			},
			want: &chronograf.User{
				Name:     "billietta",
				Provider: "google",
				Scheme:   "oauth2",
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
		got, err := s.Get(tt.args.ctx, chronograf.UserQuery{ID: &tt.args.usr.ID})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestUsersStore_GetWithNameProviderScheme(t *testing.T) {
	type args struct {
		ctx      context.Context
		name     string
		provider string
		usr      *chronograf.User
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.User
		wantErr  bool
		addFirst bool
	}{
		{
			name: "User not found",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "google",
					Scheme:   "oauth2",
				},
			},
			wantErr: true,
		},
		{
			name: "Get user",
			args: args{
				ctx: context.Background(),
				usr: &chronograf.User{
					Name:     "billietta",
					Provider: "google",
					Scheme:   "oauth2",
				},
			},
			want: &chronograf.User{
				Name:     "billietta",
				Provider: "google",
				Scheme:   "oauth2",
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

		got, err := s.Get(tt.args.ctx, chronograf.UserQuery{
			Name:     &tt.args.usr.Name,
			Provider: &tt.args.usr.Provider,
			Scheme:   &tt.args.usr.Scheme,
		})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestUsersStore_GetInvalid(t *testing.T) {
	client, err := NewTestClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Open(context.TODO()); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	s := client.UsersStore

	_, err = s.Get(context.Background(), chronograf.UserQuery{})
	if err == nil {
		t.Errorf("Invalid Get. UsersStore.Get() error = %v", err)
	}
}

func TestUsersStore_Add(t *testing.T) {
	type args struct {
		ctx      context.Context
		u        *chronograf.User
		addFirst bool
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
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "editor",
						},
					},
				},
			},
			want: &chronograf.User{
				Name:     "docbrown",
				Provider: "github",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Name: "editor",
					},
				},
			},
		},
		{
			name: "User already exists",
			args: args{
				ctx:      context.Background(),
				addFirst: true,
				u: &chronograf.User{
					Name:     "docbrown",
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
		if tt.args.addFirst {
			_, _ = s.Add(tt.args.ctx, tt.args.u)
		}
		got, err := s.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}

		if tt.wantErr {
			continue
		}

		got, err = s.Get(tt.args.ctx, chronograf.UserQuery{ID: &got.ID})
		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}
		if diff := cmp.Diff(got, tt.want, cmpOptions...); diff != "" {
			t.Errorf("%q. UsersStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
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
			var err error
			tt.args.user, err = s.Add(tt.args.ctx, tt.args.user)
			fmt.Println(err)
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
		name     string
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
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "viewer",
						},
					},
				},
				roles: []chronograf.Role{
					{
						Name: "editor",
					},
				},
			},
			want: &chronograf.User{
				Name:     "bobetta",
				Provider: "github",
				Scheme:   "oauth2",
				Roles: []chronograf.Role{
					{
						Name: "editor",
					},
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
					Provider: "github",
					Scheme:   "oauth2",
				},
				provider: "google",
				scheme:   "oauth2",
				name:     "billietta",
			},
			want: &chronograf.User{
				Name:     "billietta",
				Provider: "google",
				Scheme:   "oauth2",
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

		if tt.args.name != "" {
			tt.args.usr.Name = tt.args.name
		}

		if err := s.Update(tt.args.ctx, tt.args.usr); (err != nil) != tt.wantErr {
			t.Errorf("%q. UsersStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
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
			t.Errorf("%q. UsersStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
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
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "viewer",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "editor",
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
			if diff := cmp.Diff(got, tt.want[i], cmpOptions...); diff != "" {
				t.Errorf("%q. UsersStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestUsersStore_Num(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		users   []chronograf.User
		want    int
		wantErr bool
	}{
		{
			name: "No users",
			want: 0,
		},
		{
			name: "Update new user",
			want: 2,
			users: []chronograf.User{
				{
					Name:     "howdy",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "viewer",
						},
					},
				},
				{
					Name:     "doody",
					Provider: "github",
					Scheme:   "oauth2",
					Roles: []chronograf.Role{
						{
							Name: "editor",
						},
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
		s := client.UsersStore

		for _, u := range tt.users {
			s.Add(tt.ctx, &u)
		}
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
