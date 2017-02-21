package enterprise

import (
	"container/ring"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
)

func TestClient_Add(t *testing.T) {
	type fields struct {
		Ctrl   *mockCtrl
		Logger chronograf.Logger
	}
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Successful Create User",
			fields: fields{
				Ctrl: &mockCtrl{
					createUser: func(ctx context.Context, name, passwd string) error {
						return nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
			want: &chronograf.User{
				Name:   "marty",
				Passwd: "johnny be good",
			},
		},
		{
			name: "Failure to Create User",
			fields: fields{
				Ctrl: &mockCtrl{
					createUser: func(ctx context.Context, name, passwd string) error {
						return fmt.Errorf("1.21 Gigawatts! Tom, how could I have been so careless?")
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Client{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		got, err := c.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Add() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_Delete(t *testing.T) {
	type fields struct {
		Ctrl   *mockCtrl
		Logger chronograf.Logger
	}
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Successful Delete User",
			fields: fields{
				Ctrl: &mockCtrl{
					deleteUser: func(ctx context.Context, name string) error {
						return nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
		},
		{
			name: "Failure to Delete User",
			fields: fields{
				Ctrl: &mockCtrl{
					deleteUser: func(ctx context.Context, name string) error {
						return fmt.Errorf("1.21 Gigawatts! Tom, how could I have been so careless?")
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Client{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		if err := c.Delete(tt.args.ctx, tt.args.u); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestClient_Get(t *testing.T) {
	type fields struct {
		Ctrl      *mockCtrl
		Logger    chronograf.Logger
		dataNodes *ring.Ring
		opened    bool
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.User
		wantErr bool
	}{
		{
			name: "Successful Get User",
			fields: fields{
				Ctrl: &mockCtrl{
					user: func(ctx context.Context, name string) (*User, error) {
						return &User{
							Name:     "marty",
							Password: "johnny be good",
							Permissions: map[string][]string{
								"": {
									"ViewChronograf",
									"ReadData",
									"WriteData",
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx:  context.Background(),
				name: "marty",
			},
			want: &chronograf.User{
				Name: "marty",
				Permissions: chronograf.Permissions{
					{
						Scope:   chronograf.AllScope,
						Allowed: chronograf.Allowances{"ViewChronograf", "ReadData", "WriteData"},
					},
				},
			},
		},
		{
			name: "Failure to get User",
			fields: fields{
				Ctrl: &mockCtrl{
					user: func(ctx context.Context, name string) (*User, error) {
						return nil, fmt.Errorf("1.21 Gigawatts! Tom, how could I have been so careless?")
					},
				},
			},
			args: args{
				ctx:  context.Background(),
				name: "marty",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Client{
			Ctrl:      tt.fields.Ctrl,
			Logger:    tt.fields.Logger,
			dataNodes: tt.fields.dataNodes,
			opened:    tt.fields.opened,
		}
		got, err := c.Get(tt.args.ctx, tt.args.name)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Get() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestClient_Update(t *testing.T) {
	type fields struct {
		Ctrl   *mockCtrl
		Logger chronograf.Logger
	}
	type args struct {
		ctx context.Context
		u   *chronograf.User
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "Successful Change Password",
			fields: fields{
				Ctrl: &mockCtrl{
					changePassword: func(ctx context.Context, name, passwd string) error {
						return nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
		},
		{
			name: "Failure to Change Password",
			fields: fields{
				Ctrl: &mockCtrl{
					changePassword: func(ctx context.Context, name, passwd string) error {
						return fmt.Errorf("Ronald Reagan, the actor?! Ha Then who’s Vice President Jerry Lewis? I suppose Jane Wyman is First Lady")
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
				},
			},
			wantErr: true,
		},
		{
			name: "Success setting permissions User",
			fields: fields{
				Ctrl: &mockCtrl{
					setUserPerms: func(ctx context.Context, name string, perms Permissions) error {
						return nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "marty",
					Permissions: chronograf.Permissions{
						{
							Scope:   chronograf.AllScope,
							Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "Failure setting permissions User",
			fields: fields{
				Ctrl: &mockCtrl{
					setUserPerms: func(ctx context.Context, name string, perms Permissions) error {
						return fmt.Errorf("They found me, I don't know how, but they found me.")
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name: "marty",
					Permissions: chronograf.Permissions{
						{
							Scope:   chronograf.AllScope,
							Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Client{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		if err := c.Update(tt.args.ctx, tt.args.u); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestClient_All(t *testing.T) {
	type fields struct {
		Ctrl   *mockCtrl
		Logger chronograf.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []chronograf.User
		wantErr bool
	}{
		{
			name: "Successful Get User",
			fields: fields{
				Ctrl: &mockCtrl{
					users: func(ctx context.Context, name *string) (*Users, error) {
						return &Users{
							Users: []User{
								{
									Name:     "marty",
									Password: "johnny be good",
									Permissions: map[string][]string{
										"": {
											"ViewChronograf",
											"ReadData",
											"WriteData",
										},
									},
								},
							},
						}, nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
			},
			want: []chronograf.User{
				{
					Name: "marty",
					Permissions: chronograf.Permissions{
						{
							Scope:   chronograf.AllScope,
							Allowed: chronograf.Allowances{"ViewChronograf", "ReadData", "WriteData"},
						},
					},
				},
			},
		},
		{
			name: "Failure to get User",
			fields: fields{
				Ctrl: &mockCtrl{
					users: func(ctx context.Context, name *string) (*Users, error) {
						return nil, fmt.Errorf("1.21 Gigawatts! Tom, how could I have been so careless?")
					},
				},
			},
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		c := &Client{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		got, err := c.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.All() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func Test_toEnterprise(t *testing.T) {
	tests := []struct {
		name  string
		perms chronograf.Permissions
		want  Permissions
	}{
		{
			name: "All Scopes",
			want: Permissions{"": []string{"ViewChronograf", "KapacitorAPI"}},
			perms: chronograf.Permissions{
				{
					Scope:   chronograf.AllScope,
					Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
				},
			},
		},
		{
			name: "DB Scope",
			want: Permissions{"telegraf": []string{"ReadData", "WriteData"}},
			perms: chronograf.Permissions{
				{
					Scope:   chronograf.DBScope,
					Name:    "telegraf",
					Allowed: chronograf.Allowances{"ReadData", "WriteData"},
				},
			},
		},
	}
	for _, tt := range tests {
		if got := toEnterprise(tt.perms); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. toEnterprise() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func Test_toChronograf(t *testing.T) {
	tests := []struct {
		name  string
		perms Permissions
		want  chronograf.Permissions
	}{
		{
			name:  "All Scopes",
			perms: Permissions{"": []string{"ViewChronograf", "KapacitorAPI"}},
			want: chronograf.Permissions{
				{
					Scope:   chronograf.AllScope,
					Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
				},
			},
		},
		{
			name:  "DB Scope",
			perms: Permissions{"telegraf": []string{"ReadData", "WriteData"}},
			want: chronograf.Permissions{
				{
					Scope:   chronograf.DBScope,
					Name:    "telegraf",
					Allowed: chronograf.Allowances{"ReadData", "WriteData"},
				},
			},
		},
	}
	for _, tt := range tests {
		if got := toChronograf(tt.perms); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. toChronograf() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

type mockCtrl struct {
	showCluster    func(ctx context.Context) (*Cluster, error)
	user           func(ctx context.Context, name string) (*User, error)
	createUser     func(ctx context.Context, name, passwd string) error
	deleteUser     func(ctx context.Context, name string) error
	changePassword func(ctx context.Context, name, passwd string) error
	users          func(ctx context.Context, name *string) (*Users, error)
	setUserPerms   func(ctx context.Context, name string, perms Permissions) error
}

func (m *mockCtrl) ShowCluster(ctx context.Context) (*Cluster, error) {
	return m.showCluster(ctx)
}
func (m *mockCtrl) User(ctx context.Context, name string) (*User, error) {
	return m.user(ctx, name)
}
func (m *mockCtrl) CreateUser(ctx context.Context, name, passwd string) error {
	return m.createUser(ctx, name, passwd)
}
func (m *mockCtrl) DeleteUser(ctx context.Context, name string) error {
	return m.deleteUser(ctx, name)
}
func (m *mockCtrl) ChangePassword(ctx context.Context, name, passwd string) error {
	return m.changePassword(ctx, name, passwd)
}
func (m *mockCtrl) Users(ctx context.Context, name *string) (*Users, error) {
	return m.users(ctx, name)
}
func (m *mockCtrl) SetUserPerms(ctx context.Context, name string, perms Permissions) error {
	return m.setUserPerms(ctx, name, perms)
}
