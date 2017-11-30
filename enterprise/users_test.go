package enterprise_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/enterprise"
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
					setUserPerms: func(ctx context.Context, name string, perms enterprise.Permissions) error {
						return nil
					},
					user: func(ctx context.Context, name string) (*enterprise.User, error) {
						return &enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
				Name: "marty",
				Permissions: chronograf.Permissions{
					{
						Scope:   chronograf.AllScope,
						Allowed: chronograf.Allowances{"ViewChronograf", "ReadData", "WriteData"},
					},
				},
				Roles: []chronograf.Role{},
			},
		},
		{
			name: "Successful Create User with roles",
			fields: fields{
				Ctrl: &mockCtrl{
					createUser: func(ctx context.Context, name, passwd string) error {
						return nil
					},
					setUserPerms: func(ctx context.Context, name string, perms enterprise.Permissions) error {
						return nil
					},
					user: func(ctx context.Context, name string) (*enterprise.User, error) {
						return &enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{
							"marty": enterprise.Roles{
								Roles: []enterprise.Role{
									{
										Name: "admin",
									},
								},
							},
						}, nil
					},
					addRoleUsers: func(ctx context.Context, name string, users []string) error {
						return nil
					},
				},
			},
			args: args{
				ctx: context.Background(),
				u: &chronograf.User{
					Name:   "marty",
					Passwd: "johnny be good",
					Roles: []chronograf.Role{
						{
							Name: "admin",
						},
					},
				},
			},
			want: &chronograf.User{
				Name: "marty",
				Permissions: chronograf.Permissions{
					{
						Scope:   chronograf.AllScope,
						Allowed: chronograf.Allowances{"ViewChronograf", "ReadData", "WriteData"},
					},
				},
				Roles: []chronograf.Role{
					{
						Name:        "admin",
						Users:       []chronograf.User{},
						Permissions: chronograf.Permissions{},
					},
				},
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
		c := &enterprise.UserStore{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		got, err := c.Add(tt.args.ctx, tt.args.u)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. Client.Add() = \n%#v\n, want \n%#v\n", tt.name, got, tt.want)
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
		c := &enterprise.UserStore{
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
		Ctrl   *mockCtrl
		Logger chronograf.Logger
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
					user: func(ctx context.Context, name string) (*enterprise.User, error) {
						return &enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
				Roles: []chronograf.Role{},
			},
		},
		{
			name: "Successful Get User with roles",
			fields: fields{
				Ctrl: &mockCtrl{
					user: func(ctx context.Context, name string) (*enterprise.User, error) {
						return &enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{
							"marty": enterprise.Roles{
								Roles: []enterprise.Role{
									{
										Name: "timetravels",
										Permissions: map[string][]string{
											"": {
												"ViewChronograf",
												"ReadData",
												"WriteData",
											},
										},
										Users: []string{"marty", "docbrown"},
									},
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
				Roles: []chronograf.Role{
					{
						Name: "timetravels",
						Permissions: chronograf.Permissions{
							{
								Scope:   chronograf.AllScope,
								Allowed: chronograf.Allowances{"ViewChronograf", "ReadData", "WriteData"},
							},
						},
						Users: []chronograf.User{},
					},
				},
			},
		},
		{
			name: "Failure to get User",
			fields: fields{
				Ctrl: &mockCtrl{
					user: func(ctx context.Context, name string) (*enterprise.User, error) {
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
		c := &enterprise.UserStore{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		got, err := c.Get(tt.args.ctx, chronograf.UserQuery{Name: &tt.args.name})
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
					setUserPerms: func(ctx context.Context, name string, perms enterprise.Permissions) error {
						return nil
					},
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
			name: "Success setting permissions and roles for user",
			fields: fields{
				Ctrl: &mockCtrl{
					setUserPerms: func(ctx context.Context, name string, perms enterprise.Permissions) error {
						return nil
					},
					addRoleUsers: func(ctx context.Context, name string, users []string) error {
						return nil
					},
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
					Roles: []chronograf.Role{
						{
							Name: "adminrole",
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
					setUserPerms: func(ctx context.Context, name string, perms enterprise.Permissions) error {
						return fmt.Errorf("They found me, I don't know how, but they found me.")
					},
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
		c := &enterprise.UserStore{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		if err := c.Update(tt.args.ctx, tt.args.u); (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestClient_Num(t *testing.T) {
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
					users: func(ctx context.Context, name *string) (*enterprise.Users, error) {
						return &enterprise.Users{
							Users: []enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
					Roles: []chronograf.Role{},
				},
			},
		},
		{
			name: "Failure to get User",
			fields: fields{
				Ctrl: &mockCtrl{
					users: func(ctx context.Context, name *string) (*enterprise.Users, error) {
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
		c := &enterprise.UserStore{
			Ctrl:   tt.fields.Ctrl,
			Logger: tt.fields.Logger,
		}
		got, err := c.Num(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. Client.Num() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if got != len(tt.want) {
			t.Errorf("%q. Client.Num() = %v, want %v", tt.name, got, len(tt.want))
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
					users: func(ctx context.Context, name *string) (*enterprise.Users, error) {
						return &enterprise.Users{
							Users: []enterprise.User{
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
					userRoles: func(ctx context.Context) (map[string]enterprise.Roles, error) {
						return map[string]enterprise.Roles{}, nil
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
					Roles: []chronograf.Role{},
				},
			},
		},
		{
			name: "Failure to get User",
			fields: fields{
				Ctrl: &mockCtrl{
					users: func(ctx context.Context, name *string) (*enterprise.Users, error) {
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
		c := &enterprise.UserStore{
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

func Test_ToEnterprise(t *testing.T) {
	tests := []struct {
		name  string
		perms chronograf.Permissions
		want  enterprise.Permissions
	}{
		{
			name: "All Scopes",
			want: enterprise.Permissions{"": []string{"ViewChronograf", "KapacitorAPI"}},
			perms: chronograf.Permissions{
				{
					Scope:   chronograf.AllScope,
					Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
				},
			},
		},
		{
			name: "DB Scope",
			want: enterprise.Permissions{"telegraf": []string{"ReadData", "WriteData"}},
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
		if got := enterprise.ToEnterprise(tt.perms); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. ToEnterprise() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func Test_ToChronograf(t *testing.T) {
	tests := []struct {
		name  string
		perms enterprise.Permissions
		want  chronograf.Permissions
	}{
		{
			name:  "All Scopes",
			perms: enterprise.Permissions{"": []string{"ViewChronograf", "KapacitorAPI"}},
			want: chronograf.Permissions{
				{
					Scope:   chronograf.AllScope,
					Allowed: chronograf.Allowances{"ViewChronograf", "KapacitorAPI"},
				},
			},
		},
		{
			name:  "DB Scope",
			perms: enterprise.Permissions{"telegraf": []string{"ReadData", "WriteData"}},
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
		if got := enterprise.ToChronograf(tt.perms); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. toChronograf() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

type mockCtrl struct {
	showCluster    func(ctx context.Context) (*enterprise.Cluster, error)
	user           func(ctx context.Context, name string) (*enterprise.User, error)
	createUser     func(ctx context.Context, name, passwd string) error
	deleteUser     func(ctx context.Context, name string) error
	changePassword func(ctx context.Context, name, passwd string) error
	users          func(ctx context.Context, name *string) (*enterprise.Users, error)
	setUserPerms   func(ctx context.Context, name string, perms enterprise.Permissions) error

	userRoles func(ctx context.Context) (map[string]enterprise.Roles, error)

	roles           func(ctx context.Context, name *string) (*enterprise.Roles, error)
	role            func(ctx context.Context, name string) (*enterprise.Role, error)
	createRole      func(ctx context.Context, name string) error
	deleteRole      func(ctx context.Context, name string) error
	setRolePerms    func(ctx context.Context, name string, perms enterprise.Permissions) error
	setRoleUsers    func(ctx context.Context, name string, users []string) error
	addRoleUsers    func(ctx context.Context, name string, users []string) error
	removeRoleUsers func(ctx context.Context, name string, users []string) error
}

func (m *mockCtrl) ShowCluster(ctx context.Context) (*enterprise.Cluster, error) {
	return m.showCluster(ctx)
}

func (m *mockCtrl) User(ctx context.Context, name string) (*enterprise.User, error) {
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

func (m *mockCtrl) Users(ctx context.Context, name *string) (*enterprise.Users, error) {
	return m.users(ctx, name)
}

func (m *mockCtrl) SetUserPerms(ctx context.Context, name string, perms enterprise.Permissions) error {
	return m.setUserPerms(ctx, name, perms)
}

func (m *mockCtrl) UserRoles(ctx context.Context) (map[string]enterprise.Roles, error) {
	return m.userRoles(ctx)
}

func (m *mockCtrl) Roles(ctx context.Context, name *string) (*enterprise.Roles, error) {
	return m.roles(ctx, name)
}

func (m *mockCtrl) Role(ctx context.Context, name string) (*enterprise.Role, error) {
	return m.role(ctx, name)
}

func (m *mockCtrl) CreateRole(ctx context.Context, name string) error {
	return m.createRole(ctx, name)
}

func (m *mockCtrl) DeleteRole(ctx context.Context, name string) error {
	return m.deleteRole(ctx, name)
}

func (m *mockCtrl) SetRolePerms(ctx context.Context, name string, perms enterprise.Permissions) error {
	return m.setRolePerms(ctx, name, perms)
}

func (m *mockCtrl) SetRoleUsers(ctx context.Context, name string, users []string) error {
	return m.setRoleUsers(ctx, name, users)
}

func (m *mockCtrl) AddRoleUsers(ctx context.Context, name string, users []string) error {
	return m.addRoleUsers(ctx, name, users)
}

func (m *mockCtrl) RemoveRoleUsers(ctx context.Context, name string, users []string) error {
	return m.removeRoleUsers(ctx, name, users)
}
