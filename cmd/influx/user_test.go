package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	input "github.com/tcnksm/go-input"
)

func newCMDUserDeps(
	userSVC influxdb.UserService,
	passSVC influxdb.PasswordsService,
	getPassFn func(*input.UI, bool) string,
) cmdUserDeps {
	return cmdUserDeps{
		userSVC: userSVC,
		orgSvc: &mock.OrganizationService{
			FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
				return &influxdb.Organization{ID: influxdb.ID(9000), Name: "influxdata"}, nil
			},
		},
		passSVC: passSVC,
		urmSVC: &mock.UserResourceMappingService{
			CreateMappingFn: func(context.Context, *platform.UserResourceMapping) error {
				return nil
			},
		},
		getPassFn: getPassFn,
	}
}

func TestCmdUser(t *testing.T) {
	setViperOptions()

	type userResult struct {
		user     influxdb.User
		password string
	}

	fakeSVCFn := func(dep cmdUserDeps) userSVCsFn {
		return func() (
			cmdUserDeps,
			error) {
			return dep, nil
		}
	}

	t.Run("create", func(t *testing.T) {
		tests := []struct {
			name     string
			expected userResult
			flags    []string
			envVars  map[string]string
		}{
			{
				name:  "basic just name",
				flags: []string{"--name=new name", "--org=org name"},
				expected: userResult{
					user: influxdb.User{
						Name: "new name",
					},
				},
			},
			{
				name: "with password",
				flags: []string{
					"--name=new name",
					"--password=pass1",
					"--org=org name",
				},
				expected: userResult{
					user: influxdb.User{
						Name: "new name",
					},
					password: "pass1",
				},
			},
			{
				name: "with password and env",
				flags: []string{
					"--name=new name",
					"--password=pass1",
				},
				envVars: map[string]string{
					"INFLUX_ORG":    "",
					"INFLUX_ORG_ID": influxdb.ID(1).String(),
				},
				expected: userResult{
					user: influxdb.User{
						Name: "new name",
					},
					password: "pass1",
				},
			},
			{
				name: "shorts",
				flags: []string{
					"-n=new name",
					"-o=org name",
				},
				expected: userResult{
					user: influxdb.User{
						Name: "new name",
					},
				},
			},
		}

		cmdFn := func(expected userResult) *cobra.Command {
			svc := mock.NewUserService()
			svc.CreateUserFn = func(ctx context.Context, User *influxdb.User) error {
				if expected.user != *User {
					return fmt.Errorf("unexpected User;\n\twant= %+v\n\tgot=  %+v", expected, *User)
				}
				return nil
			}
			passSVC := mock.NewPasswordsService()
			passSVC.SetPasswordFn = func(ctx context.Context, id influxdb.ID, password string) error {
				if expected.password != password {
					return fmt.Errorf("unexpected password;\n\twant= %+v\n\tgot=  %+v", expected.password, password)
				}
				return nil
			}

			builder := newCmdUserBuilder(fakeSVCFn(newCMDUserDeps(svc, passSVC, nil)), out(ioutil.Discard))
			cmd := builder.cmdCreate()
			cmd.RunE = builder.cmdCreateRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()
				cmd := cmdFn(tt.expected)
				cmd.SetArgs(tt.flags)
				err := cmd.Execute()
				require.NoError(t, err)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tests := []struct {
			name       string
			expectedID influxdb.ID
			flag       string
		}{
			{
				name:       "long id",
				expectedID: influxdb.ID(1),
				flag:       "--id=",
			},
			{
				name:       "shorts",
				expectedID: influxdb.ID(1),
				flag:       "-i=",
			},
		}

		cmdFn := func(expectedID influxdb.ID) *cobra.Command {
			svc := mock.NewUserService()
			svc.FindUserByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
				return &influxdb.User{ID: id}, nil
			}
			svc.DeleteUserFn = func(ctx context.Context, id influxdb.ID) error {
				if expectedID != id {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedID, id)
				}
				return nil
			}

			builder := newCmdUserBuilder(fakeSVCFn(newCMDUserDeps(svc, nil, nil)), out(ioutil.Discard))
			cmd := builder.cmdDelete()
			cmd.RunE = builder.cmdDeleteRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expectedID)
				idFlag := tt.flag + tt.expectedID.String()
				cmd.SetArgs([]string{idFlag})
				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("find", func(t *testing.T) {
		type called struct {
			name string
			id   influxdb.ID
		}

		tests := []struct {
			name     string
			expected called
			flags    []string
		}{
			{
				name: "id",
				flags: []string{
					"--id=" + influxdb.ID(2).String(),
				},
				expected: called{
					id: 2,
				},
			},
			{
				name:     "name",
				flags:    []string{"--name=name1"},
				expected: called{name: "name1"},
			},
			{
				name: "shorts",
				flags: []string{
					"-n=name1",
					"-i=" + influxdb.ID(1).String(),
				},
				expected: called{name: "name1", id: 1},
			},
		}

		cmdFn := func() (*cobra.Command, *called) {
			calls := new(called)

			svc := mock.NewUserService()
			svc.FindUsersFn = func(ctx context.Context, f influxdb.UserFilter, opt ...influxdb.FindOptions) ([]*influxdb.User, int, error) {
				if f.ID != nil {
					calls.id = *f.ID
				}
				if f.Name != nil {
					calls.name = *f.Name
				}
				return nil, 0, nil
			}

			builder := newCmdUserBuilder(fakeSVCFn(newCMDUserDeps(svc, nil, nil)), in(new(bytes.Buffer)), out(ioutil.Discard))
			cmd := builder.cmdFind()
			cmd.RunE = builder.cmdFindRunEFn
			return cmd, calls
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd, calls := cmdFn()
				cmd.SetArgs(tt.flags)

				require.NoError(t, cmd.Execute())
				assert.Equal(t, tt.expected, *calls)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("update", func(t *testing.T) {
		tests := []struct {
			name     string
			expected influxdb.UserUpdate
			flags    []string
		}{
			{
				name: "basic just name",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
				},
				expected: influxdb.UserUpdate{
					Name: strPtr("new name"),
				},
			},
			{
				name: "with all fields",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
				},
				expected: influxdb.UserUpdate{
					Name: strPtr("new name"),
				},
			},
			{
				name: "shorts",
				flags: []string{
					"-i=" + influxdb.ID(3).String(),
					"-n=new name",
				},
				expected: influxdb.UserUpdate{
					Name: strPtr("new name"),
				},
			},
		}

		cmdFn := func(expectedUpdate influxdb.UserUpdate) *cobra.Command {
			svc := mock.NewUserService()
			svc.UpdateUserFn = func(ctx context.Context, id influxdb.ID, upd influxdb.UserUpdate) (*influxdb.User, error) {
				if id != 3 {
					return nil, fmt.Errorf("unexpecte id:\n\twant= %s\n\tgot=  %s", influxdb.ID(3), id)
				}
				if !reflect.DeepEqual(expectedUpdate, upd) {
					return nil, fmt.Errorf("unexpected User update;\n\twant= %+v\n\tgot=  %+v", expectedUpdate, upd)
				}
				return &influxdb.User{}, nil
			}

			builder := newCmdUserBuilder(fakeSVCFn(newCMDUserDeps(svc, nil, nil)), out(ioutil.Discard))
			cmd := builder.cmdUpdate()
			cmd.RunE = builder.cmdUpdateRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expected)
				cmd.SetArgs(tt.flags)
				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("password", func(t *testing.T) {
		tests := []struct {
			name     string
			expected string
			flags    []string
		}{
			{
				name: "basic id",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
				},
				expected: "pass1",
			},
			{
				name: "shorts",
				flags: []string{
					"-i=" + influxdb.ID(3).String(),
					"-n=new name",
				},
				expected: "pass1",
			},
		}

		cmdFn := func(expected string) *cobra.Command {
			svc := mock.NewUserService()
			svc.FindUserFn = func(ctx context.Context, f influxdb.UserFilter) (*influxdb.User, error) {
				usr := new(influxdb.User)
				if id := f.ID; id != nil {
					usr.ID = *id
				}
				if name := f.Name; name != nil {
					usr.Name = *name
				}
				return usr, nil
			}
			passSVC := mock.NewPasswordsService()
			passSVC.SetPasswordFn = func(ctx context.Context, id influxdb.ID, pass string) error {
				if pass != expected {
					return fmt.Errorf("unexpecte id:\n\twant= %s\n\tgot=  %s", pass, expected)
				}
				return nil
			}

			getPassFn := func(*input.UI, bool) string {
				return expected
			}
			builder := newCmdUserBuilder(fakeSVCFn(newCMDUserDeps(svc, passSVC, getPassFn)),
				out(ioutil.Discard))
			cmd := builder.cmdPassword()
			cmd.RunE = builder.cmdPasswordRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expected)
				cmd.SetArgs(tt.flags)
				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})
}
