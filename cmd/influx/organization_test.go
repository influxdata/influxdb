package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCmdOrg(t *testing.T) {
	fakeOrgSVCFn := func(svc influxdb.OrganizationService) orgSVCFn {
		return func() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error) {
			return svc, mock.NewUserResourceMappingService(), mock.NewUserService(), nil
		}
	}

	fakeOrgUrmSVCsFn := func(svc influxdb.OrganizationService, urmSVC influxdb.UserResourceMappingService) orgSVCFn {
		return func() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error) {
			return svc, urmSVC, mock.NewUserService(), nil
		}
	}

	t.Run("create", func(t *testing.T) {
		tests := []struct {
			name     string
			expected influxdb.Organization
			flags    []string
		}{
			{
				name:  "all",
				flags: []string{"--name=new name", "--description=desc"},
				expected: influxdb.Organization{
					Name:        "new name",
					Description: "desc",
				},
			},
			{
				name:  "shorts",
				flags: []string{"-n=new name", "-d=desc"},
				expected: influxdb.Organization{
					Name:        "new name",
					Description: "desc",
				},
			},
		}

		cmdFn := func(expectedOrg influxdb.Organization) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewOrganizationService()
			svc.CreateOrganizationF = func(ctx context.Context, org *influxdb.Organization) error {
				if expectedOrg != *org {
					return fmt.Errorf("unexpected org;\n\twant= %+v\n\tgot=  %+v", expectedOrg, *org)
				}
				return nil
			}

			return func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), f, opt)
				return builder.cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expected))
				cmd.SetArgs(append([]string{"org", "create"}, tt.flags...))

				require.NoError(t, cmd.Execute())
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
				name:       "id",
				expectedID: influxdb.ID(1),
				flag:       "--id=",
			},
			{
				name:       "shorts",
				expectedID: influxdb.ID(1),
				flag:       "-i=",
			},
		}

		cmdFn := func(expectedID influxdb.ID) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewOrganizationService()
			svc.FindOrganizationByIDF = func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
				return &influxdb.Organization{ID: id}, nil
			}
			svc.DeleteOrganizationF = func(ctx context.Context, id influxdb.ID) error {
				if expectedID != id {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedID, id)
				}
				return nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), g, opt)
				return builder.cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expectedID))
				idFlag := tt.flag + tt.expectedID.String()
				cmd.SetArgs([]string{"org", "find", idFlag})

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("list", func(t *testing.T) {
		type called struct {
			name string
			id   influxdb.ID
		}

		tests := []struct {
			name     string
			expected called
			flags    []string
			command  string
			envVars  map[string]string
		}{
			{
				name:     "org id",
				flags:    []string{"--id=" + influxdb.ID(3).String()},
				envVars:  envVarsZeroMap,
				expected: called{id: 3},
			},
			{
				name:     "name",
				flags:    []string{"--name=name1"},
				envVars:  envVarsZeroMap,
				expected: called{name: "name1"},
			},
			{
				name: "shorts",
				flags: []string{
					"-n=name1",
					"-i=" + influxdb.ID(1).String(),
				},
				envVars:  envVarsZeroMap,
				expected: called{name: "name1", id: 1},
			},
			{
				name: "env vars",
				envVars: map[string]string{
					"INFLUX_ORG_ID": influxdb.ID(1).String(),
					"INFLUX_ORG":    "name1",
				},
				flags:    []string{"-i=" + influxdb.ID(1).String()},
				expected: called{name: "name1", id: 1},
			},
			{
				name:     "ls alias",
				command:  "ls",
				flags:    []string{"--name=name1"},
				envVars:  envVarsZeroMap,
				expected: called{name: "name1"},
			},
			{
				name:     "find alias",
				command:  "find",
				flags:    []string{"--name=name1"},
				envVars:  envVarsZeroMap,
				expected: called{name: "name1"},
			},
		}

		cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
			calls := new(called)

			svc := mock.NewOrganizationService()
			svc.FindOrganizationsF = func(ctx context.Context, f influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
				if f.ID != nil {
					calls.id = *f.ID
				}
				if f.Name != nil {
					calls.name = *f.Name
				}
				return nil, 0, nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), g, opt)
				return builder.cmd()
			}, calls
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmdFn, calls := cmdFn()
				cmd := builder.cmd(cmdFn)

				if tt.command == "" {
					tt.command = "list"
				}

				cmd.SetArgs(append([]string{"org", tt.command}, tt.flags...))

				require.NoError(t, cmd.Execute())
				assert.Equal(t, tt.expected, *calls)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("update", func(t *testing.T) {
		tests := []struct {
			name     string
			expected influxdb.OrganizationUpdate
			flags    []string
			envVars  map[string]string
		}{
			{
				name: "basic just name",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
				},
				expected: influxdb.OrganizationUpdate{
					Name: strPtr("new name"),
				},
			},
			{
				name: "with all fields",
				flags: []string{
					"--id=" + influxdb.ID(3).String(),
					"--name=new name",
					"--description=desc",
				},
				expected: influxdb.OrganizationUpdate{
					Name:        strPtr("new name"),
					Description: strPtr("desc"),
				},
			},
			{
				name: "shorts",
				flags: []string{
					"-i=" + influxdb.ID(3).String(),
					"-n=new name",
					"-d=desc",
				},
				expected: influxdb.OrganizationUpdate{
					Name:        strPtr("new name"),
					Description: strPtr("desc"),
				},
			},
			{
				name: "env var",
				envVars: map[string]string{
					"INFLUX_ORG":             "new name",
					"INFLUX_ORG_ID":          influxdb.ID(3).String(),
					"INFLUX_ORG_DESCRIPTION": "desc",
				},
				expected: influxdb.OrganizationUpdate{
					Name:        strPtr("new name"),
					Description: strPtr("desc"),
				},
			},
		}

		cmdFn := func(expectedUpdate influxdb.OrganizationUpdate) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewOrganizationService()
			svc.UpdateOrganizationF = func(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
				if id != 3 {
					return nil, fmt.Errorf("unexpecte id:\n\twant= %s\n\tgot=  %s", influxdb.ID(3), id)
				}
				if !reflect.DeepEqual(expectedUpdate, upd) {
					return nil, fmt.Errorf("unexpected bucket update;\n\twant= %+v\n\tgot=  %+v", expectedUpdate, upd)
				}
				return &influxdb.Organization{}, nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), g, opt)
				return builder.cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expected))
				cmd.SetArgs(append([]string{"org", "update"}, tt.flags...))

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("members", func(t *testing.T) {
		type (
			called struct {
				name     string
				id       influxdb.ID
				memberID influxdb.ID
			}

			testCase struct {
				name        string
				expected    called
				memberFlags []string
				envVars     map[string]string
			}
		)

		testMemberFn := func(t *testing.T, cmdName string, cmdFn func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called), testCases ...testCase) {
			for _, tt := range testCases {
				fn := func(t *testing.T) {
					envVars := tt.envVars
					if len(envVars) == 0 {
						envVars = envVarsZeroMap
					}
					defer addEnvVars(t, envVars)()

					outBuf := new(bytes.Buffer)
					defer func() {
						if t.Failed() && outBuf.Len() > 0 {
							t.Log(outBuf.String())
						}
					}()

					builder := newInfluxCmdBuilder(
						in(new(bytes.Buffer)),
						out(outBuf),
					)
					nestedCmd, calls := cmdFn()
					cmd := builder.cmd(nestedCmd)
					cmd.SetArgs(append([]string{"org", "members", cmdName}, tt.memberFlags...))

					require.NoError(t, cmd.Execute())
					assert.Equal(t, tt.expected, *calls)
				}

				t.Run(tt.name, fn)
			}
		}

		t.Run("list", func(t *testing.T) {
			tests := []testCase{
				{
					name:        "org id",
					memberFlags: []string{"--id=" + influxdb.ID(3).String()},
					envVars:     envVarsZeroMap,
					expected:    called{id: 3},
				},
				{
					name:        "org id short",
					memberFlags: []string{"-i=" + influxdb.ID(3).String()},
					envVars:     envVarsZeroMap,
					expected:    called{id: 3},
				},
				{
					name: "org id env var",
					envVars: map[string]string{
						"INFLUX_ORG":    "",
						"INFLUX_ORG_ID": influxdb.ID(3).String(),
					},
					expected: called{id: 3},
				},
				{
					name:        "name",
					memberFlags: []string{"--name=name1"},
					envVars:     envVarsZeroMap,
					expected:    called{name: "name1"},
				},
				{
					name:        "name short",
					memberFlags: []string{"-n=name1"},
					envVars:     envVarsZeroMap,
					expected:    called{name: "name1"},
				},
				{
					name:     "name env var",
					envVars:  map[string]string{"INFLUX_ORG": "name1"},
					expected: called{name: "name1"},
				},
			}

			cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
				calls := new(called)

				svc := mock.NewOrganizationService()
				svc.FindOrganizationF = func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					if f.ID != nil {
						calls.id = *f.ID
					}
					if f.Name != nil {
						calls.name = *f.Name
					}
					return &influxdb.Organization{ID: 1}, nil
				}

				return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
					builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), g, opt)
					return builder.cmd()
				}, calls
			}

			testMemberFn(t, "list", cmdFn, tests...)
			testMemberFn(t, "ls", cmdFn, tests[0:1]...)
			testMemberFn(t, "find", cmdFn, tests[0:1]...)
		})

		t.Run("add", func(t *testing.T) {
			cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
				calls := new(called)

				svc := mock.NewOrganizationService()
				svc.FindOrganizationF = func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					if f.ID != nil {
						calls.id = *f.ID
					}
					if f.Name != nil {
						calls.name = *f.Name
					}
					return &influxdb.Organization{ID: 1}, nil
				}
				urmSVC := mock.NewUserResourceMappingService()
				urmSVC.CreateMappingFn = func(ctx context.Context, m *influxdb.UserResourceMapping) error {
					calls.memberID = m.UserID
					return nil
				}

				return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
					builder := newCmdOrgBuilder(fakeOrgUrmSVCsFn(svc, urmSVC), g, opt)
					return builder.cmd()
				}, calls
			}

			addTests := []testCase{
				{
					name: "org id",
					memberFlags: []string{
						"--id=" + influxdb.ID(3).String(),
						"--member=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org id shorts",
					memberFlags: []string{
						"-i=" + influxdb.ID(3).String(),
						"-m=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org name",
					memberFlags: []string{
						"--name=name1",
						"--member=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{name: "name1", memberID: 4},
				},
				{
					name: "org name shorts",
					memberFlags: []string{
						"-n=name1",
						"-m=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{name: "name1", memberID: 4},
				},
			}

			testMemberFn(t, "add", cmdFn, addTests...)
		})

		t.Run("remove", func(t *testing.T) {
			cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
				calls := new(called)

				svc := mock.NewOrganizationService()
				svc.FindOrganizationF = func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					if f.ID != nil {
						calls.id = *f.ID
					}
					if f.Name != nil {
						calls.name = *f.Name
					}
					return &influxdb.Organization{ID: 1}, nil
				}
				urmSVC := mock.NewUserResourceMappingService()
				urmSVC.DeleteMappingFn = func(ctx context.Context, resourceID, userID influxdb.ID) error {
					calls.memberID = userID
					return nil
				}

				return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
					builder := newCmdOrgBuilder(fakeOrgUrmSVCsFn(svc, urmSVC), g, opt)
					return builder.cmd()
				}, calls
			}

			addTests := []testCase{
				{
					name: "org id",
					memberFlags: []string{
						"--id=" + influxdb.ID(3).String(),
						"--member=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org id shorts",
					memberFlags: []string{
						"-i=" + influxdb.ID(3).String(),
						"-m=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org name",
					memberFlags: []string{
						"--name=name1",
						"--member=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{name: "name1", memberID: 4},
				},
				{
					name: "org name shorts",
					memberFlags: []string{
						"-n=name1",
						"-m=" + influxdb.ID(4).String(),
					},
					envVars:  envVarsZeroMap,
					expected: called{name: "name1", memberID: 4},
				},
			}

			testMemberFn(t, "remove", cmdFn, addTests...)
		})
	})
}

var envVarsZeroMap = map[string]string{
	"INFLUX_ORG_ID": "",
	"INFLUX_ORG":    "",
}
