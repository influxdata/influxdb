package main

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCmdOrg(t *testing.T) {
	setViperOptions()

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

		cmdFn := func(expectedOrg influxdb.Organization) *cobra.Command {
			svc := mock.NewOrganizationService()
			svc.CreateOrganizationF = func(ctx context.Context, org *influxdb.Organization) error {
				if expectedOrg != *org {
					return fmt.Errorf("unexpected org;\n\twant= %+v\n\tgot=  %+v", expectedOrg, *org)
				}
				return nil
			}

			builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), out(new(bytes.Buffer)))
			cmd := builder.cmdCreate()
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expected)
				cmd.Flags().Parse(tt.flags)
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

		cmdFn := func(expectedID influxdb.ID) *cobra.Command {
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

			builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), out(new(bytes.Buffer)))
			cmd := builder.cmdDelete()
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expectedID)
				idFlag := tt.flag + tt.expectedID.String()
				cmd.Flags().Parse([]string{idFlag})
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
			envVars  map[string]string
		}{
			{
				name:     "org id",
				flags:    []string{"--id=" + influxdb.ID(3).String()},
				expected: called{id: 3},
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
			{
				name: "env vars",
				envVars: map[string]string{
					"INFLUX_ORG_ID": influxdb.ID(1).String(),
					"INFLUX_ORG":    "name1",
				},
				flags:    []string{"-i=" + influxdb.ID(1).String()},
				expected: called{name: "name1", id: 1},
			},
		}

		cmdFn := func() (*cobra.Command, *called) {
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

			builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), in(new(bytes.Buffer)), out(new(bytes.Buffer)))
			cmd := builder.cmdFind()
			return cmd, calls
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				cmd, calls := cmdFn()
				cmd.Flags().Parse(tt.flags)

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

		cmdFn := func(expectedUpdate influxdb.OrganizationUpdate) *cobra.Command {
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

			builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), out(new(bytes.Buffer)))
			cmd := builder.cmdUpdate()
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				cmd := cmdFn(tt.expected)
				cmd.Flags().Parse(tt.flags)
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

		testMemberFn := func(t *testing.T, cmdFn func() (*cobra.Command, *called), testCases ...testCase) {
			for _, tt := range testCases {
				fn := func(t *testing.T) {
					defer addEnvVars(t, tt.envVars)()

					cmd, calls := cmdFn()
					cmd.Flags().Parse(tt.memberFlags)

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
					expected:    called{id: 3},
				},
				{
					name:        "org id short",
					memberFlags: []string{"-i=" + influxdb.ID(3).String()},
					expected:    called{id: 3},
				},
				{
					name:     "org id env var",
					envVars:  map[string]string{"INFLUX_ORG_ID": influxdb.ID(3).String()},
					expected: called{id: 3},
				},
				{
					name:        "name",
					memberFlags: []string{"--name=name1"},
					expected:    called{name: "name1"},
				},
				{
					name:        "name short",
					memberFlags: []string{"-n=name1"},
					expected:    called{name: "name1"},
				},
				{
					name:     "name env var",
					envVars:  map[string]string{"INFLUX_ORG": "name1"},
					expected: called{name: "name1"},
				},
			}

			cmdFn := func() (*cobra.Command, *called) {
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

				builder := newCmdOrgBuilder(fakeOrgSVCFn(svc), in(new(bytes.Buffer)), out(new(bytes.Buffer)))
				cmd := builder.cmdMemberList()
				return cmd, calls
			}

			testMemberFn(t, cmdFn, tests...)
		})

		t.Run("add", func(t *testing.T) {
			cmdFn := func() (*cobra.Command, *called) {
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

				builder := newCmdOrgBuilder(fakeOrgUrmSVCsFn(svc, urmSVC), in(new(bytes.Buffer)), out(new(bytes.Buffer)))
				cmd := builder.cmdMemberAdd()
				return cmd, calls
			}

			addTests := []testCase{
				{
					name: "org id",
					memberFlags: []string{
						"--id=" + influxdb.ID(3).String(),
						"--member=" + influxdb.ID(4).String(),
					},
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org id shorts",
					memberFlags: []string{
						"-i=" + influxdb.ID(3).String(),
						"-m=" + influxdb.ID(4).String(),
					},
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org name",
					memberFlags: []string{
						"--name=name1",
						"--member=" + influxdb.ID(4).String(),
					},
					expected: called{name: "name1", memberID: 4},
				},
				{
					name: "org name shorts",
					memberFlags: []string{
						"-n=name1",
						"-m=" + influxdb.ID(4).String(),
					},
					expected: called{name: "name1", memberID: 4},
				},
			}

			testMemberFn(t, cmdFn, addTests...)
		})

		t.Run("remove", func(t *testing.T) {
			cmdFn := func() (*cobra.Command, *called) {
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

				builder := newCmdOrgBuilder(fakeOrgUrmSVCsFn(svc, urmSVC), in(new(bytes.Buffer)), out(new(bytes.Buffer)))
				cmd := builder.cmdMemberRemove()
				return cmd, calls
			}

			addTests := []testCase{
				{
					name: "org id",
					memberFlags: []string{
						"--id=" + influxdb.ID(3).String(),
						"--member=" + influxdb.ID(4).String(),
					},
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org id shorts",
					memberFlags: []string{
						"-i=" + influxdb.ID(3).String(),
						"-m=" + influxdb.ID(4).String(),
					},
					expected: called{id: 3, memberID: 4},
				},
				{
					name: "org name",
					memberFlags: []string{
						"--name=name1",
						"--member=" + influxdb.ID(4).String(),
					},
					expected: called{name: "name1", memberID: 4},
				},
				{
					name: "org name shorts",
					memberFlags: []string{
						"-n=name1",
						"-m=" + influxdb.ID(4).String(),
					},
					expected: called{name: "name1", memberID: 4},
				},
			}

			testMemberFn(t, cmdFn, addTests...)
		})
	})
}
