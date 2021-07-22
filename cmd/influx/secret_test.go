package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	input "github.com/tcnksm/go-input"
)

func TestCmdSecret(t *testing.T) {
	orgID := influxdb.ID(9000)

	fakeSVCFn := func(svc influxdb.SecretService, fn func(*input.UI) string) secretSVCsFn {
		return func() (influxdb.SecretService, influxdb.OrganizationService, func(*input.UI) string, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: orgID, Name: "influxdata"}, nil
				},
			}, fn, nil
		}
	}

	t.Run("list", func(t *testing.T) {
		type called []string
		tests := []struct {
			name     string
			expected called
			flags    []string
			command  string
			envVars  map[string]string
		}{
			{
				name:     "org id",
				flags:    []string{"--org-id=" + influxdb.ID(3).String()},
				envVars:  envVarsZeroMap,
				expected: called{"k1", "k2", "k3"},
			},
			{
				name:     "org",
				flags:    []string{"--org=rg"},
				envVars:  envVarsZeroMap,
				expected: called{"k1", "k2", "k3"},
			},
			{
				name: "env vars",
				envVars: map[string]string{
					"INFLUX_ORG": "rg",
				},
				flags:    []string{},
				expected: called{"k1", "k2", "k3"},
			},
			{
				name:     "ls alias",
				command:  "ls",
				flags:    []string{"--org=rg"},
				envVars:  envVarsZeroMap,
				expected: called{"k1", "k2", "k3"},
			},
			{
				name:     "find alias",
				command:  "find",
				flags:    []string{"--org=rg"},
				envVars:  envVarsZeroMap,
				expected: called{"k1", "k2", "k3"},
			},
		}

		cmdFn := func() (func(*globalFlags, genericCLIOpts) *cobra.Command, *called) {
			calls := new(called)
			svc := mock.NewSecretService()
			svc.GetSecretKeysFn = func(ctx context.Context, organizationID influxdb.ID) ([]string, error) {
				if !organizationID.Valid() {
					return []string{}, nil
				}
				*calls = []string{"k1", "k2", "k3"}
				return []string{}, nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdSecretBuilder(fakeSVCFn(svc, nil), g, opt)
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
				nestedCmdFn, calls := cmdFn()
				cmd := builder.cmd(nestedCmdFn)

				if tt.command == "" {
					tt.command = "list"
				}

				cmd.SetArgs(append([]string{"secret", tt.command}, tt.flags...))

				require.NoError(t, cmd.Execute())
				assert.Equal(t, tt.expected, *calls)
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("delete", func(t *testing.T) {
		tests := []struct {
			name        string
			expectedKey string
			flags       []string
		}{
			{
				name:        "with key",
				expectedKey: "key1",
				flags: []string{
					"--org=org name", "--key=key1",
				},
			},
			{
				name:        "shorts",
				expectedKey: "key1",
				flags:       []string{"-o=" + orgID.String(), "-k=key1"},
			},
		}

		cmdFn := func(expectedKey string) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewSecretService()
			svc.DeleteSecretFn = func(ctx context.Context, orgID influxdb.ID, ks ...string) error {
				if expectedKey != ks[0] {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedKey, ks[0])
				}
				return nil
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdSecretBuilder(fakeSVCFn(svc, nil), g, opt)
				return builder.cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expectedKey))
				cmd.SetArgs(append([]string{"secret", "delete"}, tt.flags...))

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

	t.Run("update", func(t *testing.T) {
		tests := []struct {
			name        string
			expectedKey string
			flags       []string
		}{
			{
				name:        "with key",
				expectedKey: "key1",
				flags: []string{
					"--org=org name", "--key=key1",
				},
			},
			{
				name:        "with key and value",
				expectedKey: "key1",
				flags: []string{
					"--org=org name", "--key=key1", "--value=v1",
				},
			},
			{
				name:        "shorts",
				expectedKey: "key1",
				flags:       []string{"-o=" + orgID.String(), "-k=key1"},
			},
			{
				name:        "shorts with value",
				expectedKey: "key1",
				flags:       []string{"-o=" + orgID.String(), "-k=key1", "-v=v1"},
			},
		}

		cmdFn := func(expectedKey string) func(*globalFlags, genericCLIOpts) *cobra.Command {
			svc := mock.NewSecretService()
			svc.PatchSecretsFn = func(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
				var key string
				for k := range m {
					key = k
					break
				}
				if expectedKey != key {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedKey, key)
				}
				return nil
			}

			getSctFn := func(*input.UI) string {
				return "ss"
			}

			return func(g *globalFlags, opt genericCLIOpts) *cobra.Command {
				builder := newCmdSecretBuilder(fakeSVCFn(svc, getSctFn), g, opt)
				return builder.cmd()
			}
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				builder := newInfluxCmdBuilder(
					in(new(bytes.Buffer)),
					out(ioutil.Discard),
				)
				cmd := builder.cmd(cmdFn(tt.expectedKey))
				cmd.SetArgs(append([]string{"secret", "update"}, tt.flags...))

				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})
}
