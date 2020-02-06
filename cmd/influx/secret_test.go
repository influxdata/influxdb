package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	input "github.com/tcnksm/go-input"
)

func TestCmdSecret(t *testing.T) {
	setViperOptions()

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

	t.Run("find", func(t *testing.T) {
		type called []string
		tests := []struct {
			name     string
			expected called
			flags    []string
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
		}

		cmdFn := func() (*cobra.Command, *called) {
			calls := new(called)
			svc := mock.NewSecretService()
			svc.GetSecretKeysFn = func(ctx context.Context, organizationID influxdb.ID) ([]string, error) {
				if !organizationID.Valid() {
					return []string{}, nil
				}
				*calls = []string{"k1", "k2", "k3"}
				return []string{}, nil
			}

			builder := newCmdSecretBuilder(fakeSVCFn(svc, nil), in(new(bytes.Buffer)), out(ioutil.Discard))
			cmd := builder.cmdFind()
			cmd.RunE = builder.cmdFindRunEFn
			return cmd, calls
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				defer addEnvVars(t, tt.envVars)()

				cmd, calls := cmdFn()
				cmd.SetArgs(tt.flags)

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

		cmdFn := func(expectedKey string) *cobra.Command {
			svc := mock.NewSecretService()
			svc.DeleteSecretFn = func(ctx context.Context, orgID influxdb.ID, ks ...string) error {
				if expectedKey != ks[0] {
					return fmt.Errorf("unexpected id:\n\twant= %s\n\tgot=  %s", expectedKey, ks[0])
				}
				return nil
			}

			builder := newCmdSecretBuilder(fakeSVCFn(svc, nil), out(ioutil.Discard))
			cmd := builder.cmdDelete()
			cmd.RunE = builder.cmdDeleteRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expectedKey)
				cmd.SetArgs(tt.flags)
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
				name:        "shorts",
				expectedKey: "key1",
				flags:       []string{"-o=" + orgID.String(), "-k=key1"},
			},
		}

		cmdFn := func(expectedKey string) *cobra.Command {
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

			builder := newCmdSecretBuilder(fakeSVCFn(svc, getSctFn), out(ioutil.Discard))
			cmd := builder.cmdUpdate()
			cmd.RunE = builder.cmdUpdateRunEFn
			return cmd
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				cmd := cmdFn(tt.expectedKey)
				cmd.SetArgs(tt.flags)
				require.NoError(t, cmd.Execute())
			}

			t.Run(tt.name, fn)
		}
	})

}
