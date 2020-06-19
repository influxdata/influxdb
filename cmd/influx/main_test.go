package main

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influx/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_influx_cmd(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		envVars  map[string]string
		expected globalFlags
	}{
		{
			name:    "all full length flags set",
			args:    []string{"--token=TOKEN", "--host=HOST", "--skip-verify=true"},
			envVars: envVarsZeroMap,
			expected: globalFlags{
				Config: config.Config{
					Token: "TOKEN",
					Host:  "HOST",
				},
				skipVerify: true,
			},
		},
		{
			name:    "token p flag set",
			args:    []string{"-t=TOKEN", "--host=HOST", "--skip-verify=true"},
			envVars: envVarsZeroMap,
			expected: globalFlags{
				Config: config.Config{
					Token: "TOKEN",
					Host:  "HOST",
				},
				skipVerify: true,
			},
		},
		{
			name: "env vars set",
			args: []string{"--skip-verify=true"},
			envVars: map[string]string{
				"INFLUX_TOKEN": "TOKEN",
				"INFLUX_HOST":  "HOST",
			},
			expected: globalFlags{
				Config: config.Config{
					Token: "TOKEN",
					Host:  "HOST",
				},
				skipVerify: true,
			},
		},
		{
			name: "env vars and flags set",
			args: []string{"--token=flag-token", "--host=flag-host"},
			envVars: map[string]string{
				"INFLUX_TOKEN": "TOKEN",
				"INFLUX_HOST":  "HOST",
			},
			expected: globalFlags{
				Config: config.Config{
					Token: "flag-token",
					Host:  "flag-host",
				},
				skipVerify: false,
			},
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			defer addEnvVars(t, tt.envVars)()

			builder := newInfluxCmdBuilder(
				in(new(bytes.Buffer)),
				out(ioutil.Discard),
				err(ioutil.Discard),
				runEMiddlware(func(fn cobraRunEFn) cobraRunEFn { return fn }),
			)

			flagCapture := new(globalFlags)
			influxCmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				flagCapture = f
				return &cobra.Command{Use: "foo"}
			})

			influxCmd.SetArgs(append([]string{"foo"}, tt.args...))

			require.NoError(t, influxCmd.Execute())

			assert.Equal(t, tt.expected.Host, flagCapture.Host)
			assert.Equal(t, tt.expected.Token, flagCapture.Token)
			assert.Equal(t, tt.expected.skipVerify, flagCapture.skipVerify)
		}

		t.Run(tt.name, fn)
	}
}
