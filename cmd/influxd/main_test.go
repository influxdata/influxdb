package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// TestRootCommand_FlagErrorStatus pins which commands answer a malformed
// command line with a sysexits status and which are left alone.
//
// Cobra resolves FlagErrorFunc by walking up to the nearest ancestor that has
// one, so the server's opt-in reaches every subcommand below the root unless
// each is given a pass-through of its own. A subcommand acquiring a status it
// never opted into is exactly what the exit-code work promises will not happen
// -- see "What did not change" in EXIT_CODES.md -- and nothing but a test keeps
// that true as commands are added.
func TestRootCommand_FlagErrorStatus(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want int
	}{
		// Opted in: the server itself, under either spelling, and the command
		// that reports what the server would do.
		{"server", []string{"--bogus-flag"}, exit.CodeUsage},
		{"run", []string{"run", "--bogus-flag"}, exit.CodeUsage},
		{"print-config", []string{"print-config", "--bogus-flag"}, exit.CodeUsage},

		// Not opted in. The nested case is the one inheritance would reach
		// furthest into: dump-wal has no func of its own, and the walk has to
		// stop at inspect rather than carry on to the root.
		{"inspect", []string{"inspect", "--bogus-flag"}, exit.CodeGeneric},
		{"inspect subcommand", []string{"inspect", "dump-wal", "--bogus-flag"}, exit.CodeGeneric},
		{"upgrade", []string{"upgrade", "--bogus-flag"}, exit.CodeGeneric},
		{"downgrade", []string{"downgrade", "--bogus-flag"}, exit.CodeGeneric},
		{"recovery", []string{"recovery", "--bogus-flag"}, exit.CodeGeneric},
		{"version", []string{"version", "--bogus-flag"}, exit.CodeGeneric},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A command tree of its own per case: cobra records parsed flags on
			// the command, and viper's bindings are per instance.
			cmd, err := newRootCommand(context.Background(), viper.New())
			require.NoError(t, err)

			// Every case fails in ParseFlags, before RunE, so no server starts.
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(tt.args)

			err = cmd.Execute()
			require.Error(t, err, "an unknown flag must not be accepted")
			require.Equal(t, tt.want, exit.Code(err),
				"%v must exit %s, got %s", tt.args, exit.Name(tt.want), exit.Name(exit.Code(err)))
		})
	}
}

// TestRootCommand_PreDispatchConfigStatus pins the status influxd reports for a
// failure that happens while newRootCommand is assembling the tree, before
// cobra has looked at the command line at all.
//
// TestRootCommand_FlagErrorStatus above cannot see any of this: it builds every
// tree from a bare viper with no config file, so each of its cases fails in
// ParseFlags, after a command has been selected. The config file and the
// INFLUXD_* environment are read during construction instead, which is why a
// broken one exits EX_CONFIG whatever was typed -- `influxd inspect` and
// `influxd version` included, neither of which will run. That is the documented
// exception to the subcommand guarantee, and the two are one edit apart: a
// change that made construction lazy, or that stopped classifying these, would
// silently move the boundary. See "Failures before a command is selected" in
// EXIT_CODES.md.
//
// Not parallel, here or in the subtests: t.Setenv forbids it.
func TestRootCommand_PreDispatchConfigStatus(t *testing.T) {
	// All three cases want the same status, so each also names a fragment of
	// the message its own site produces. Without that they would pass for any
	// EX_CONFIG at all, and the ways to fail this way before dispatch are
	// close enough together that a case could quietly stop testing what it was
	// written for.
	tests := []struct {
		name     string
		config   string            // written to a config.toml the case points at
		env      map[string]string // INFLUXD_* values set for the case
		want     int
		contains string
	}{
		{
			name:     "config file that will not parse",
			config:   "[meta\ndir = \"/var/lib/influxdb/meta\"\n",
			want:     exit.CodeConfig,
			contains: "failed to load config file",
		},
		{
			name:     "1.x keys in a 2.x config file",
			config:   "[meta]\ndir = \"/var/lib/influxdb/meta\"\n",
			want:     exit.CodeConfig,
			contains: "found flags from an InfluxDB 1.x configuration",
		},
		{
			name:     "environment value the option will not accept",
			env:      map[string]string{"INFLUXD_LOG_LEVEL": "not-a-level"},
			want:     exit.CodeConfig,
			contains: `invalid value "not-a-level" for "log-level" from the environment`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Every case points at a config.toml of its own, written or not:
			// initializeConfig ignores a file that is absent, and naming one
			// keeps the isolation that makes the environment case
			// deterministic -- with CONFIG_PATH unset influxd searches the
			// working directory, so a config file appearing next to this test
			// would otherwise decide the result.
			//
			// A path rather than the temp directory itself because
			// initializeConfig switches on the extension: a directory whose
			// name happens to contain a dot is taken for a config file, and
			// failing to read it is also EX_CONFIG, so a case named like this
			// one's neighbour would pass for the wrong reason.
			configPath := filepath.Join(t.TempDir(), "config.toml")
			if tt.config != "" {
				require.NoError(t, os.WriteFile(configPath, []byte(tt.config), 0o600))
			}
			t.Setenv("INFLUXD_CONFIG_PATH", configPath)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			// The tree never assembles, so there is no command to execute and
			// nothing for a FlagErrorFunc to have a say in. main reports this
			// error through handleErr, which exits with exit.Code of it.
			_, err := newRootCommand(context.Background(), viper.New())
			require.Error(t, err, "a configuration influxd cannot read must not yield a command tree")
			require.ErrorContains(t, err, tt.contains, "the case must fail where it means to")
			require.Equal(t, tt.want, exit.Code(err),
				"must exit %s, got %s", exit.Name(tt.want), exit.Name(exit.Code(err)))
		})
	}
}
