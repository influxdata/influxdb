package main

import (
	"bytes"
	"context"
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
