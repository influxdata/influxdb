package launcher

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidFlags(t *testing.T) {
	t.Parallel()

	v2config := `
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
`

	v1config := `
reporting-disabled = false

# Bind address to use for the RPC service for backup and restore.
bind-address = "127.0.0.1:8088"

[http]
  flux-enabled = false

[data]
  index-version = "inmem"`

	tests := []struct {
		name   string
		config string
		want   []string
	}{
		{
			name:   "empty config",
			config: "",
			want:   []string(nil),
		},
		{
			name:   "v2 config",
			config: v2config,
			want:   []string(nil),
		},
		{
			name:   "v1 config",
			config: v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
		{
			name:   "mixed config",
			config: v2config + v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.config)
			v := viper.GetViper()
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(r))
			got := invalidFlags(v)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestErrInvalidFlags_ExitCode covers the status for the failure an operator is
// most likely to meet on an upgrade: a 1.x config file handed to a 2.x server.
// It is reported before the launcher exists, so it is one of the few statuses
// not pinned by Launcher.run.
func TestErrInvalidFlags_ExitCode(t *testing.T) {
	t.Parallel()

	err := errInvalidFlags([]string{"data.index-version"}, "/etc/influxdb/config.toml")
	require.Error(t, err)
	require.Equal(t, exit.CodeConfig, exit.Code(err),
		"a 1.x config file must exit %s: editing the file is the only fix, so a "+
			"supervisor set to stop retrying on a config error must not restart into it",
		exit.Name(exit.CodeConfig))
	require.ErrorContains(t, err, "data.index-version",
		"pinning a status must not disturb the message")
	require.ErrorContains(t, err, "/etc/influxdb/config.toml")
}

func TestInfluxdOpts_HealthAuthRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      HealthAuthMode
		hardening bool
		want      bool
	}{
		{name: "auto follows hardening on", mode: HealthAuthAuto, hardening: true, want: true},
		{name: "auto follows hardening off", mode: HealthAuthAuto, want: false},
		{name: "required ignores hardening on", mode: HealthAuthRequired, hardening: true, want: true},
		{name: "required ignores hardening off", mode: HealthAuthRequired, want: true},
		{
			// The opt-out this mode exists for: an operator who hardens
			// everything else keeps the /health and /ready bodies their
			// monitoring parses.
			name: "disabled ignores hardening on", mode: HealthAuthDisabled, hardening: true, want: false,
		},
		{name: "disabled ignores hardening off", mode: HealthAuthDisabled, want: false},
		{
			// Anything constructing InfluxdOpts directly -- tests, embedders --
			// leaves the mode at its zero value, which must behave like auto so
			// hardening keeps implying health auth for them.
			name: "zero value follows hardening on", mode: "", hardening: true, want: true,
		},
		{name: "zero value follows hardening off", mode: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := &InfluxdOpts{
				HardeningEnabled: tt.hardening,
				HealthAuthMode:   tt.mode,
			}
			assert.Equal(t, tt.want, o.healthAuthRequired())
			// The resolver is a pure read; it must not rewrite either input.
			assert.Equal(t, tt.hardening, o.HardeningEnabled)
			assert.Equal(t, tt.mode, o.HealthAuthMode)
		})
	}
}

// resolveOpts runs the real command wiring over args and returns the options it
// produced, stopping short of RunE so no server starts. The error is
// ParseFlags's, so a caller can assert on a rejected value.
func resolveOpts(t *testing.T, v *viper.Viper, args ...string) (*InfluxdOpts, error) {
	t.Helper()
	o := NewOpts(v)
	cmd, err := newInfluxdCommand(context.Background(), o)
	require.NoError(t, err)
	return o, cmd.ParseFlags(args)
}

// TestNewInfluxdCommand_HealthAuthMode covers the command line. The mode is a
// pflag.Value, so it is validated as it is parsed; it has no bare form, since
// nothing sets NoOptDefVal, and a bare --health-auth-mode consumes whatever
// follows it as the value.
func TestNewInfluxdCommand_HealthAuthMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		wantMode     HealthAuthMode
		wantRequired bool
		wantErr      string // non-empty: ParseFlags must fail and the error must contain it
	}{
		{
			name:         "hardening alone",
			args:         []string{"--hardening-enabled"},
			wantMode:     HealthAuthAuto,
			wantRequired: true,
		},
		{
			name:         "hardening with an explicit opt-out",
			args:         []string{"--hardening-enabled", "--health-auth-mode=disabled"},
			wantMode:     HealthAuthDisabled,
			wantRequired: false,
		},
		{
			name:         "hardening with an explicit opt-in",
			args:         []string{"--hardening-enabled", "--health-auth-mode=required"},
			wantMode:     HealthAuthRequired,
			wantRequired: true,
		},
		{
			name:         "required on its own",
			args:         []string{"--health-auth-mode=required"},
			wantMode:     HealthAuthRequired,
			wantRequired: true,
		},
		{
			name:         "disabled on its own",
			args:         []string{"--health-auth-mode=disabled"},
			wantMode:     HealthAuthDisabled,
			wantRequired: false,
		},
		{
			name:         "space-separated value",
			args:         []string{"--health-auth-mode", "required"},
			wantMode:     HealthAuthRequired,
			wantRequired: true,
		},
		{
			name:         "no hardening flags at all",
			wantMode:     HealthAuthAuto,
			wantRequired: false,
		},
		{
			name:    "unknown value is rejected",
			args:    []string{"--health-auth-mode=bogus"},
			wantErr: "expected auto, required, or disabled",
		},
		{
			name:    "values are case-sensitive",
			args:    []string{"--health-auth-mode=Required"},
			wantErr: "expected auto, required, or disabled",
		},
		{
			name:    "bare flag as the last argument",
			args:    []string{"--health-auth-mode"},
			wantErr: "flag needs an argument: --health-auth-mode",
		},
		{
			name:    "bare flag swallows the next argument",
			args:    []string{"--health-auth-mode", "--hardening-enabled"},
			wantErr: `invalid argument "--hardening-enabled" for "--health-auth-mode" flag`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o, err := resolveOpts(t, viper.New(), tt.args...)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, o.HealthAuthMode)
			assert.Equal(t, tt.wantRequired, o.healthAuthRequired())
		})
	}
}

// TestNewInfluxdCommand_HealthAuthModeFromEnv covers the environment. INFLUXD_*
// is the usual way to configure a containerized influxd, so an opt-out that only
// worked on the command line would not reach the deployments most likely to run
// with hardening on. Not parallel: t.Setenv forbids it.
func TestNewInfluxdCommand_HealthAuthModeFromEnv(t *testing.T) {
	t.Setenv("INFLUXD_HEALTH_AUTH_MODE", "disabled")

	o, err := resolveOpts(t, viper.New(), "--hardening-enabled")
	require.NoError(t, err)
	assert.Equal(t, HealthAuthDisabled, o.HealthAuthMode)
	assert.False(t, o.healthAuthRequired())
}

// TestPrintConfig_RejectsPositionalArgs covers the half of a malformed command
// line that cobra does not route through FlagErrorFunc. `print-config --bogus`
// is a flag error and exits EX_USAGE; an Args violation is handed back to the
// caller directly, so without cli.UsageArgs the same operator mistake would
// exit 1 on the same command.
func TestPrintConfig_RejectsPositionalArgs(t *testing.T) {
	t.Parallel()

	cmd, err := newInfluxdCommand(context.Background(), NewOpts(viper.New()))
	require.NoError(t, err)

	// Args are validated before RunE, so nothing is printed and no options are
	// resolved.
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"print-config", "bogus"})

	err = cmd.Execute()
	require.Error(t, err, "print-config takes no positional arguments")
	require.Equal(t, exit.CodeUsage, exit.Code(err),
		"a wrong command line must exit %s however it is wrong", exit.Name(exit.CodeUsage))
}

// TestNewInfluxdCommand_HealthAuthModeFromConfigFile covers the third source.
func TestNewInfluxdCommand_HealthAuthModeFromConfigFile(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.SetConfigType("toml")
	require.NoError(t, v.ReadConfig(strings.NewReader("health-auth-mode = \"disabled\"\n")))

	o, err := resolveOpts(t, v, "--hardening-enabled")
	require.NoError(t, err)
	assert.Equal(t, HealthAuthDisabled, o.HealthAuthMode)
	assert.False(t, o.healthAuthRequired())
}

// TestNewInfluxdCommand_HealthAuthModeInvalidEnvIgnored pins a kit-wide
// behavior rather than one this option chose: cli.BindOptions drops an
// environment or config-file value that the option's Set rejects, without a
// log line (kit/cli/viper.go, the pflag.Value case), just as it does for *bool
// and *zapcore.Level. So a value shaped for the old bool flag leaves the mode at
// auto, and under --hardening-enabled health auth is enforced despite it. Only
// the command line is strict. If kit/cli is ever tightened to return the error,
// this test should fail and be deleted.
func TestNewInfluxdCommand_HealthAuthModeInvalidEnvIgnored(t *testing.T) {
	t.Setenv("INFLUXD_HEALTH_AUTH_MODE", "false")

	o, err := resolveOpts(t, viper.New(), "--hardening-enabled")
	require.NoError(t, err)
	assert.Equal(t, HealthAuthAuto, o.HealthAuthMode)
	assert.True(t, o.healthAuthRequired())
}

// TestNewInfluxdCommand_StartupErrorLinger covers every way the option can be
// supplied. It is a duration, which is the part worth pinning: viper's
// cast.ToDurationE reads "30s" from a config file, and the derived env var name
// is generated rather than declared, so nothing else in the tree would catch a
// rename.
func TestNewInfluxdCommand_StartupErrorLinger(t *testing.T) {
	t.Parallel()

	t.Run("absent", func(t *testing.T) {
		t.Parallel()

		o, err := resolveOpts(t, viper.New())
		assert.NoError(t, err)
		assert.Zero(t, o.StartupErrorLinger, "the default must exit immediately, as before")
	})

	t.Run("command line", func(t *testing.T) {
		t.Parallel()

		o, err := resolveOpts(t, viper.New(), "--startup-error-linger=30s")
		assert.NoError(t, err)
		assert.Equal(t, 30*time.Second, o.StartupErrorLinger)
	})

	t.Run("config file", func(t *testing.T) {
		t.Parallel()

		v := viper.New()
		v.SetConfigType("yaml")
		require.NoError(t, v.ReadConfig(strings.NewReader("startup-error-linger: 1m\n")))

		o, err := resolveOpts(t, v)
		assert.NoError(t, err)
		assert.Equal(t, time.Minute, o.StartupErrorLinger)
	})
}

// TestNewInfluxdCommand_StartupErrorLingerFromEnv pins the derived env var.
// INFLUXD_* is how a containerized influxd is configured, which is also where a
// failed startup is hardest to observe. Not parallel: t.Setenv forbids it.
func TestNewInfluxdCommand_StartupErrorLingerFromEnv(t *testing.T) {
	t.Setenv("INFLUXD_STARTUP_ERROR_LINGER", "45s")

	o, err := resolveOpts(t, viper.New())
	assert.NoError(t, err)
	assert.Equal(t, 45*time.Second, o.StartupErrorLinger)
}

// printConfig runs the real print-config subcommand over args and returns the
// YAML it wrote, which is what an operator redirects into a config file.
func printConfig(t *testing.T, args ...string) string {
	t.Helper()
	o := NewOpts(viper.New())
	cmd, err := newInfluxdCommand(context.Background(), o)
	require.NoError(t, err)

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"print-config"}, args...))
	require.NoError(t, cmd.Execute())

	// print-config carries a Deprecated notice, and cobra's OutOrStderr resolves
	// to the writer SetOut installed, so the notice lands ahead of the YAML. A
	// real operator redirecting stdout to a file does not get it; strip it here
	// so the round trip below fails only for reasons about the options.
	printed := out.String()
	if notice, rest, found := strings.Cut(printed, "\n"); found && strings.HasPrefix(notice, "Command ") {
		printed = rest
	}
	return printed
}

// TestPrintConfig_ReportsStartupErrorLinger keeps the option discoverable: an
// operator finds it by reading what print-config emits.
func TestPrintConfig_ReportsStartupErrorLinger(t *testing.T) {
	t.Parallel()

	got := printConfig(t)
	assert.Contains(t, got, "startup-error-linger: 0s")
}
