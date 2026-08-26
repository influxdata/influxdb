package launcher

import (
	"bytes"
	"context"
	"strings"
	"testing"

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

func TestInfluxdOpts_ApplyHardeningImplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hardening  bool
		healthAuth bool
		explicit   bool
		want       bool
	}{
		{
			name:      "hardening alone implies health auth",
			hardening: true,
			want:      true,
		},
		{
			// The opt-out this whole mechanism exists for: an operator who
			// hardens everything else keeps the /health and /ready bodies
			// their monitoring parses.
			name:      "explicit false overrides the implication",
			hardening: true,
			explicit:  true,
			want:      false,
		},
		{
			name:       "explicit true agrees with the implication",
			hardening:  true,
			healthAuth: true,
			explicit:   true,
			want:       true,
		},
		{
			name: "neither flag leaves it off",
			want: false,
		},
		{
			name:       "health auth alone does not need hardening",
			healthAuth: true,
			explicit:   true,
			want:       true,
		},
		{
			// Anything constructing InfluxdOpts directly -- tests, embedders --
			// never sets the explicit bit, so the implication stays
			// unconditional for them.
			name:      "implication applies when explicitness is unknown",
			hardening: true,
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := &InfluxdOpts{
				HardeningEnabled:     tt.hardening,
				HealthAuthEnabled:    tt.healthAuth,
				HealthAuthEnabledSet: tt.explicit,
			}
			o.applyHardeningImplications()
			assert.Equal(t, tt.want, o.HealthAuthEnabled)
			// The implication reads HardeningEnabled; it must not rewrite it.
			assert.Equal(t, tt.hardening, o.HardeningEnabled)
		})
	}
}

// resolveOpts runs the real command wiring over args and returns the options it
// produced, stopping short of RunE so no server starts. ParseFlags populates the
// flag values and PreRunE records whether the operator named the flag, which is
// the half of the answer the config file and environment cannot supply, and then
// applies the implication.
//
// Deliberately no applyHardeningImplications call of its own: PreRunE owns that
// now, and doing it here as well would let these tests keep passing if the
// wiring stopped doing it -- which is exactly the defect print-config had.
func resolveOpts(t *testing.T, v *viper.Viper, args ...string) *InfluxdOpts {
	t.Helper()
	o := NewOpts(v)
	cmd, err := newInfluxdCommand(context.Background(), o)
	require.NoError(t, err)
	require.NoError(t, cmd.ParseFlags(args))
	require.NoError(t, cmd.PreRunE(cmd, nil))
	return o
}

// TestNewInfluxdCommand_HealthAuthOptOut covers the command-line half: viper
// cannot tell an unset bool flag from one set to false, so the wiring consults
// pflag's Changed. Without that, --health-auth-enabled=false is silently
// overwritten by --hardening-enabled.
func TestNewInfluxdCommand_HealthAuthOptOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want bool
	}{
		{
			name: "hardening alone",
			args: []string{"--hardening-enabled"},
			want: true,
		},
		{
			name: "hardening with an explicit opt-out",
			args: []string{"--hardening-enabled", "--health-auth-enabled=false"},
			want: false,
		},
		{
			name: "hardening with an explicit opt-in",
			args: []string{"--hardening-enabled", "--health-auth-enabled=true"},
			want: true,
		},
		{
			name: "health auth on its own",
			args: []string{"--health-auth-enabled"},
			want: true,
		},
		{
			name: "no hardening flags at all",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			o := resolveOpts(t, viper.New(), tt.args...)
			assert.Equal(t, tt.want, o.HealthAuthEnabled)
		})
	}
}

// TestNewInfluxdCommand_HealthAuthOptOutFromEnv covers the environment half.
// INFLUXD_* is the usual way to configure a containerized influxd, so an
// opt-out that only worked on the command line would not reach the deployments
// most likely to run with hardening on. Not parallel: t.Setenv forbids it.
func TestNewInfluxdCommand_HealthAuthOptOutFromEnv(t *testing.T) {
	t.Setenv("INFLUXD_HEALTH_AUTH_ENABLED", "false")

	o := resolveOpts(t, viper.New(), "--hardening-enabled")
	assert.False(t, o.HealthAuthEnabled)
	assert.True(t, o.HealthAuthEnabledSet, "the environment supplied a value")
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

// TestPrintConfig_ReportsHardeningImplications pins that print-config resolves
// the options the way the server does. print-config exists to answer "what would
// the server do", and the implication is part of that answer.
func TestPrintConfig_ReportsHardeningImplications(t *testing.T) {
	t.Parallel()

	got := printConfig(t, "--hardening-enabled")
	assert.Contains(t, got, "health-auth-enabled: true",
		"a hardened server gates check detail, so print-config must say so")
}

// TestPrintConfig_RoundTripPreservesHardening is the failure that makes the
// above more than cosmetic. The documented workflow is to redirect print-config
// into a config file, and every key it prints is then a key the operator has
// "supplied" -- which is what suppresses the implication. Printing the resolved
// value is what makes the round trip a fixed point instead of a silent downgrade
// from a gated /health to an ungated one.
func TestPrintConfig_RoundTripPreservesHardening(t *testing.T) {
	t.Parallel()

	printed := printConfig(t, "--hardening-enabled")

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(printed)))

	// No --hardening-enabled on the command line this time: it is in the file,
	// exactly as it would be for a server started from the printed config.
	o := resolveOpts(t, v)
	require.True(t, o.HardeningEnabled, "the printed config still asks for hardening")
	assert.True(t, o.HealthAuthEnabled,
		"a config file printed from a hardened server must still describe a hardened server")

	// And the round trip is stable: printing the resolved options again yields
	// the same answer rather than decaying one generation at a time.
	assert.Contains(t, printed, "hardening-enabled: true")
}

// TestPrintConfig_RoundTripPreservesOptOut is the converse, and the reason the
// fix is "print what was resolved" rather than "ignore the printed key": an
// operator who opted out explicitly must still be opted out after a round trip.
func TestPrintConfig_RoundTripPreservesOptOut(t *testing.T) {
	t.Parallel()

	printed := printConfig(t, "--hardening-enabled", "--health-auth-enabled=false")
	assert.Contains(t, printed, "health-auth-enabled: false")

	v := viper.New()
	v.SetConfigType("yaml")
	require.NoError(t, v.ReadConfig(strings.NewReader(printed)))

	o := resolveOpts(t, v)
	require.True(t, o.HardeningEnabled)
	assert.False(t, o.HealthAuthEnabled, "an explicit opt-out survives the round trip")
}

// TestNewInfluxdCommand_HealthAuthOptOutFromConfigFile covers the third source.
// The probe has to read viper before BindOptions binds the flag over the top:
// once bound, viper falls back to the flag's default and reports every key as
// set, which would make the implication unconditional again.
func TestNewInfluxdCommand_HealthAuthOptOutFromConfigFile(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.SetConfigType("toml")
	require.NoError(t, v.ReadConfig(strings.NewReader("health-auth-enabled = false\n")))

	o := resolveOpts(t, v, "--hardening-enabled")
	assert.False(t, o.HealthAuthEnabled)
	assert.True(t, o.HealthAuthEnabledSet, "the config file supplied a value")
}
