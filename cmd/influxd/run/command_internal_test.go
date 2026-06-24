package run

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestCommand_logEnvVarDiagnostics_UnmatchedGatesWarn pins the one behavior
// worth protecting: a warn is emitted only when the namespace contains
// unmatched env vars. Without this, the diagnostic would warn on every
// startup.
func TestCommand_logEnvVarDiagnostics_UnmatchedGatesWarn(t *testing.T) {
	tests := []struct {
		name          string
		environ       []string
		wantWarnCount int
	}{
		{"no unmatched vars, no warn", []string{"INFLUXDB_FOO=1"}, 0},
		{"unmatched var, one warn", []string{"INFLUXDB_TYPO=1"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.WarnLevel)
			cmd := &Command{Environ: func() []string { return tc.environ }}
			cmd.logEnvVarDiagnostics(zap.New(core), []string{"INFLUXDB_FOO"})
			require.Equal(t, tc.wantWarnCount, logs.Len())
		})
	}
}
