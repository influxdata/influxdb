package toml

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplyEnvOverrides_CSVLeafDoesNotPolluteIndexedVars is a regression test
// for a bug where the comma-separated unindexed env override path for leaf-type
// slices added the unindexed prefix to IndexedVars, which is documented to
// contain only subscripted names. Pollution was observable through slice
// growth error messages that cited an unindexed var as if it were indexed.
func TestApplyEnvOverrides_CSVLeafDoesNotPolluteIndexedVars(t *testing.T) {
	type config struct {
		Vals []string `toml:"vals"`
	}

	env := func(s string) string {
		if s == "X_VALS" {
			return "a,b,c"
		}
		return ""
	}

	var c config
	result, err := applyEnvOverrides(env, "X", reflect.ValueOf(&c), "")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c"}, c.Vals)
	require.True(t, result.Applied)
	require.Equal(t, []string{"X_VALS"}, result.AllVars)
	require.Empty(t, result.IndexedVars, "unindexed env var must not appear in IndexedVars")
}
