package launcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthAuthMode_Set(t *testing.T) {
	t.Parallel()

	t.Run("accepts the three documented values", func(t *testing.T) {
		t.Parallel()
		for _, want := range []HealthAuthMode{HealthAuthAuto, HealthAuthRequired, HealthAuthDisabled} {
			var m HealthAuthMode
			require.NoError(t, m.Set(want.String()))
			assert.Equal(t, want, m)
			assert.Equal(t, string(want), m.String(), "String must round-trip through Set")
		}
	})

	t.Run("rejects everything else and leaves the receiver alone", func(t *testing.T) {
		t.Parallel()
		// true/false are the values an operator used to the old bool flag will
		// reach for, Required pins case-sensitivity, and the empty string is
		// what --health-auth-mode="" produces.
		for _, bad := range []string{"", "true", "false", "Required", "AUTO", "yes", "on"} {
			m := HealthAuthRequired
			err := m.Set(bad)
			require.Errorf(t, err, "Set(%q) should fail", bad)
			assert.Contains(t, err.Error(), "expected auto, required, or disabled",
				"the error must name the accepted values")
			assert.Equal(t, HealthAuthRequired, m, "a rejected Set(%q) must not modify the receiver", bad)
		}
	})

	t.Run("Type is set for help output", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, HealthAuthMode("").Type())
	})
}
