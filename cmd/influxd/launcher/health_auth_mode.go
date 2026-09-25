package launcher

import (
	"fmt"

	"github.com/spf13/pflag"
)

// HealthAuthMode says whether /health and /ready withhold check detail from
// callers who cannot prove operator permissions. See
// http.HealthReadyHandler.SetHealthAuthRequired for what is withheld.
type HealthAuthMode string

const (
	// HealthAuthAuto follows --hardening-enabled: detail is gated when hardening
	// is on and served to everyone when it is off.
	HealthAuthAuto HealthAuthMode = "auto"
	// HealthAuthRequired gates detail regardless of --hardening-enabled.
	HealthAuthRequired HealthAuthMode = "required"
	// HealthAuthDisabled serves full detail to every caller regardless of
	// --hardening-enabled, for operators whose monitoring parses the bodies.
	HealthAuthDisabled HealthAuthMode = "disabled"
)

// Compile-time check that *HealthAuthMode implements pflag.Value so it can be
// registered directly as a command-line flag.
var _ pflag.Value = (*HealthAuthMode)(nil)

func (m HealthAuthMode) String() string {
	return string(m)
}

// Set satisfies pflag.Value. Only the three documented values are accepted;
// the error names them so a typo on the command line is self-explanatory.
func (m *HealthAuthMode) Set(s string) error {
	switch v := HealthAuthMode(s); v {
	case HealthAuthAuto, HealthAuthRequired, HealthAuthDisabled:
		*m = v
		return nil
	}
	return fmt.Errorf("unknown health auth mode %q; expected auto, required, or disabled", s)
}

// Type is the placeholder pflag prints after the flag name in --help. It must
// not be one of the names viper's flag-cast switch special-cases ("bool",
// "int", "stringSlice", ...), which an enumeration is not.
func (m HealthAuthMode) Type() string {
	return "auto|required|disabled"
}
