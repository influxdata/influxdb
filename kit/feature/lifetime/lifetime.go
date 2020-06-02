package lifetime

import (
	"strings"
)

// Lifetime represents the intended lifetime of the feature flag.
//
// The zero value is Temporary, the most common case, but Permanent
// is included to mark special cases where a flag is not intended
// to be removed, e.g. enabling debug tracing for an organization.
//
// TODO(gavincabbage): This may become a stale date, which can then
// 		be used to trigger a notification to the contact when the flag
//		has become stale, to encourage flag cleanup.
type Lifetime int

const (
	// Temporary indicates a flag is intended to be removed after a feature is no longer in development.
	Temporary Lifetime = iota
	// Permanent indicates a flag is not intended to be removed.
	Permanent
)

// UnmarshalYAML implements yaml.Unmarshaler and interprets a case-insensitive text
// representation as a lifetime constant.
func (l *Lifetime) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	switch strings.ToLower(s) {
	case "permanent":
		*l = Permanent
	default:
		*l = Temporary
	}

	return nil
}
