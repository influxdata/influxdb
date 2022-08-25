package feature

import (
	"context"
	"strings"

	"github.com/opentracing/opentracing-go"
)

type contextKey string

const featureContextKey contextKey = "influx/feature/v1"

// Flagger returns flag values.
type Flagger interface {
	// Flags returns a map of flag keys to flag values.
	//
	// If an authorization is present on the context, it may be used to compute flag
	// values according to the affiliated user ID and its organization and other mappings.
	// Otherwise, they should be computed generally or return a default.
	//
	// One or more flags may be provided to restrict the results.
	// Otherwise, all flags should be computed.
	Flags(context.Context, ...Flag) (map[string]interface{}, error)
}

// Annotate the context with a map computed of computed flags.
func Annotate(ctx context.Context, f Flagger, flags ...Flag) (context.Context, error) {
	computed, err := f.Flags(ctx, flags...)
	if err != nil {
		return nil, err
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		for k, v := range computed {
			span.LogKV(k, v)
		}
	}

	return context.WithValue(ctx, featureContextKey, computed), nil
}

// FlagsFromContext returns the map of flags attached to the context
// by Annotate, or nil if none is found.
func FlagsFromContext(ctx context.Context) map[string]interface{} {
	v, ok := ctx.Value(featureContextKey).(map[string]interface{})
	if !ok {
		return nil
	}
	return v
}

type ByKeyFn func(string) (Flag, bool)

// ExposedFlagsFromContext returns the filtered map of exposed  flags attached
// to the context by Annotate, or nil if none is found.
func ExposedFlagsFromContext(ctx context.Context, byKey ByKeyFn) map[string]interface{} {
	m := FlagsFromContext(ctx)
	if m == nil {
		return nil
	}

	filtered := make(map[string]interface{})
	for k, v := range m {
		if flag, found := byKey(k); found && flag.Expose() {
			filtered[k] = v
		}
	}

	return filtered
}

// Lifetime represents the intended lifetime of the feature flag.
//
// The zero value is Temporary, the most common case, but Permanent
// is included to mark special cases where a flag is not intended
// to be removed, e.g. enabling debug tracing for an organization.
//
// TODO(gavincabbage): This may become a stale date, which can then
//
//	be used to trigger a notification to the contact when the flag
//	has become stale, to encourage flag cleanup.
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

type defaultFlagger struct{}

// DefaultFlagger returns a flagger that always returns default values.
func DefaultFlagger() Flagger {
	return &defaultFlagger{}
}

// Flags returns a map of default values. It never returns an error.
func (*defaultFlagger) Flags(_ context.Context, flags ...Flag) (map[string]interface{}, error) {
	if len(flags) == 0 {
		flags = Flags()
	}

	m := make(map[string]interface{}, len(flags))
	for _, flag := range flags {
		m[flag.Key()] = flag.Default()
	}

	return m, nil
}

// Flags returns all feature flags.
func Flags() []Flag {
	return all
}

// ByKey returns the Flag corresponding to the given key.
func ByKey(k string) (Flag, bool) {
	v, found := byKey[k]
	return v, found
}
