package feature

import (
	"context"

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

type testingFlagger struct{}

// TestingFlagger returns a flagger that always returns testing values.
func TestingFlagger() Flagger {
	return &testingFlagger{}
}

// Flags returns a map of testing values. It never returns an error.
func (*testingFlagger) Flags(_ context.Context, flags ...Flag) (map[string]interface{}, error) {
	if len(flags) == 0 {
		flags = Flags()
	}

	m := make(map[string]interface{}, len(flags))
	for _, flag := range flags {
		m[flag.Key()] = flag.TestingDefault()
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
