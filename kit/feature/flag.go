//go:generate go run ./_codegen/main.go --in ../../flags.yml --out ./list.go

package feature

import (
	"context"
	"fmt"
)

// Flag represents a generic feature flag with a key and a default.
type Flag interface {
	// Key returns the programmatic backend identifier for the flag.
	Key() string
	// Default returns the type-agnostic zero value for the flag.
	// Type-specific flag implementations may expose a typed default
	// (e.g. BoolFlag includes a boolean Default field).
	Default() interface{}
	// Expose the flag.
	Expose() bool
}

// MakeFlag constructs a Flag. The concrete implementation is inferred from the provided default.
func MakeFlag(name, key, owner string, defaultValue interface{}, lifetime Lifetime, expose bool) Flag {
	if v, ok := defaultValue.(int); ok {
		defaultValue = int32(v)
	}
	b := MakeBase(name, key, owner, defaultValue, lifetime, expose)
	switch v := defaultValue.(type) {
	case bool:
		return BoolFlag{b, v}
	case float64:
		return FloatFlag{b, v}
	case int32:
		return IntFlag{b, v}
	case string:
		return StringFlag{b, v}
	default:
		return StringFlag{b, fmt.Sprintf("%v", v)}
	}
}

// flag base type.
type Base struct {
	// name of the flag.
	name string
	// key is the programmatic backend identifier for the flag.
	key string
	// defaultValue for the flag.
	defaultValue interface{}
	// owner is an individual or team responsible for the flag.
	owner string
	// lifetime of the feature flag.
	lifetime Lifetime
	// expose the flag.
	expose bool
}

var _ Flag = Base{}

// MakeBase constructs a flag flag.
func MakeBase(name, key, owner string, defaultValue interface{}, lifetime Lifetime, expose bool) Base {
	return Base{
		name:         name,
		key:          key,
		owner:        owner,
		defaultValue: defaultValue,
		lifetime:     lifetime,
		expose:       expose,
	}
}

// Key returns the programmatic backend identifier for the flag.
func (f Base) Key() string {
	return f.key
}

// Default returns the type-agnostic zero value for the flag.
func (f Base) Default() interface{} {
	return f.defaultValue
}

// Expose the flag.
func (f Base) Expose() bool {
	return f.expose
}

func (f Base) value(ctx context.Context, flagger ...Flagger) (interface{}, bool) {
	var (
		m  map[string]interface{}
		ok bool
	)
	if len(flagger) < 1 {
		m, ok = ctx.Value(featureContextKey).(map[string]interface{})
	} else {
		var err error
		m, err = flagger[0].Flags(ctx, f)
		ok = err == nil
	}
	if !ok {
		return nil, false
	}

	v, ok := m[f.Key()]
	if !ok {
		return nil, false
	}

	return v, true
}

// StringFlag implements Flag for string values.
type StringFlag struct {
	Base
	defaultString string
}

var _ Flag = StringFlag{}

// MakeStringFlag returns a string flag with the given Base and default.
func MakeStringFlag(name, key, owner string, defaultValue string, lifetime Lifetime, expose bool) StringFlag {
	b := MakeBase(name, key, owner, defaultValue, lifetime, expose)
	return StringFlag{b, defaultValue}
}

// String value of the flag on the request context.
func (f StringFlag) String(ctx context.Context, flagger ...Flagger) string {
	i, ok := f.value(ctx, flagger...)
	if !ok {
		return f.defaultString
	}
	s, ok := i.(string)
	if !ok {
		return f.defaultString
	}
	return s
}

// FloatFlag implements Flag for float values.
type FloatFlag struct {
	Base
	defaultFloat float64
}

var _ Flag = FloatFlag{}

// MakeFloatFlag returns a string flag with the given Base and default.
func MakeFloatFlag(name, key, owner string, defaultValue float64, lifetime Lifetime, expose bool) FloatFlag {
	b := MakeBase(name, key, owner, defaultValue, lifetime, expose)
	return FloatFlag{b, defaultValue}
}

// Float value of the flag on the request context.
func (f FloatFlag) Float(ctx context.Context, flagger ...Flagger) float64 {
	i, ok := f.value(ctx, flagger...)
	if !ok {
		return f.defaultFloat
	}
	v, ok := i.(float64)
	if !ok {
		return f.defaultFloat
	}
	return v
}

// IntFlag implements Flag for integer values.
type IntFlag struct {
	Base
	defaultInt int32
}

var _ Flag = IntFlag{}

// MakeIntFlag returns a string flag with the given Base and default.
func MakeIntFlag(name, key, owner string, defaultValue int32, lifetime Lifetime, expose bool) IntFlag {
	b := MakeBase(name, key, owner, defaultValue, lifetime, expose)
	return IntFlag{b, defaultValue}
}

// Int value of the flag on the request context.
func (f IntFlag) Int(ctx context.Context, flagger ...Flagger) int32 {
	i, ok := f.value(ctx, flagger...)
	if !ok {
		return f.defaultInt
	}
	v, ok := i.(int32)
	if !ok {
		return f.defaultInt
	}
	return v
}

// BoolFlag implements Flag for boolean values.
type BoolFlag struct {
	Base
	defaultBool bool
}

var _ Flag = BoolFlag{}

// MakeBoolFlag returns a string flag with the given Base and default.
func MakeBoolFlag(name, key, owner string, defaultValue bool, lifetime Lifetime, expose bool) BoolFlag {
	b := MakeBase(name, key, owner, defaultValue, lifetime, expose)
	return BoolFlag{b, defaultValue}
}

// Enabled indicates whether flag is true or false on the request context.
func (f BoolFlag) Enabled(ctx context.Context, flagger ...Flagger) bool {
	i, ok := f.value(ctx, flagger...)
	if !ok {
		return f.defaultBool
	}
	v, ok := i.(bool)
	if !ok {
		return f.defaultBool
	}
	return v
}
