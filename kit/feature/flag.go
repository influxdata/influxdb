//go:generate go run ./_codegen/main.go --in ../../flags.yml --out ./list.go

package feature

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/v2/kit/feature/lifetime"
)

// Flag represents a generic feature flag with a key and a default.
type Flag interface {
	// Key returns the programmatic backend identifier for the flag.
	Key() string
	// Default returns the type-agnostic zero value for the flag.
	// Type-specific flag implementations may expose a typed default
	// (e.g. BoolFlag includes a boolean Default field).
	Default() interface{}
	// TestingDefault returns the type-agnostic default value to be
	// used during testing.
	TestingDefault() interface{}
	// Expose the flag.
	Expose() bool
}

// MakeFlag constructs a Flag. The concrete implementation is inferred from the provided default.
func MakeFlag(name, key, owner string, defaultValue interface{}, testingDefault interface{}, lifetime lifetime.Lifetime, expose bool) Flag {
	b := MakeBase(name, key, owner, defaultValue, testingDefault, lifetime, expose)
	switch v := defaultValue.(type) {
	case bool:
		return BoolFlag{b, v, testingDefault.(bool)}
	case float64:
		return FloatFlag{b, v, testingDefault.(float64)}
	case int32:
		return IntFlag{b, v, testingDefault.(int32)}
	case int:
		return IntFlag{b, int32(v), int32(testingDefault.(int))}
	case string:
		return StringFlag{b, v, testingDefault.(string)}
	default:
		return StringFlag{b, fmt.Sprintf("%v", v), fmt.Sprintf("%v", v)}
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
	// testing default value for the flag.
	testingDefault interface{}
	// owner is an individual or team responsible for the flag.
	owner string
	// lifetime of the feature flag.
	lifetime lifetime.Lifetime
	// expose the flag.
	expose bool
}

var _ Flag = Base{}

// MakeBase constructs a flag flag.
func MakeBase(name, key, owner string, defaultValue interface{}, testingDefault interface{}, lifetime lifetime.Lifetime, expose bool) Base {
	return Base{
		name:           name,
		key:            key,
		owner:          owner,
		defaultValue:   defaultValue,
		testingDefault: testingDefault,
		lifetime:       lifetime,
		expose:         expose,
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

// Default returns the type-agnostic zero value for the flag.
func (f Base) TestingDefault() interface{} {
	return f.testingDefault
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
	testingString string
}

var _ Flag = StringFlag{}

// MakeStringFlag returns a string flag with the given Base and default.
func MakeStringFlag(name, key, owner string, defaultValue string, testingDefault string, lifetime lifetime.Lifetime, expose bool) StringFlag {
	b := MakeBase(name, key, owner, defaultValue, testingDefault, lifetime, expose)
	return StringFlag{b, defaultValue, testingDefault}
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
	testingFloat float64
}

var _ Flag = FloatFlag{}

// MakeFloatFlag returns a string flag with the given Base and default.
func MakeFloatFlag(name, key, owner string, defaultValue float64, testingDefault float64, lifetime lifetime.Lifetime, expose bool) FloatFlag {
	b := MakeBase(name, key, owner, defaultValue, testingDefault, lifetime, expose)
	return FloatFlag{b, defaultValue, testingDefault}
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
	testingInt int32
}

var _ Flag = IntFlag{}

// MakeIntFlag returns a string flag with the given Base and default.
func MakeIntFlag(name, key, owner string, defaultValue int32, testingDefault int32, lifetime lifetime.Lifetime, expose bool) IntFlag {
	b := MakeBase(name, key, owner, defaultValue, testingDefault, lifetime, expose)
	return IntFlag{b, defaultValue, testingDefault}
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
	testingBool bool
}

var _ Flag = BoolFlag{}

// MakeBoolFlag returns a string flag with the given Base and default.
func MakeBoolFlag(name, key, owner string, defaultValue bool, testingDefault bool, lifetime lifetime.Lifetime, expose bool) BoolFlag {
	b := MakeBase(name, key, owner, defaultValue, testingDefault, lifetime, expose)
	return BoolFlag{b, defaultValue, testingDefault}
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
