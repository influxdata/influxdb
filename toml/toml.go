// Package toml adds support to marshal and unmarshal types not in the official TOML spec.
package toml // import "github.com/influxdata/influxdb/toml"

import (
	"encoding"
	"errors"
	"fmt"
	"math"
	"os"
	"os/user"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// MaxEnvSliceGrowth limits how many elements can be appended to a slice via
// environment variable overrides, preventing unbounded memory allocation.
const MaxEnvSliceGrowth = 64

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// String returns the string representation of the duration.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	// Ignore if there is no value set.
	if len(text) == 0 {
		return nil
	}

	// Otherwise parse as a duration formatted string.
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	// Set duration and return.
	*d = Duration(duration)
	return nil
}

// MarshalText converts a duration to a string for decoding toml
func (d Duration) MarshalText() (text []byte, err error) {
	return []byte(d.String()), nil
}

// Size represents a TOML parseable file size.
// Users can specify size using "k" or "K" for kibibytes, "m" or "M" for mebibytes,
// and "g" or "G" for gibibytes. If a size suffix isn't specified then bytes are assumed.
type Size uint64

// SSize is like Size but uses a signed int64, allowing negative values.
type SSize int64

var (
	ErrSizeEmpty     = errors.New("size was empty")
	ErrSizeBadSuffix = errors.New("unknown size suffix")
	ErrSizeParse     = errors.New("invalid size")
	ErrSizeOverflow  = fmt.Errorf("size would overflow the max size (%d) of a uint64", uint64(math.MaxUint64))
	ErrSSizeOverflow = fmt.Errorf("size would overflow the max size (%d) of an int64", int64(math.MaxInt64))
)

// sizeConstraint is the type constraint for size types.
type sizeConstraint interface {
	~uint64 | ~int64
}

// parseSizeSuffix extracts the multiplier and numeric text from a size string.
// It returns the numeric portion of the text and the multiplier.
func parseSizeSuffix(text []byte) (numText []byte, mult uint64, err error) {
	if len(text) == 0 {
		return nil, 0, ErrSizeEmpty
	}

	mult = 1
	numText = text

	suffix := text[len(text)-1]
	if !unicode.IsDigit(rune(suffix)) {
		switch suffix {
		case 'k', 'K':
			mult = 1 << 10 // KiB
		case 'm', 'M':
			mult = 1 << 20 // MiB
		case 'g', 'G':
			mult = 1 << 30 // GiB
		default:
			return nil, 0, fmt.Errorf("%w: %c (expected k, m, or g)", ErrSizeBadSuffix, suffix)
		}
		numText = text[:len(text)-1]
	}

	return numText, mult, nil
}

// unmarshalSize parses a byte size from text into a size type.
// Unsigned types reject negative values; signed types accept them.
func unmarshalSize[T sizeConstraint](dst *T, text []byte) error {
	// Detect and strip leading negative sign.
	negative := len(text) > 0 && text[0] == '-'
	if negative {
		text = text[1:]
	}

	numText, mult, err := parseSizeSuffix(text)
	if err != nil {
		return err
	}

	// Determine bit width and overflow limit based on signedness.
	// For unsigned types, T(0)-1 wraps to a large positive value.
	var bitSize int
	var maxVal uint64
	var overflowErr error
	if T(0)-1 > 0 {
		if negative {
			return fmt.Errorf("%w: negative value not allowed", ErrSizeParse)
		}
		bitSize = 64
		maxVal = math.MaxUint64
		overflowErr = ErrSizeOverflow
	} else {
		bitSize = 63
		maxVal = math.MaxInt64
		overflowErr = ErrSSizeOverflow
	}

	size, err := strconv.ParseUint(string(numText), 10, bitSize)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrSizeParse, err)
	}

	if maxVal/mult < size {
		return fmt.Errorf("%w: %s", overflowErr, string(text))
	}

	result := T(size * mult)
	if negative {
		result = -result
	}

	*dst = result
	return nil
}

// marshalSize formats a size value with the largest whole-unit suffix.
func marshalSize[T sizeConstraint](size T) ([]byte, error) {
	negative := size < 0
	var abs uint64
	if negative {
		abs = uint64(-size)
	} else {
		abs = uint64(size)
	}

	var suffix byte
	switch {
	case abs >= 1<<30 && abs%(1<<30) == 0:
		abs /= 1 << 30
		suffix = 'g'
	case abs >= 1<<20 && abs%(1<<20) == 0:
		abs /= 1 << 20
		suffix = 'm'
	case abs >= 1<<10 && abs%(1<<10) == 0:
		abs /= 1 << 10
		suffix = 'k'
	}

	var s string
	if negative {
		s = fmt.Sprintf("-%d", abs)
	} else {
		s = strconv.FormatUint(abs, 10)
	}
	if suffix != 0 {
		s += string(suffix)
	}
	return []byte(s), nil
}

// UnmarshalText parses a byte size from text.
func (s *Size) UnmarshalText(text []byte) error {
	return unmarshalSize(s, text)
}

// MarshalText converts a Size to a string for encoding toml.
func (s Size) MarshalText() ([]byte, error) {
	return marshalSize(s)
}

// UnmarshalText parses a byte size from text, allowing negative values.
func (s *SSize) UnmarshalText(text []byte) error {
	return unmarshalSize(s, text)
}

// MarshalText converts an SSize to a string for encoding toml.
func (s SSize) MarshalText() ([]byte, error) {
	return marshalSize(s)
}

type FileMode uint32

func (m *FileMode) UnmarshalText(text []byte) error {
	// Ignore if there is no value set.
	if len(text) == 0 {
		return nil
	}

	mode, err := strconv.ParseUint(string(text), 8, 32)
	if err != nil {
		return err
	} else if mode == 0 {
		return errors.New("file mode cannot be zero")
	}
	*m = FileMode(mode)
	return nil
}

func (m FileMode) MarshalText() (text []byte, err error) {
	if m != 0 {
		return []byte(fmt.Sprintf("%04o", m)), nil
	} else {
		return []byte(""), nil
	}
}

type Group int

func (g *Group) unmarshalGroupName(groupName string) error {
	var gid int

	group, lookupErr := user.LookupGroup(groupName)
	if lookupErr != nil {
		// Is groupName really a numeric group?
		if _, err := strconv.Atoi(groupName); err != nil {
			// No, not a number.
			return lookupErr
		}
		group, lookupErr = user.LookupGroupId(groupName)
		if lookupErr != nil {
			return lookupErr
		}
	}

	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return err
	}

	*g = Group(gid)
	return nil
}

func (g *Group) UnmarshalText(text []byte) error {
	return g.unmarshalGroupName(string(text))
}

func (g *Group) UnmarshalTOML(data interface{}) error {
	if grpName, ok := data.(string); ok {
		return g.unmarshalGroupName(grpName)
	} else if gid, ok := data.(int64); ok {
		*g = Group(gid)
		return nil
	}
	return errors.New("group must be a name (string) or id (int)")
}

func ApplyEnvOverrides(getenv func(string) string, prefix string, val interface{}) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	_, err := applyEnvOverrides(getenv, prefix, reflect.ValueOf(val), "")
	return err
}

func applyEnvOverrides(getenv func(string) string, prefix string, spec reflect.Value, structKey string) (bool, error) {
	element := spec

	value := strings.TrimSpace(getenv(prefix))

	// If we have a pointer, dereference it
	if spec.Kind() == reflect.Pointer {
		if spec.IsNil() {
			return false, nil
		}
		element = spec.Elem()
	}

	// If element is a named type and is addressable,
	// check the address to see if it implements encoding.TextUnmarshaler.
	if element.Type().Name() != "" && element.CanAddr() {
		if u, ok := element.Addr().Interface().(encoding.TextUnmarshaler); ok {
			// Skip any fields we don't have a value to set
			if len(value) == 0 {
				return false, nil
			}
			if err := u.UnmarshalText([]byte(value)); err != nil {
				return false, fmt.Errorf("failed to apply %v to %v using TextUnmarshaler %v and value '%v': %w", prefix, structKey, element.Type().String(), value, err)
			}
			return true, nil
		}
	}

	switch element.Kind() {
	case reflect.String:
		if len(value) == 0 {
			return false, nil
		}
		element.SetString(value)
		return true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(value) == 0 {
			return false, nil
		}
		intValue, err := strconv.ParseInt(value, 0, element.Type().Bits())
		if err != nil {
			return false, fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetInt(intValue)
		return true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if len(value) == 0 {
			return false, nil
		}
		intValue, err := strconv.ParseUint(value, 0, element.Type().Bits())
		if err != nil {
			return false, fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetUint(intValue)
		return true, nil
	case reflect.Bool:
		if len(value) == 0 {
			return false, nil
		}
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return false, fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetBool(boolValue)
		return true, nil
	case reflect.Float32, reflect.Float64:
		if len(value) == 0 {
			return false, nil
		}
		floatValue, err := strconv.ParseFloat(value, element.Type().Bits())
		if err != nil {
			return false, fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetFloat(floatValue)
		return true, nil
	case reflect.Slice:
		startLen := element.Len()
		foundOverrides := false

		// Handle indexed slices (e.g. VALUE_0, VALUE_1, VALUE_2, etc.)
		for idx, envOutOfBounds := 0, false; idx < element.Len() || !envOutOfBounds; idx++ {
			// Are we still within the bounds of the starting slice?
			indexedEnvName := fmt.Sprintf("%s_%d", prefix, idx)
			if idx < element.Len() {
				f := element.Index(idx)

				// Apply the unindexed environment variable as a default value, if available.
				// Finding a default environment value does not count when considering if we continue
				// extending the slice, so we throw the found return value away.
				if _, err := applyEnvOverrides(getenv, prefix, f, structKey); err != nil {
					return false, err
				}

				// Apply the indexed environment variable as an override value.
				if found, err := applyEnvOverrides(getenv, indexedEnvName, f, structKey); err != nil {
					return found, err
				} else if found {
					foundOverrides = true
				}
			} else {
				// We have run past the end of starting slice, but are there more environment array indices?
				// Create a zero-value value to unmarshal the environment override into.
				f := reflect.New(element.Type().Elem()).Elem()
				// Apply the unindexed environment variable as a default value, same as for existing elements.
				// Only meaningful for struct/pointer elements where individual fields can be defaulted.
				elemKind := element.Type().Elem().Kind()
				if elemKind == reflect.Struct || elemKind == reflect.Pointer {
					if _, err := applyEnvOverrides(getenv, prefix, f, structKey); err != nil {
						return false, err
					}
				}
				if found, err := applyEnvOverrides(getenv, indexedEnvName, f, structKey); err != nil {
					return found, err
				} else if found {
					foundOverrides = true
					// We found environment variables to override into newValue. Check for growth bound before appending.
					if idx-startLen >= MaxEnvSliceGrowth {
						return false, fmt.Errorf("env override %s would grow slice beyond maximum of %d appended elements", indexedEnvName, MaxEnvSliceGrowth)
					}

					element.Set(reflect.Append(element, f))
				} else {
					// We seem to have run past the end of the environment indices.
					envOutOfBounds = true
				}
			}
		}

		// If the type is s slice but have value not parsed as slice e.g. GRAPHITE_0_TEMPLATES="item1,item2"
		if element.Len() == 0 && len(value) > 0 {
			foundOverrides = true
			parts := strings.Split(value, ",")
			if len(parts) > MaxEnvSliceGrowth {
				return false, fmt.Errorf("env override %s has %d comma-separated values, exceeding maximum of %d", prefix, len(parts), MaxEnvSliceGrowth)
			}
			for idx, val := range parts {
				// Append a zero value and then set it. This way we aren't assuming element is a []string.
				element.Set(reflect.Append(element, reflect.Zero(element.Type().Elem())))
				f := element.Index(idx)
				if _, err := applyEnvOverrides(func(n string) string { return val }, prefix, f, structKey); err != nil {
					return false, err
				}
			}
		}

		return foundOverrides, nil

	case reflect.Struct:
		foundOverrides := false

		typeOfSpec := element.Type()
		for i := 0; i < element.NumField(); i++ {
			field := element.Field(i)

			foundField, err := func() (bool, error) {
				// Skip any fields that we cannot set
				if !field.CanSet() && field.Kind() != reflect.Slice {
					return false, nil
				}

				structField := typeOfSpec.Field(i)
				fieldName := structField.Name

				configName := structField.Tag.Get("toml")
				if configName == "-" {
					// Skip fields with tag `toml:"-"`.
					return false, nil
				}

				if configName == "" && structField.Anonymous {
					// Embedded field without a toml tag.
					// Don't modify prefix.
					return applyEnvOverrides(getenv, prefix, field, fieldName)
				}

				// Fall back to field name if no toml tag, matching BurntSushi/toml behavior.
				if configName == "" {
					configName = fieldName
				}

				// Replace hyphens with underscores to avoid issues with shells
				configName = strings.ReplaceAll(configName, "-", "_")

				envKey := strings.ToUpper(configName)
				if prefix != "" {
					envKey = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
				}

				// Apply recursively to field. Works for scalars, structs, slices, pointers, etc.
				return applyEnvOverrides(getenv, envKey, field, fieldName)
			}()
			if err != nil {
				return false, err
			}
			if foundField {
				foundOverrides = true
			}
		}

		return foundOverrides, nil
	}

	return false, nil
}
