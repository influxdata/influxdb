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

// UnmarshalText parses a byte size from text.
func (s *Size) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return fmt.Errorf("size was empty")
	}

	// The multiplier defaults to 1 in case the size has
	// no suffix (and is then just raw bytes)
	mult := uint64(1)

	// Preserve the original text for error messages
	sizeText := text

	// Parse unit of measure
	suffix := text[len(sizeText)-1]
	if !unicode.IsDigit(rune(suffix)) {
		switch suffix {
		case 'k', 'K':
			mult = 1 << 10 // KiB
		case 'm', 'M':
			mult = 1 << 20 // MiB
		case 'g', 'G':
			mult = 1 << 30 // GiB
		default:
			return fmt.Errorf("unknown size suffix: %c (expected k, m, or g)", suffix)
		}
		sizeText = sizeText[:len(sizeText)-1]
	}

	// Parse numeric portion of value.
	size, err := strconv.ParseUint(string(sizeText), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid size: %s", string(text))
	}

	if math.MaxUint64/mult < size {
		return fmt.Errorf("size would overflow the max size (%d) of a uint: %s", uint64(math.MaxUint64), string(text))
	}

	size *= mult

	*s = Size(size)
	return nil
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
	}
	return nil, nil
}

type Group int

func (g *Group) UnmarshalTOML(data interface{}) error {
	if grpName, ok := data.(string); ok {
		group, err := user.LookupGroup(grpName)
		if err != nil {
			return err
		}

		gid, err := strconv.Atoi(group.Gid)
		if err != nil {
			return err
		}
		*g = Group(gid)
		return nil
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
	return applyEnvOverrides(getenv, prefix, reflect.ValueOf(val), "")
}

func applyEnvOverrides(getenv func(string) string, prefix string, spec reflect.Value, structKey string) error {
	element := spec
	// If spec is a named type and is addressable,
	// check the address to see if it implements encoding.TextUnmarshaler.
	if spec.Kind() != reflect.Ptr && spec.Type().Name() != "" && spec.CanAddr() {
		v := spec.Addr()
		if u, ok := v.Interface().(encoding.TextUnmarshaler); ok {
			value := getenv(prefix)
			// Skip any fields we don't have a value to set
			if len(value) == 0 {
				return nil
			}
			return u.UnmarshalText([]byte(value))
		}
	}
	// If we have a pointer, dereference it
	if spec.Kind() == reflect.Ptr {
		element = spec.Elem()
	}

	value := getenv(prefix)

	switch element.Kind() {
	case reflect.String:
		if len(value) == 0 {
			return nil
		}
		element.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intValue, err := strconv.ParseInt(value, 0, element.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %s", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetInt(intValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		intValue, err := strconv.ParseUint(value, 0, element.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %s", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetUint(intValue)
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %s", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetBool(boolValue)
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, element.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v to %v using type %v and value '%v': %s", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetFloat(floatValue)
	case reflect.Slice:
		// If the type is s slice, apply to each using the index as a suffix, e.g. GRAPHITE_0, GRAPHITE_0_TEMPLATES_0 or GRAPHITE_0_TEMPLATES="item1,item2"
		for j := 0; j < element.Len(); j++ {
			f := element.Index(j)
			if err := applyEnvOverrides(getenv, prefix, f, structKey); err != nil {
				return err
			}

			if err := applyEnvOverrides(getenv, fmt.Sprintf("%s_%d", prefix, j), f, structKey); err != nil {
				return err
			}
		}

		// If the type is s slice but have value not parsed as slice e.g. GRAPHITE_0_TEMPLATES="item1,item2"
		if element.Len() == 0 && len(value) > 0 {
			rules := strings.Split(value, ",")

			for _, rule := range rules {
				element.Set(reflect.Append(element, reflect.ValueOf(rule)))
			}
		}
	case reflect.Struct:
		typeOfSpec := element.Type()
		for i := 0; i < element.NumField(); i++ {
			field := element.Field(i)

			// Skip any fields that we cannot set
			if !field.CanSet() && field.Kind() != reflect.Slice {
				continue
			}

			structField := typeOfSpec.Field(i)
			fieldName := structField.Name

			configName := structField.Tag.Get("toml")
			if configName == "-" {
				// Skip fields with tag `toml:"-"`.
				continue
			}

			if configName == "" && structField.Anonymous {
				// Embedded field without a toml tag.
				// Don't modify prefix.
				if err := applyEnvOverrides(getenv, prefix, field, fieldName); err != nil {
					return err
				}
				continue
			}

			// Replace hyphens with underscores to avoid issues with shells
			configName = strings.Replace(configName, "-", "_", -1)

			envKey := strings.ToUpper(configName)
			if prefix != "" {
				envKey = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
			}

			// If it's a sub-config, recursively apply
			if field.Kind() == reflect.Struct || field.Kind() == reflect.Ptr ||
				field.Kind() == reflect.Slice || field.Kind() == reflect.Array {
				if err := applyEnvOverrides(getenv, envKey, field, fieldName); err != nil {
					return err
				}
				continue
			}

			value := getenv(envKey)
			// Skip any fields we don't have a value to set
			if len(value) == 0 {
				continue
			}

			if err := applyEnvOverrides(getenv, envKey, field, fieldName); err != nil {
				return err
			}
		}
	}
	return nil
}
