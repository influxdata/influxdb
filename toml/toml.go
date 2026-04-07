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
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
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

// MarshalText converts a duration to a string for encoding toml
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

	// Decode the trailing rune. The valid suffixes are all ASCII single-byte,
	// but the input may end in a multi-byte rune (typically when the input is
	// malformed); using DecodeLastRune ensures we report the actual character
	// in the error message rather than a stray UTF-8 continuation byte.
	suffix, suffixSize := utf8.DecodeLastRune(text)
	if !unicode.IsDigit(suffix) {
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
		numText = text[:len(text)-suffixSize]
		if len(numText) == 0 {
			return nil, 0, fmt.Errorf("%w: missing numeric value before suffix %c", ErrSizeParse, suffix)
		}
	}

	return numText, mult, nil
}

// unmarshalSize parses a byte size from text into a size type.
// Unsigned types reject negative values; signed types accept them.
func unmarshalSize[T sizeConstraint](dst *T, text []byte) error {
	// Preserve original text for error messages before stripping the sign.
	originalText := text

	// Detect and strip leading negative sign. Decode the leading rune rather
	// than indexing the first byte so the check is obviously rune-safe; the
	// happy path here is ASCII either way, but explicit decoding spares future
	// readers from reasoning about UTF-8 invariants.
	leading, leadingSize := utf8.DecodeRune(text)
	negative := leading == '-'
	if negative {
		text = text[leadingSize:]
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
		// Use 64-bit parsing so the numeric part can represent abs(MinInt64) = MaxInt64+1.
		bitSize = 64
		if negative {
			maxVal = uint64(math.MaxInt64) + 1 // abs(MinInt64)
		} else {
			maxVal = uint64(math.MaxInt64)
		}
		overflowErr = ErrSSizeOverflow
	}

	size, err := strconv.ParseUint(string(numText), 10, bitSize)
	if err != nil {
		return fmt.Errorf("%w: error parsing %q: %w", ErrSizeParse, string(originalText), err)
	}

	if maxVal/mult < size {
		return fmt.Errorf("%w: %q", overflowErr, string(originalText))
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
		// Compute absolute value in the unsigned domain to avoid signed overflow on MinInt64.
		abs = ^uint64(size) + 1
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
	// Trim whitespace; user.LookupGroup does not.
	groupName = strings.TrimSpace(groupName)
	if groupName == "" {
		return errors.New("group name is empty")
	}

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

// UnmatchedEnvVars returns the names of environment variables in environ that
// match the prefix namespace (start with prefix+"_") but are not present in
// applied. The applied list is typically the result of ApplyEnvOverrides.
//
// This is useful for detecting typos in user-set environment variable names:
// any var that the user set with the configured prefix but that didn't match
// a config field will appear in the result. Callers can log the result at
// startup to give operators feedback about config they thought they were
// setting but weren't.
//
// environ should be in the format returned by os.Environ() — each entry is
// "KEY=VALUE". Entries without an "=" are ignored. The returned slice is
// sorted and deduplicated. If prefix is empty, UnmatchedEnvVars returns nil.
func UnmatchedEnvVars(environ []string, prefix string, applied []string) []string {
	if prefix == "" {
		return nil
	}
	appliedSet := make(map[string]struct{}, len(applied))
	for _, name := range applied {
		appliedSet[name] = struct{}{}
	}

	keyPrefix := prefix + "_"
	seen := make(map[string]struct{})
	var unmatched []string
	for _, entry := range environ {
		eq := strings.IndexByte(entry, '=')
		if eq < 0 {
			continue
		}
		key := entry[:eq]
		if !strings.HasPrefix(key, keyPrefix) {
			continue
		}
		if _, ok := appliedSet[key]; ok {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		unmatched = append(unmatched, key)
	}
	slices.Sort(unmatched)
	return unmatched
}

// Defaulter is implemented by config types that can populate themselves with
// default values. Slice element types reachable from a configuration root passed
// to ApplyEnvOverrides must implement Defaulter so that elements appended via
// indexed environment variables are seeded with defaults before overrides are
// applied. This avoids producing partially-configured elements with zero values
// for unset fields. NewConfig-style constructors should delegate to ApplyDefaults
// so the same defaults are produced regardless of construction path.
type Defaulter interface {
	ApplyDefaults()
}

// VerifyDefaulters walks the type tree of cfg and reports an error if any slice
// element type that would be appended via indexed env var overrides does not
// implement Defaulter. cfg may be a value or a pointer; the type tree is walked
// from its (dereferenced) type.
//
// Element types that implement encoding.TextUnmarshaler are exempt: they are
// treated as leaves by ApplyEnvOverrides and have no fields to default.
// Primitive element types (string, int, etc.) are also exempt for the same
// reason. Fields tagged `toml:"-"` and unexported fields are skipped because
// ApplyEnvOverrides skips them too.
//
// VerifyDefaulters is intended to be called from a test in the package that
// owns the config root, as a CI safety net for the convention that slice
// element types must implement ApplyDefaults.
func VerifyDefaulters(cfg interface{}) error {
	defaulterType := reflect.TypeOf((*Defaulter)(nil)).Elem()
	textUnmarshalerType := reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

	// requiresDefaulter reports whether a slice element type must implement
	// Defaulter. Only struct (or pointer-to-struct) element types that don't
	// implement TextUnmarshaler need it.
	requiresDefaulter := func(elemType reflect.Type) bool {
		target := elemType
		if target.Kind() == reflect.Pointer {
			target = target.Elem()
		}
		if target.Kind() != reflect.Struct {
			return false
		}
		if reflect.PointerTo(target).Implements(textUnmarshalerType) {
			return false
		}
		return true
	}

	var violations []string
	seen := make(map[reflect.Type]bool)
	var walk func(t reflect.Type, path string)
	walk = func(t reflect.Type, path string) {
		if seen[t] {
			return
		}
		seen[t] = true

		switch t.Kind() {
		case reflect.Pointer:
			walk(t.Elem(), path)
		case reflect.Slice:
			elem := t.Elem()
			if requiresDefaulter(elem) {
				// For value-element slices ([]T), Defaulter is implemented by *T.
				// For pointer-element slices ([]*T), Defaulter is implemented by *T directly.
				ptrType := reflect.PointerTo(elem)
				if elem.Kind() == reflect.Pointer {
					ptrType = elem
				}
				if !ptrType.Implements(defaulterType) {
					violations = append(violations, fmt.Sprintf(
						"%s: slice element type %s must implement toml.Defaulter (add ApplyDefaults() method on *%s)",
						path, elem, elem))
				}
			}
			walk(elem, path+"[]")
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if !f.IsExported() {
					continue
				}
				if f.Tag.Get("toml") == "-" {
					continue
				}
				walk(f.Type, path+"."+f.Name)
			}
		}
	}

	rootType := reflect.TypeOf(cfg)
	if rootType == nil {
		return errors.New("VerifyDefaulters: cfg is nil")
	}
	rootName := rootType.String()
	if rootType.Kind() == reflect.Pointer {
		rootType = rootType.Elem()
		if rootType != nil {
			rootName = rootType.String()
		}
	}
	walk(rootType, rootName)

	if len(violations) > 0 {
		return fmt.Errorf("toml.VerifyDefaulters found %d violation(s):\n  %s",
			len(violations), strings.Join(violations, "\n  "))
	}
	return nil
}

// ApplyEnvOverrides applies environment variable overrides to the given configuration value.
// It returns the list of all environment variable names that were applied and any error encountered.
func ApplyEnvOverrides(getenv func(string) string, prefix string, val interface{}) ([]string, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	result, err := applyEnvOverrides(getenv, prefix, reflect.ValueOf(val), "")
	return result.AllVars, err
}

// envOverrideResult holds the result of applying environment overrides recursively.
type envOverrideResult struct {
	// Applied indicates whether any non-default override was applied by this or any recursive call.
	Applied bool
	// AllVars contains the names of all environment variables that were applied, including defaults.
	// Sorted and deduplicated.
	AllVars []string
	// IndexedVars contains only the indexed (non-default) environment variable names applied.
	// This excludes unindexed slice defaults and is used for slice growth error messages.
	// Sorted and deduplicated.
	IndexedVars []string
}

// insertVar inserts a variable name into a sorted slice if not already present.
func (r *envOverrideResult) insertVar(dest *[]string, name string) {
	i, found := slices.BinarySearch(*dest, name)
	if !found {
		*dest = slices.Insert(*dest, i, name)
	}
}

// mergeAllVars adds the AllVars from other into this result, without affecting Applied or IndexedVars.
// Used for default (unindexed) slice element overrides where Applied and IndexedVars are intentionally ignored.
func (r *envOverrideResult) mergeAllVars(other envOverrideResult) {
	for _, v := range other.AllVars {
		r.insertVar(&r.AllVars, v)
	}
}

// merge incorporates another result into this one, maintaining sorted, deduplicated variable name lists.
func (r *envOverrideResult) merge(other envOverrideResult) {
	if other.Applied {
		r.Applied = true
	}
	for _, v := range other.AllVars {
		r.insertVar(&r.AllVars, v)
	}
	for _, v := range other.IndexedVars {
		r.insertVar(&r.IndexedVars, v)
	}
}

// appliedEnvVar creates a result for a single leaf environment variable that was successfully applied.
func appliedEnvVar(name string) envOverrideResult {
	return envOverrideResult{Applied: true, AllVars: []string{name}, IndexedVars: []string{name}}
}

// joinStructKey builds a dotted path for structKey as we recurse into nested structs and slices.
func joinStructKey(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// indexStructKey appends a slice index to a structKey path (e.g. "Foo" -> "Foo[0]").
func indexStructKey(parent string, idx int) string {
	return fmt.Sprintf("%s[%d]", parent, idx)
}

// isLeafType reports whether values of t can be set directly from a single env
// var value (a primitive kind or a TextUnmarshaler implementation), as opposed
// to types whose configuration is spread across multiple env vars (structs).
func isLeafType(t reflect.Type) bool {
	if reflect.PointerTo(t).Implements(reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()) {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// applyEnvOverrides applies environment overrides recursively.
func applyEnvOverrides(getenv func(string) string, prefix string, spec reflect.Value, structKey string) (envOverrideResult, error) {
	element := spec

	value := strings.TrimSpace(getenv(prefix))

	var noResult envOverrideResult

	// If we have a pointer, dereference it. For nil pointers to leaf types
	// (scalars or TextUnmarshaler implementations), allocate the underlying
	// value when there is a non-empty env var to apply. This allows fields like
	// httpd.Config.UnixSocketGroup (*toml.Group) to be set purely via env vars
	// without requiring NewConfig or the TOML file to pre-allocate the pointer.
	//
	// Nil pointers to struct types are still skipped because the struct's env
	// vars target its fields (e.g., INFLUXDB_FOO_BAR), not the struct itself,
	// so there's no way to detect whether to allocate without probing every
	// possible field env var.
	if spec.Kind() == reflect.Pointer {
		if spec.IsNil() {
			if len(value) == 0 || !spec.CanSet() || !isLeafType(spec.Type().Elem()) {
				return noResult, nil
			}
			spec.Set(reflect.New(spec.Type().Elem()))
		}
		element = spec.Elem()
	}

	// If element is a named type and is addressable,
	// check the address to see if it implements encoding.TextUnmarshaler.
	if element.Type().Name() != "" && element.CanAddr() {
		if u, ok := element.Addr().Interface().(encoding.TextUnmarshaler); ok {
			// Skip any fields we don't have a value to set
			if len(value) == 0 {
				return noResult, nil
			}
			if err := u.UnmarshalText([]byte(value)); err != nil {
				return noResult, fmt.Errorf("failed to apply %v to %v using TextUnmarshaler %v and value %q: %w", prefix, structKey, element.Type().String(), value, err)
			}
			return appliedEnvVar(prefix), nil
		}
	}

	switch element.Kind() {
	case reflect.String:
		if len(value) == 0 {
			return noResult, nil
		}
		element.SetString(value)
		return appliedEnvVar(prefix), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if len(value) == 0 {
			return noResult, nil
		}
		// Base 0 lets the user pick the radix via prefix:
		//   0x / 0X  -> hex
		//   0o / 0O  -> octal
		//   bare 0   -> octal (C-style; e.g., "010" is 8)
		//   0b / 0B  -> binary
		//   otherwise -> decimal
		// This intentionally differs from TOML's stricter integer syntax.
		intValue, err := strconv.ParseInt(value, 0, element.Type().Bits())
		if err != nil {
			return noResult, fmt.Errorf("failed to apply %v to %v using type %v and value %q: %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetInt(intValue)
		return appliedEnvVar(prefix), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if len(value) == 0 {
			return noResult, nil
		}
		// See the int case above for the supported numeric prefixes.
		intValue, err := strconv.ParseUint(value, 0, element.Type().Bits())
		if err != nil {
			return noResult, fmt.Errorf("failed to apply %v to %v using type %v and value %q: %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetUint(intValue)
		return appliedEnvVar(prefix), nil
	case reflect.Bool:
		if len(value) == 0 {
			return noResult, nil
		}
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return noResult, fmt.Errorf("failed to apply %v to %v using type %v and value %q: %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetBool(boolValue)
		return appliedEnvVar(prefix), nil
	case reflect.Float32, reflect.Float64:
		if len(value) == 0 {
			return noResult, nil
		}
		floatValue, err := strconv.ParseFloat(value, element.Type().Bits())
		if err != nil {
			return noResult, fmt.Errorf("failed to apply %v to %v using type %v and value %q: %w", prefix, structKey, element.Type().String(), value, err)
		}
		element.SetFloat(floatValue)
		return appliedEnvVar(prefix), nil
	case reflect.Slice:
		startLen := element.Len()
		var result envOverrideResult

		// Handle indexed slices (e.g. VALUE_0, VALUE_1, VALUE_2, etc.)
		for idx, envOutOfBounds := 0, false; idx < element.Len() || !envOutOfBounds; idx++ {
			// Are we still within the bounds of the starting slice?
			indexedEnvName := fmt.Sprintf("%s_%d", prefix, idx)
			if idx < element.Len() {
				f := element.Index(idx)

				// Apply the unindexed environment variable as a default value, if available.
				// Finding a default environment value does not count when considering if we continue
				// extending the slice, so we throw the found return value away.
				if defaultResult, err := applyEnvOverrides(getenv, prefix, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				} else {
					result.mergeAllVars(defaultResult)
				}

				// Apply the indexed environment variable as an override value.
				if indexedResult, err := applyEnvOverrides(getenv, indexedEnvName, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				} else {
					result.merge(indexedResult)
				}
			} else {
				// We have run past the end of starting slice, but are there more environment array indices?
				// Create a zero-value value to unmarshal the environment override into.
				f := reflect.New(element.Type().Elem()).Elem()
				// For pointer slice elements, allocate the underlying value so we can call
				// methods on it (e.g., ApplyDefaults) and apply env overrides to its fields.
				if f.Kind() == reflect.Pointer && f.IsNil() {
					f.Set(reflect.New(f.Type().Elem()))
				}
				// If the element type implements Defaulter, seed the new element with its
				// type-level defaults before applying any env vars. Built-in defaults are the
				// weakest layer; unindexed env defaults override them, indexed env vars override
				// those. ApplyDefaults is called on every probe iteration; the cost is negligible
				// and the alternative (deferring until we know the element will be appended)
				// would require re-running the unindexed default application after ApplyDefaults.
				// For pointer slice elements (f is already a pointer), check the value directly;
				// otherwise check the address.
				var defaulter Defaulter
				if f.Kind() == reflect.Pointer {
					defaulter, _ = f.Interface().(Defaulter)
				} else if f.CanAddr() {
					defaulter, _ = f.Addr().Interface().(Defaulter)
				}
				if defaulter != nil {
					defaulter.ApplyDefaults()
				}
				// Apply the unindexed environment variable as a default value, same as for existing elements.
				// Skipped for leaf element types (scalars and TextUnmarshaler implementations) because
				// the required indexed override would fully replace the value anyway, making the default
				// pure wasted work. For non-leaf elements (structs whose individual fields can be defaulted),
				// the unindexed default contributes fields that the indexed override doesn't touch.
				var defaultResult envOverrideResult
				if !isLeafType(element.Type().Elem()) {
					var err error
					if defaultResult, err = applyEnvOverrides(getenv, prefix, f, indexStructKey(structKey, idx)); err != nil {
						return noResult, err
					}
				}
				if indexedResult, err := applyEnvOverrides(getenv, indexedEnvName, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				} else if indexedResult.Applied {
					// Only record default vars when the element is actually appended.
					result.mergeAllVars(defaultResult)
					result.merge(indexedResult)
					// We found environment variables to override into newValue. Check for growth bound before appending.
					if idx-startLen >= MaxEnvSliceGrowth {
						overridesStr := "overrides"
						if len(indexedResult.IndexedVars) == 1 {
							overridesStr = "override"
						}
						return noResult, fmt.Errorf(
							"env %s %s would grow slice beyond maximum of %d appended elements", overridesStr, strings.Join(indexedResult.IndexedVars, ","), MaxEnvSliceGrowth)
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
			result.Applied = true
			result.insertVar(&result.AllVars, prefix)
			result.insertVar(&result.IndexedVars, prefix)
			parts := strings.Split(value, ",")
			if len(parts) > MaxEnvSliceGrowth {
				return noResult, fmt.Errorf("env override %s has %d comma-separated values, exceeding maximum of %d", prefix, len(parts), MaxEnvSliceGrowth)
			}
			for idx, val := range parts {
				// Append a zero value and then set it. This way we aren't assuming element is a []string.
				element.Set(reflect.Append(element, reflect.Zero(element.Type().Elem())))
				f := element.Index(idx)
				// The custom getenv returns val for any key, so the recursive call will
				// report prefix as applied. Since we already recorded prefix above, merge
				// deduplicates it automatically — no manual DeleteFunc needed.
				if csvResult, err := applyEnvOverrides(func(n string) string { return val }, prefix, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				} else {
					result.merge(csvResult)
				}
			}
		}

		return result, nil

	case reflect.Struct:
		var result envOverrideResult

		typeOfSpec := element.Type()
		for i := 0; i < element.NumField(); i++ {
			field := element.Field(i)

			fieldResult, err := func() (envOverrideResult, error) {
				// Skip any fields that we cannot set
				if !field.CanSet() && field.Kind() != reflect.Slice {
					return noResult, nil
				}

				structField := typeOfSpec.Field(i)
				fieldName := structField.Name

				configName := structField.Tag.Get("toml")
				if configName == "-" {
					// Skip fields with tag `toml:"-"`.
					return noResult, nil
				}

				if configName == "" && structField.Anonymous {
					// Embedded field without a toml tag.
					// Don't modify prefix.
					return applyEnvOverrides(getenv, prefix, field, joinStructKey(structKey, fieldName))
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
				return applyEnvOverrides(getenv, envKey, field, joinStructKey(structKey, fieldName))
			}()
			if err != nil {
				return noResult, err
			}
			result.merge(fieldResult)
		}

		return result, nil
	}

	return noResult, nil
}
