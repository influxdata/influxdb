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
// This is to prevent unbounded growth by environment variables, a
// potential security issue, as well as unintentionally unbounded growth due
// to errors in management scripts.
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
	ErrSizeEmpty      = errors.New("size was empty")
	ErrSizeBadSuffix  = errors.New("unknown size suffix")
	ErrSizeParse      = errors.New("invalid size")
	ErrSizeOverflow   = fmt.Errorf("size would overflow the max size (%d) of a uint64", uint64(math.MaxUint64))
	ErrSSizeOverflow  = fmt.Errorf("size would overflow the max size (%d) of an int64", int64(math.MaxInt64))
	ErrSizeOutOfRange = errors.New("size value out of range for target type")
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
	// Check that mult is not zero to prevent division by zero later. This should never happen.
	if mult == 0 {
		return fmt.Errorf("%w: parsing size suffix of %q got multiplier of 0", ErrSizeParse, string(originalText))
	}

	// Determine bit width and overflow limit based on signedness.
	// For unsigned types, T(0)-1 wraps to a large positive value.
	var bitSize int
	var maxVal uint64
	var overflowErr error
	if T(0)-1 > 0 {
		if negative {
			return fmt.Errorf("%w: negative value not allowed: %q", ErrSizeParse, string(originalText))
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

// ToInt returns the value as an int, or an error wrapping ErrSizeOutOfRange
// if it does not fit. Size is uint64 so it may exceed the representable range
// of int on any platform (and routinely does on 32-bit platforms). Callers
// passing a Size to APIs that take int should go through ToInt rather than a
// bare cast.
func (s Size) ToInt() (int, error) {
	if uint64(s) > math.MaxInt {
		return 0, fmt.Errorf("%w: size value %d exceeds maximum int value %d", ErrSizeOutOfRange, uint64(s), math.MaxInt)
	}
	return int(s), nil
}

// ToInt64 returns the value as an int64, or an error wrapping
// ErrSizeOutOfRange if it does not fit. Size is uint64 so values above
// math.MaxInt64 silently wrap to negative when cast directly. ToInt64 rejects
// those instead.
func (s Size) ToInt64() (int64, error) {
	if uint64(s) > math.MaxInt64 {
		return 0, fmt.Errorf("%w: size value %d exceeds maximum int64 value %d", ErrSizeOutOfRange, uint64(s), int64(math.MaxInt64))
	}
	return int64(s), nil
}

// UnmarshalText parses a byte size from text, allowing negative values.
func (s *SSize) UnmarshalText(text []byte) error {
	return unmarshalSize(s, text)
}

// MarshalText converts an SSize to a string for encoding toml.
func (s SSize) MarshalText() ([]byte, error) {
	return marshalSize(s)
}

// ToInt returns the value as an int, or an error wrapping ErrSizeOutOfRange
// if it does not fit. SSize is int64 so on 32-bit platforms values outside
// the int32 range cannot be represented as int. Callers passing an SSize to
// APIs that take int should go through ToInt rather than a bare cast.
func (s SSize) ToInt() (int, error) {
	if int64(s) > math.MaxInt || int64(s) < math.MinInt {
		return 0, fmt.Errorf("%w: ssize value %d is outside int range [%d, %d]", ErrSizeOutOfRange, int64(s), math.MinInt, math.MaxInt)
	}
	return int(s), nil
}

// ToUint64 returns the value as a uint64, or an error wrapping
// ErrSizeOutOfRange if it is negative. SSize is int64 so negative values
// silently wrap to large positive uint64 values when cast directly. ToUint64
// rejects those instead.
func (s SSize) ToUint64() (uint64, error) {
	if int64(s) < 0 {
		return 0, fmt.Errorf("%w: ssize value %d cannot be converted to uint64: negative", ErrSizeOutOfRange, int64(s))
	}
	return uint64(s), nil
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
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
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

// Interface types used by the reflect-based traversals below. Computing these
// once at package scope keeps VerifyConfigType and isLeafType free of repeated
// reflect.TypeOf calls.
var (
	defaulterType       = reflect.TypeOf((*Defaulter)(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

// requiresDefaulter reports whether a slice element type must implement
// Defaulter. Only struct (or pointer-to-struct) element types that don't
// implement TextUnmarshaler need it.
func requiresDefaulter(elemType reflect.Type) bool {
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

// walkForConfigType is the recursive worker used by VerifyConfigType. It
// walks the type tree rooted at t and returns a list of messages describing
// anything that would prevent the type from serving as a config:
//   - slice element types that must implement Defaulter but don't
//   - cycles in the type graph (recursive types cannot be represented in TOML)
//
// stack tracks types currently on the recursion path so a revisit is only
// flagged when it actually forms a cycle; a type that appears as two sibling
// fields of a struct is walked twice without being reported, because the
// first walk's deferred delete runs before the second begins.
func walkForConfigType(t reflect.Type, path string, stack map[reflect.Type]bool) []string {
	if stack[t] {
		return []string{fmt.Sprintf(
			"%s: type %s forms a cycle (recursive types cannot be represented in TOML)",
			path, t)}
	}
	stack[t] = true
	// Simply deleting the type when returning up the struct key is OK since cycles are an error.
	defer delete(stack, t)

	switch t.Kind() {
	case reflect.Pointer:
		return walkForConfigType(t.Elem(), path, stack)
	case reflect.Slice:
		var violations []string
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
					path, elem, ptrType))
			}
		}
		return append(violations, walkForConfigType(elem, path+"[]", stack)...)
	case reflect.Struct:
		var violations []string
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			if f.Tag.Get("toml") == "-" {
				continue
			}
			violations = append(violations, walkForConfigType(f.Type, path+"."+f.Name, stack)...)
		}
		return violations
	}
	return nil
}

// VerifyConfigType walks the type tree of cfg and reports an error if the
// type cannot serve as a valid configuration root. It currently checks:
//   - slice element types that would be appended via indexed env var
//     overrides must implement Defaulter
//   - the type graph must not contain cycles, which cannot be expressed in
//     TOML or via the environment variable override scheme in this package
//
// cfg may be a value or a pointer; the type tree is walked from its
// (dereferenced) type. Element types that implement encoding.TextUnmarshaler
// are exempt from the Defaulter check: they are treated as leaves by
// ApplyEnvOverrides and have no fields to default. Primitive element types
// (string, int, etc.) are also exempt for the same reason. Fields tagged
// `toml:"-"` and unexported fields are skipped because ApplyEnvOverrides
// skips them too.
//
// VerifyConfigType is intended to be called from a test in the package that
// owns the config root, as a CI safety net for the conventions that a config
// type must satisfy.
func VerifyConfigType(cfg interface{}) error {
	rootType := reflect.TypeOf(cfg)
	if rootType == nil {
		return errors.New("VerifyConfigType: cfg is nil")
	}
	rootName := rootType.String()
	if rootType.Kind() == reflect.Pointer {
		rootType = rootType.Elem()
		if rootType != nil {
			rootName = rootType.String()
		}
	}

	violations := walkForConfigType(rootType, rootName, make(map[reflect.Type]bool))
	if len(violations) > 0 {
		return fmt.Errorf("toml.VerifyConfigType found %d violation(s):\n  %s",
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
	if reflect.PointerTo(t).Implements(textUnmarshalerType) {
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
		var sliceResult envOverrideResult

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
					sliceResult.mergeAllVars(defaultResult)
				}

				// Apply the indexed environment variable as an override value.
				if indexedResult, err := applyEnvOverrides(getenv, indexedEnvName, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				} else {
					sliceResult.merge(indexedResult)
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
				// type-level defaults before applying any env vars. Precedence is:
				// ApplyDefaults < unindexed env defaults < indexed env vars.
				//
				// Both ApplyDefaults and the unindexed env default below mutate f eagerly
				// on every iteration, including the final probe iteration whose f is then
				// discarded. The cost is negligible and the alternative — deferring both
				// until we know the element will be appended — would require running them
				// in the same dependency order on a fresh f after the indexed check, with
				// no real benefit.
				//
				// For pointer slice elements (f is already a pointer), check the value
				// directly; otherwise check the address.
				var defaulter Defaulter
				if f.Kind() == reflect.Pointer {
					defaulter, _ = f.Interface().(Defaulter)
				} else if f.CanAddr() {
					defaulter, _ = f.Addr().Interface().(Defaulter)
				}
				if defaulter != nil {
					defaulter.ApplyDefaults()
				} else if requiresDefaulter(f.Type()) {
					// We should never hit this error in production because unit tests with
					// VerifyConfigType should prevent this from becoming an issue.
					return noResult, fmt.Errorf("%s: slice element type %s does not implement toml.Defaulter",
						structKey, f.Type())
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
					sliceResult.mergeAllVars(defaultResult)
					sliceResult.merge(indexedResult)
					// We found environment variables to override into newValue. Check for growth bound before appending.
					if idx-startLen >= MaxEnvSliceGrowth {
						overridesStr := "overrides"
						if len(indexedResult.IndexedVars) == 1 {
							overridesStr = "override"
						}
						return noResult, fmt.Errorf(
							"env %s %s would append more than %d elements", overridesStr, strings.Join(indexedResult.IndexedVars, ","), MaxEnvSliceGrowth)
					}

					element.Set(reflect.Append(element, f))
				} else {
					// We seem to have run past the end of the environment indices.
					envOutOfBounds = true
				}
			}
		}

		// Slices of leaf types also support setting using a comma-delimited list in the unindexed env var.
		// You can't mix unindexed and indexed leaf type overrides, because that leads to surprising
		// and highly unintuitive results.
		if isLeafType(element.Type().Elem()) && len(value) > 0 {
			if sliceResult.Applied {
				return noResult, fmt.Errorf("unindexed env override %s would conflict with indexed overrides (%s). Use either indexed or unindexed only for this config",
					prefix, strings.Join(sliceResult.IndexedVars, ","))
			}
			sliceResult.Applied = true
			sliceResult.insertVar(&sliceResult.AllVars, prefix)
			parts := strings.Split(value, ",")
			if len(parts) > MaxEnvSliceGrowth {
				return noResult, fmt.Errorf("env override %s has %d comma-separated values, exceeding maximum of %d", prefix, len(parts), MaxEnvSliceGrowth)
			}
			// Clear existing elements before applying the comma-delimited list. Create slice with zero values.
			element.Set(reflect.MakeSlice(element.Type(), len(parts), len(parts)))
			for idx, val := range parts {
				f := element.Index(idx)
				// The custom getenv returns val for any key, so the recursive call will
				// report prefix as applied. Since we already recorded prefix above, merge
				// deduplicates it automatically — no manual DeleteFunc needed.
				// Since we know this is a leaf type and not a struct, no other environment variables other
				// than prefix can be pulled in, and prefix is already in AllVars. Skipping a merge here prevents
				// polluting the indexed var list while still maintaining overall correctness.
				if _, err := applyEnvOverrides(func(n string) string { return val }, prefix, f, indexStructKey(structKey, idx)); err != nil {
					return noResult, err
				}
			}
		}

		return sliceResult, nil

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
