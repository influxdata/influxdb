package check

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
)

// UnitSeconds is the Unit of a Measure whose values are durations. Durations
// are reported as float seconds, unrounded, whatever rounding the check's
// message applies.
const UnitSeconds = "seconds"

// measureUnitKey is the JSON key that carries a Measure's Unit beside its
// values. It is reserved: no value may use it as a key.
const measureUnitKey = "unit"

// Measure is a group of numbers sharing one unit, reported by a check beside
// its message so a poller can read them without parsing the message text. It
// renders as a flat JSON object of its values plus a "unit" string:
//
//	{"completed":94,"total":200,"unit":"shards"}
//
// A Measure attached to a response through WithMeasure is copied and
// validated; see there for the rules.
type Measure struct {
	Unit   string
	Values map[string]float64
}

// Measures maps a group name to its Measure. Each group renders as its own
// key on the check object, beside name, status and message.
type Measures map[string]Measure

// MarshalJSON renders the values in sorted key order followed by "unit", so
// the bytes are deterministic. A non-finite value fails the marshal, as it
// would for any float64; WithMeasure drops them before they get here.
func (m Measure) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for _, k := range slices.Sorted(maps.Keys(m.Values)) {
		if err := writeJSONMember(&buf, k, m.Values[k]); err != nil {
			return nil, fmt.Errorf("marshaling measure value %q: %w", k, err)
		}
		buf.WriteByte(',')
	}
	if err := writeJSONMember(&buf, measureUnitKey, m.Unit); err != nil {
		return nil, fmt.Errorf("marshaling measure unit: %w", err)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON accepts an object whose "unit" is a string and whose every
// other member is a number. Anything else is an error, which is how
// BasicResponse.UnmarshalJSON tells a measure group from an unrelated key.
func (m *Measure) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decoding measure: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("decoding measure: not an object")
	}
	rawUnit, ok := raw[measureUnitKey]
	if !ok {
		return fmt.Errorf("decoding measure: missing %q", measureUnitKey)
	}
	var out Measure
	if isJSONNull(rawUnit) {
		return fmt.Errorf("decoding measure %q: null", measureUnitKey)
	}
	if err := json.Unmarshal(rawUnit, &out.Unit); err != nil {
		return fmt.Errorf("decoding measure %q: %w", measureUnitKey, err)
	}
	delete(raw, measureUnitKey)
	if len(raw) > 0 {
		out.Values = make(map[string]float64, len(raw))
	}
	for k, v := range raw {
		// encoding/json leaves a float64 untouched for null rather than
		// failing, which would turn a missing number into a reported 0.
		if isJSONNull(v) {
			return fmt.Errorf("decoding measure value %q: null", k)
		}
		var f float64
		if err := json.Unmarshal(v, &f); err != nil {
			return fmt.Errorf("decoding measure value %q: %w", k, err)
		}
		out.Values[k] = f
	}
	*m = out
	return nil
}

// isJSONNull reports whether raw is the JSON literal null.
func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// cleanMeasure returns a copy of m fit to be served under group, and false if
// nothing usable is left. The group must be a valid identifier that does not
// collide with a check's own fields; the unit must be a valid identifier;
// each value key must be a valid identifier other than "unit", and each value
// must be finite, since encoding/json cannot encode NaN or ±Inf and one bad
// value would otherwise fail the whole /health body. Invalid entries are
// dropped rather than reported: a health check must not fail to render
// because a producer handed it a bad number.
func cleanMeasure(group string, m Measure) (Measure, bool) {
	if !validIdent(group) || isReservedField(group) || !validIdent(m.Unit) {
		return Measure{}, false
	}
	out := Measure{Unit: m.Unit, Values: make(map[string]float64, len(m.Values))}
	for k, v := range m.Values {
		if k == measureUnitKey || !validIdent(k) || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		out.Values[k] = v
	}
	if len(out.Values) == 0 {
		return Measure{}, false
	}
	return out, true
}

// isReservedField reports whether key is one of the check object's own
// fields, which a measure group may not shadow.
func isReservedField(key string) bool {
	switch key {
	case "name", "status", "message", "checks":
		return true
	default:
		return false
	}
}

// validIdent reports whether s is a non-empty run of [a-z0-9_]. Group names,
// value keys and units become JSON keys and metric labels on the polling
// side, so they are held to a set that is safe in both.
func validIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// writeJSONMember appends `"key":value` to buf, encoding both with
// encoding/json so escaping and number formatting match the rest of the body.
func writeJSONMember(buf *bytes.Buffer, key string, value any) error {
	k, err := json.Marshal(key)
	if err != nil {
		return err
	}
	v, err := json.Marshal(value)
	if err != nil {
		return err
	}
	buf.Write(k)
	buf.WriteByte(':')
	buf.Write(v)
	return nil
}
