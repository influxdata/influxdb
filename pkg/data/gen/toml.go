package gen

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cast"
)

type SeriesLimit int64

func (s *SeriesLimit) UnmarshalTOML(data interface{}) error {
	v, ok := data.(int64)
	if !ok {
		return errors.New("series-limit: invalid value")
	}

	if v < 0 {
		return errors.New("series-limit: must be ≥ 0")
	}

	*s = SeriesLimit(v)
	return nil
}

type sample float64

func (s *sample) UnmarshalTOML(data interface{}) error {
	v, ok := data.(float64)
	if !ok {
		return errors.New("sample: must be a float")
	}

	if v <= 0 || v > 1.0 {
		return errors.New("sample: must be 0 < sample ≤ 1.0")
	}

	*s = sample(v)

	return nil
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalTOML(data interface{}) error {
	text, ok := data.(string)
	if !ok {
		return fmt.Errorf("invalid duration, expect a Go duration as a string: %T", data)
	}

	return d.UnmarshalText([]byte(text))
}

func (d *duration) UnmarshalText(text []byte) error {
	s := string(text)

	var err error
	d.Duration, err = time.ParseDuration(s)
	if err != nil {
		return err
	}

	if d.Duration == 0 {
		d.Duration, err = time.ParseDuration("1" + s)
		if err != nil {
			return err
		}
	}

	if d.Duration <= 0 {
		return fmt.Errorf("invalid duration, must be > 0: %s", d.Duration)
	}
	return nil
}

type precision byte

const (
	precisionMillisecond precision = iota // default
	precisionNanosecond
	precisionMicrosecond
	precisionSecond
	precisionMinute
	precisionHour
)

var precisionToDuration = [...]time.Duration{
	time.Millisecond,
	time.Nanosecond,
	time.Microsecond,
	time.Second,
	time.Minute,
	time.Minute * 60,
	time.Nanosecond,
	time.Nanosecond,
}

func (p *precision) ToDuration() time.Duration {
	return precisionToDuration[*p&0x7]
}

func (p *precision) UnmarshalTOML(data interface{}) error {
	d, ok := data.(string)
	if !ok {
		return fmt.Errorf("invalid precision, expect one of (ns, us, ms, s, m, h): %T", data)
	}

	d = strings.ToLower(d)

	switch d {
	case "ns", "nanosecond":
		*p = precisionNanosecond
	case "us", "microsecond", "µs":
		*p = precisionMicrosecond
	case "ms", "millisecond":
		*p = precisionMillisecond
	case "s", "second":
		*p = precisionSecond
	case "m", "minute":
		*p = precisionMinute
	case "h", "hour":
		*p = precisionHour
	default:
		return fmt.Errorf("invalid precision, expect one of (ns, ms, s, m, h): %s", d)
	}
	return nil
}

func (t *Tag) UnmarshalTOML(data interface{}) error {
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	if n, ok := d["name"].(string); !ok || n == "" {
		return errors.New("tag: missing or invalid value for name")
	} else {
		t.Name = n
	}

	// infer source

	if _, ok := d["source"]; !ok {
		return fmt.Errorf("missing source for tag %q", t.Name)
	}

	switch v := d["source"].(type) {
	case int64, string, float64, bool:
		if src, err := decodeTagConstantSource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	case []interface{}:
		if src, err := decodeTagArraySource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	case map[string]interface{}:
		if src, err := decodeTagSource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	default:
		return fmt.Errorf("invalid source for tag %q: %T", t.Name, v)
	}

	return nil
}

func decodeTagConstantSource(data interface{}) (TagSource, error) {
	switch data.(type) {
	case int64, string, float64, bool:
		if src, err := cast.ToStringE(data); err != nil {
			return nil, err
		} else {
			return &TagArraySource{Values: []string{src}}, nil
		}
	}

	return nil, errors.New("invalid constant tag source")
}

func decodeTagArraySource(data []interface{}) (TagSource, error) {
	if len(data) == 0 {
		return nil, errors.New("empty array source")
	}

	if src, err := cast.ToStringSliceE(data); err != nil {
		return nil, err
	} else {
		return &TagArraySource{Values: src}, nil
	}
}

func decodeTagSource(data map[string]interface{}) (TagSource, error) {
	typ, ok := data["type"].(string)
	if !ok {
		return nil, errors.New("missing type field")
	}
	switch typ {
	case "sequence":
		return decodeTagSequenceSource(data)
	case "file":
		return decodeTagFileSource(data)
	default:
		return nil, fmt.Errorf("invalid type field %q", typ)
	}
}

func decodeTagFileSource(data map[string]interface{}) (TagSource, error) {
	var s TagFileSource

	if v, ok := data["path"].(string); ok {
		s.Path = v
	} else {
		return nil, errors.New("file: missing path")
	}

	return &s, nil
}

func decodeTagSequenceSource(data map[string]interface{}) (TagSource, error) {
	var s TagSequenceSource

	if v, ok := data["format"].(string); ok {
		// TODO(sgc): validate format string
		s.Format = v
	} else {
		s.Format = "value%s"
	}

	if v, ok := data["start"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("tag.sequence: invalid start, %v", err)
		} else if v < 0 {
			return nil, fmt.Errorf("tag.sequence: start must be ≥ 0")
		} else {
			s.Start = v
		}
	}

	if v, ok := data["count"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("tag.sequence: invalid count, %v", err)
		} else if v < 0 {
			return nil, fmt.Errorf("tag.sequence: count must be > 0")
		} else {
			s.Count = v
		}
	} else {
		return nil, fmt.Errorf("tag.sequence: missing count")
	}

	return &s, nil
}

func (t *Field) UnmarshalTOML(data interface{}) error {
	d, ok := data.(map[string]interface{})
	if !ok {
		return nil
	}

	if n, ok := d["name"].(string); !ok || n == "" {
		return errors.New("field: missing or invalid value for name")
	} else {
		t.Name = n
	}

	if n, ok := d["count"]; !ok {
		return errors.New("field: missing value for count")
	} else if count, err := cast.ToInt64E(n); err != nil {
		return fmt.Errorf("field: invalid count, %v", err)
	} else if count <= 0 {
		return errors.New("field: count must be > 0")
	} else {
		t.Count = count
	}

	if n, ok := d["time-precision"]; ok {
		var tp precision
		if err := tp.UnmarshalTOML(n); err != nil {
			return err
		}
		t.TimePrecision = &tp
	}

	if n, ok := d["time-interval"]; ok {
		var ti duration
		if err := ti.UnmarshalTOML(n); err != nil {
			return err
		}
		t.TimeInterval = &ti
		t.TimePrecision = nil
	}

	if t.TimePrecision == nil && t.TimeInterval == nil {
		var tp precision
		t.TimePrecision = &tp
	}

	// infer source
	if _, ok := d["source"]; !ok {
		return fmt.Errorf("missing source for field %q", t.Name)
	}

	switch v := d["source"].(type) {
	case int64, string, float64, bool:
		t.Source = &FieldConstantValue{v}
	case []interface{}:
		if src, err := decodeFieldArraySource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	case map[string]interface{}:
		if src, err := decodeFieldSource(v); err != nil {
			return err
		} else {
			t.Source = src
		}
	default:
		// unknown
		return fmt.Errorf("invalid source for tag %q: %T", t.Name, v)
	}

	return nil
}

func decodeFieldArraySource(data []interface{}) (FieldSource, error) {
	if len(data) == 0 {
		return nil, errors.New("empty array")
	}

	var (
		src interface{}
		err error
	)

	// use first value to determine slice type
	switch data[0].(type) {
	case int64:
		src, err = toInt64SliceE(data)
	case float64:
		src, err = toFloat64SliceE(data)
	case string:
		src, err = cast.ToStringSliceE(data)
	case bool:
		src, err = cast.ToBoolSliceE(data)
	default:
		err = fmt.Errorf("unsupported field source data type: %T", data[0])
	}

	if err != nil {
		return nil, err
	}

	return &FieldArraySource{Value: src}, nil
}

func decodeFieldSource(data map[string]interface{}) (FieldSource, error) {
	typ, ok := data["type"].(string)
	if !ok {
		return nil, errors.New("missing type field")
	}
	switch typ {
	case "rand<float>":
		return decodeFloatRandomSource(data)
	case "zipf<integer>":
		return decodeIntegerZipfSource(data)
	default:
		return nil, fmt.Errorf("invalid type field %q", typ)
	}
}

func decodeFloatRandomSource(data map[string]interface{}) (FieldSource, error) {
	var s FieldFloatRandomSource

	if v, ok := data["seed"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid seed, %v", err)
		} else {
			s.Seed = v
		}
	}

	if v, ok := data["min"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid min, %v", err)
		} else {
			s.Min = v
		}
	}

	if v, ok := data["max"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil {
			return nil, fmt.Errorf("rand<float>: invalid max, %v", err)
		} else {
			s.Max = v
		}
	} else {
		s.Max = 1.0
	}

	if !(s.Min <= s.Max) {
		return nil, errors.New("rand<float>: min ≤ max")
	}

	return &s, nil
}

func decodeIntegerZipfSource(data map[string]interface{}) (FieldSource, error) {
	var s FieldIntegerZipfSource

	if v, ok := data["seed"]; ok {
		if v, err := cast.ToInt64E(v); err != nil {
			return nil, fmt.Errorf("zipf<integer>: invalid seed, %v", err)
		} else {
			s.Seed = v
		}
	}

	if v, ok := data["s"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil || v <= 1.0 {
			return nil, fmt.Errorf("zipf<integer>: invalid value for s (s > 1), %v", err)
		} else {
			s.S = v
		}
	} else {
		return nil, fmt.Errorf("zipf<integer>: missing value for s")
	}

	if v, ok := data["v"]; ok {
		if v, err := cast.ToFloat64E(v); err != nil || v < 1.0 {
			return nil, fmt.Errorf("zipf<integer>: invalid value for v (v ≥ 1), %v", err)
		} else {
			s.V = v
		}
	} else {
		return nil, fmt.Errorf("zipf<integer>: missing value for v")
	}

	if v, ok := data["imax"]; ok {
		if v, err := cast.ToUint64E(v); err != nil {
			return nil, fmt.Errorf("zipf<integer>: invalid value for imax, %v", err)
		} else {
			s.IMAX = v
		}
	} else {
		return nil, fmt.Errorf("zipf<integer>: missing value for imax")
	}

	return &s, nil
}
