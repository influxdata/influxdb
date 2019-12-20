package tsm1

import (
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/value"
)

type (
	Value         = value.Value
	IntegerValue  = value.IntegerValue
	UnsignedValue = value.UnsignedValue
	FloatValue    = value.FloatValue
	BooleanValue  = value.BooleanValue
	StringValue   = value.StringValue
)

// NewValue returns a new Value with the underlying type dependent on value.
func NewValue(t int64, v interface{}) Value { return value.NewValue(t, v) }

// NewRawIntegerValue returns a new integer value.
func NewRawIntegerValue(t int64, v int64) IntegerValue { return value.NewRawIntegerValue(t, v) }

// NewRawUnsignedValue returns a new unsigned integer value.
func NewRawUnsignedValue(t int64, v uint64) UnsignedValue { return value.NewRawUnsignedValue(t, v) }

// NewRawFloatValue returns a new float value.
func NewRawFloatValue(t int64, v float64) FloatValue { return value.NewRawFloatValue(t, v) }

// NewRawBooleanValue returns a new boolean value.
func NewRawBooleanValue(t int64, v bool) BooleanValue { return value.NewRawBooleanValue(t, v) }

// NewRawStringValue returns a new string value.
func NewRawStringValue(t int64, v string) StringValue { return value.NewRawStringValue(t, v) }

// NewIntegerValue returns a new integer value.
func NewIntegerValue(t int64, v int64) Value { return value.NewIntegerValue(t, v) }

// NewUnsignedValue returns a new unsigned integer value.
func NewUnsignedValue(t int64, v uint64) Value { return value.NewUnsignedValue(t, v) }

// NewFloatValue returns a new float value.
func NewFloatValue(t int64, v float64) Value { return value.NewFloatValue(t, v) }

// NewBooleanValue returns a new boolean value.
func NewBooleanValue(t int64, v bool) Value { return value.NewBooleanValue(t, v) }

// NewStringValue returns a new string value.
func NewStringValue(t int64, v string) Value { return value.NewStringValue(t, v) }

// CollectionToValues takes in a series collection and returns it as a map of series key to
// values. It returns an error if any of the points could not be converted.
func CollectionToValues(collection *tsdb.SeriesCollection) (map[string][]Value, error) {
	values := make(map[string][]Value, collection.Length())
	var (
		keyBuf  []byte
		baseLen int
	)

	j := 0
	for citer := collection.Iterator(); citer.Next(); {
		keyBuf = append(keyBuf[:0], citer.Key()...)
		keyBuf = append(keyBuf, keyFieldSeparator...)
		baseLen = len(keyBuf)

		p := citer.Point()
		iter := p.FieldIterator()
		t := p.Time().UnixNano()

		for iter.Next() {
			keyBuf = append(keyBuf[:baseLen], iter.FieldKey()...)

			var v Value
			switch iter.Type() {
			case models.Float:
				fv, err := iter.FloatValue()
				if err != nil {
					return nil, err
				}
				v = NewFloatValue(t, fv)
			case models.Integer:
				iv, err := iter.IntegerValue()
				if err != nil {
					return nil, err
				}
				v = NewIntegerValue(t, iv)
			case models.Unsigned:
				iv, err := iter.UnsignedValue()
				if err != nil {
					return nil, err
				}
				v = NewUnsignedValue(t, iv)
			case models.String:
				v = NewStringValue(t, iter.StringValue())
			case models.Boolean:
				bv, err := iter.BooleanValue()
				if err != nil {
					return nil, err
				}
				v = NewBooleanValue(t, bv)
			default:
				return nil, fmt.Errorf("unknown field type for %s: %s",
					string(iter.FieldKey()), p.String())
			}

			vs, ok := values[string(keyBuf)]
			if ok && len(vs) > 0 && valueType(vs[0]) != valueType(v) {
				if collection.Reason == "" {
					collection.Reason = fmt.Sprintf(
						"conflicting field type: %s has field type %T but expected %T",
						citer.Key(), v.Value(), vs[0].Value())
				}
				collection.Dropped++
				collection.DroppedKeys = append(collection.DroppedKeys, citer.Key())
				continue
			}

			values[string(keyBuf)] = append(vs, v)
			collection.Copy(j, citer.Index())
			j++
		}
	}

	collection.Truncate(j)
	return values, nil
}

// ValuesToPoints takes in a map of values and returns a slice of models.Point.
func ValuesToPoints(values map[string][]Value) []models.Point {
	points := make([]models.Point, 0, len(values))
	for composite, vals := range values {
		series, field := SeriesAndFieldFromCompositeKey([]byte(composite))
		strField := string(field)
		for _, val := range vals {
			t := time.Unix(0, val.UnixNano())
			fields := models.Fields{strField: val.Value()}
			points = append(points, models.NewPointFromSeries(series, fields, t))
		}
	}
	return points
}
