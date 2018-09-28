package tsdb

import (
	"errors"

	"github.com/influxdata/platform/models"
)

// ExplodePoints creates a list of points that only contains one field per point. It also
// moves the measurement to a tag, and changes the measurement to be the provided argument.
func ExplodePoints(org, bucket []byte, points []models.Point) ([]models.Point, error) {
	if len(org) != 8 || len(bucket) != 8 {
		return nil, errors.New("invalid org/bucket")
	}

	out := make([]models.Point, 0, len(points))
	name := string(org) + string(bucket)

	var tags models.Tags
	for _, pt := range points {
		t := pt.Time()

		itr := pt.FieldIterator()
		for itr.Next() {
			tags = tags[:0]
			tags = append(tags, models.NewTag([]byte("_f"), itr.FieldKey()))
			tags = append(tags, models.NewTag([]byte("_m"), pt.Name()))
			pt.ForEachTag(func(k, v []byte) bool {
				tags = append(tags, models.NewTag(k, v))
				return true
			})

			var err error
			field := make(models.Fields, 1)
			switch itr.Type() {
			case models.Float:
				field[string(itr.FieldKey())], err = itr.FloatValue()
			case models.Integer:
				field[string(itr.FieldKey())], err = itr.IntegerValue()
			case models.Boolean:
				field[string(itr.FieldKey())], err = itr.BooleanValue()
			case models.String:
				field[string(itr.FieldKey())] = itr.StringValue()
			case models.Unsigned:
				field[string(itr.FieldKey())], err = itr.UnsignedValue()
			}
			if err != nil {
				return nil, err
			}

			pt, err := models.NewPoint(name, tags, field, t)
			if err != nil {
				return nil, err
			}
			out = append(out, pt)
		}
	}

	return out, nil
}
