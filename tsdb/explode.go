package tsdb

import (
	"encoding/binary"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/models"
)

// Values used to store the field key and measurement name as tags.
const (
	FieldKeyTagKey    = "_f"
	MeasurementTagKey = "_m"
)

var (
	FieldKeyTagKeyBytes    = []byte(FieldKeyTagKey)
	MeasurementTagKeyBytes = []byte(MeasurementTagKey)
)

// ExplodePoints creates a list of points that only contains one field per point. It also
// moves the measurement to a tag, and changes the measurement to be the provided argument.
func ExplodePoints(org, bucket platform.ID, points []models.Point) ([]models.Point, error) {
	out := make([]models.Point, 0, len(points))

	// TODO(jeff): We should add a RawEncode() method or something to the platform.ID type
	// or we should use hex encoded measurement names. Either way, we shouldn't be doing a
	// decode of the encode here, and we don't want to depend on details of how the ID type
	// is represented.

	var nameBytes [16]byte

	binary.BigEndian.PutUint64(nameBytes[0:8], uint64(org))
	binary.BigEndian.PutUint64(nameBytes[8:16], uint64(bucket))
	name := string(nameBytes[:])

	var tags models.Tags
	for _, pt := range points {
		t := pt.Time()

		itr := pt.FieldIterator()
		for itr.Next() {
			tags = tags[:0]
			tags = append(tags, models.NewTag(FieldKeyTagKeyBytes, itr.FieldKey()))
			tags = append(tags, models.NewTag(MeasurementTagKeyBytes, pt.Name()))
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
