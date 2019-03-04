package tsdb

import (
	"encoding/binary"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
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

// DecodeName converts tsdb internal serialization back to organization and bucket IDs.
func DecodeName(name [16]byte) (org, bucket platform.ID) {
	org = platform.ID(binary.BigEndian.Uint64(name[0:8]))
	bucket = platform.ID(binary.BigEndian.Uint64(name[8:16]))
	return
}

// EncodeName converts org/bucket pairs to the tsdb internal serialization
func EncodeName(org, bucket platform.ID) [16]byte {
	var nameBytes [16]byte
	binary.BigEndian.PutUint64(nameBytes[0:8], uint64(org))
	binary.BigEndian.PutUint64(nameBytes[8:16], uint64(bucket))
	return nameBytes
}

// ExplodePoints creates a list of points that only contains one field per point. It also
// moves the measurement to a tag, and changes the measurement to be the provided argument.
func ExplodePoints(org, bucket platform.ID, points []models.Point) ([]models.Point, error) {
	out := make([]models.Point, 0, len(points))

	// TODO(jeff): We should add a RawEncode() method or something to the platform.ID type
	// or we should use hex encoded measurement names. Either way, we shouldn't be doing a
	// decode of the encode here, and we don't want to depend on details of how the ID type
	// is represented.
	ob := EncodeName(org, bucket)
	name := string(ob[:])

	tags := make(models.Tags, 2)
	for _, pt := range points {
		tags = tags[:2]
		tags[1] = models.NewTag(MeasurementTagKeyBytes, pt.Name())
		pt.ForEachTag(func(k, v []byte) bool {
			tags = append(tags, models.NewTag(k, v))
			return true
		})

		t := pt.Time()
		itr := pt.FieldIterator()
		for itr.Next() {
			tags[0] = models.NewTag(FieldKeyTagKeyBytes, itr.FieldKey())

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
