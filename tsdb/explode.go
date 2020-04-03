package tsdb

import (
	"encoding/binary"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
)

// DecodeName converts tsdb internal serialization back to organization and bucket IDs.
func DecodeName(name [16]byte) (org, bucket influxdb.ID) {
	org = influxdb.ID(binary.BigEndian.Uint64(name[0:8]))
	bucket = influxdb.ID(binary.BigEndian.Uint64(name[8:16]))
	return
}

// DecodeNameSlice converts tsdb internal serialization back to organization and bucket IDs.
func DecodeNameSlice(name []byte) (org, bucket influxdb.ID) {
	return influxdb.ID(binary.BigEndian.Uint64(name[0:8])), influxdb.ID(binary.BigEndian.Uint64(name[8:16]))
}

// EncodeName converts org/bucket pairs to the tsdb internal serialization
func EncodeName(org, bucket influxdb.ID) [16]byte {
	var nameBytes [16]byte
	binary.BigEndian.PutUint64(nameBytes[0:8], uint64(org))
	binary.BigEndian.PutUint64(nameBytes[8:16], uint64(bucket))
	return nameBytes
}

// EncodeNameSlice converts org/bucket pairs to the tsdb internal serialization but returns a byte slice.
func EncodeNameSlice(org, bucket influxdb.ID) []byte {
	buf := EncodeName(org, bucket)
	return buf[:]
}

// EncodeOrgName converts org to the tsdb internal serialization that may be used
// as a prefix when searching for keys matching a specific organization.
func EncodeOrgName(org influxdb.ID) [8]byte {
	var orgBytes [8]byte
	binary.BigEndian.PutUint64(orgBytes[0:8], uint64(org))
	return orgBytes
}

// EncodeNameString converts org/bucket pairs to the tsdb internal serialization
func EncodeNameString(org, bucket influxdb.ID) string {
	name := EncodeName(org, bucket)
	return string(name[:])
}

// ExplodePoints creates a list of points that only contains one field per point. It also
// moves the measurement to a tag, and changes the measurement to be the provided argument.
func ExplodePoints(org, bucket influxdb.ID, points []models.Point) ([]models.Point, error) {
	out := make([]models.Point, 0, len(points))

	// TODO(jeff): We should add a RawEncode() method or something to the influxdb.ID type
	// or we should use hex encoded measurement names. Either way, we shouldn't be doing a
	// decode of the encode here, and we don't want to depend on details of how the ID type
	// is represented.
	ob := EncodeName(org, bucket)
	name := string(ob[:])

	tags := make(models.Tags, 1)
	for _, pt := range points {
		tags = tags[:1] // reset buffer for next point.

		tags[0] = models.NewTag(models.MeasurementTagKeyBytes, pt.Name())
		pt.ForEachTag(func(k, v []byte) bool {
			tags = append(tags, models.NewTag(k, v))
			return true
		})

		t := pt.Time()
		itr := pt.FieldIterator()
		tags = append(tags, models.Tag{}) // Make room for field key and value.

		for itr.Next() {
			tags[len(tags)-1] = models.NewTag(models.FieldKeyTagKeyBytes, itr.FieldKey())

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
