package tsdb

import "github.com/influxdata/platform/models"

// explodePoints creates a list of points that only contains one field per point. It also
// moves the measurement to a tag, and changes the measurement to be the provided argument.
func explodePoints(measurement []byte, points []models.Point) ([]models.Point, error) {
	out := make([]models.Point, 0, len(points))
	name := string(measurement)

	for _, pt := range points {
		otags, t := pt.Tags().Clone(), pt.Time()
		otags = append(otags, models.NewTag([]byte("_m"), pt.Name()))

		fields, err := pt.Fields()
		if err != nil {
			return nil, err
		}

		for key, val := range fields {
			tags := append(otags, models.Tag{Key: []byte("_f"), Value: []byte(key)})
			pt, err := models.NewPoint(name, tags, models.Fields{key: val}, t)
			if err != nil {
				return nil, err
			}
			out = append(out, pt)
		}
	}

	return out, nil
}
