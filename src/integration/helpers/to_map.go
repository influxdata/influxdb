package helpers

import (
	influxdb "github.com/influxdb/influxdb-go"
)

func ToMap(series *influxdb.Series) []map[string]interface{} {
	points := make([]map[string]interface{}, 0, len(series.Points))
	for _, p := range series.Points {
		point := map[string]interface{}{}
		for idx, column := range series.Columns {
			point[column] = p[idx]
		}
		points = append(points, point)
	}
	return points
}
