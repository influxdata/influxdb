package helpers

import "github.com/influxdb/influxdb/common"

func ToMap(series common.ApiSeries) []map[string]interface{} {
	seriesPoints := series.GetPoints()
	points := make([]map[string]interface{}, 0, len(seriesPoints))
	for _, p := range seriesPoints {
		point := map[string]interface{}{}
		for idx, column := range series.GetColumns() {
			point[column] = p[idx]
		}
		points = append(points, point)
	}
	return points
}
