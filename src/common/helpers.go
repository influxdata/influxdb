package common

import (
	"encoding/json"
	"protocol"
)

func StringToSeriesArray(seriesString string) ([]*protocol.Series, error) {
	series := []*protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	return series, err
}
