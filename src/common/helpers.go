package common

import (
	"encoding/json"
	"protocol"
	"time"
)

func StringToSeriesArray(seriesString string) ([]*protocol.Series, error) {
	series := []*protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	return series, err
}

func CurrentTime() int64 {
	return time.Now().UnixNano() / int64(1000)
}
