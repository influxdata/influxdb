package common

import (
	"encoding/json"
	"fmt"
	"protocol"
	"time"
)

func StringToSeriesArray(seriesString string, args ...interface{}) ([]*protocol.Series, error) {
	seriesString = fmt.Sprintf(seriesString, args...)
	series := []*protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	return series, err
}

func CurrentTime() int64 {
	return time.Now().UnixNano() / int64(1000)
}
