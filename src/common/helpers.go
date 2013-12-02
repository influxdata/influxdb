package common

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
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

func RingLocation(database *string, timeSeries *string, time *int64) int {
	hasher := sha1.New()
	hasher.Write([]byte(fmt.Sprintf("%s%s%d", *database, *timeSeries, *time)))
	buf := bytes.NewBuffer(hasher.Sum(nil))
	var n int64
	binary.Read(buf, binary.LittleEndian, &n)
	nInt := int(n)
	if nInt < 0 {
		nInt = nInt * -1
	}
	return nInt
}
