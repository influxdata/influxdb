package datastore

import (
	"bytes"
	"encoding/binary"
	"math"
	"time"

	"github.com/influxdb/influxdb/common"
)

func convertTimestampToUint(t int64) uint64 {
	if t < 0 {
		return uint64(math.MaxInt64 + t + 1)
	}
	return uint64(t) + uint64(math.MaxInt64) + uint64(1)
}

func convertUintTimestampToInt64(t uint64) int64 {
	if t > uint64(math.MaxInt64) {
		return int64(t-math.MaxInt64) - int64(1)
	}
	return int64(t) - math.MaxInt64 - int64(1)
}

func byteArrayForTime(t time.Time) []byte {
	timeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	timeMicro := common.TimeToMicroseconds(t)
	binary.Write(timeBuffer, binary.BigEndian, convertTimestampToUint(timeMicro))
	return timeBuffer.Bytes()
}
