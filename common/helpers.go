package common

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/influxdb/influxdb/protocol"
)

// Returns the parsed duration in nanoseconds, support 'u', 's', 'm',
// 'h', 'd' and 'w' suffixes.
func ParseTimeDuration(value string) (int64, error) {
	var constant time.Duration
	prefixSize := 1

	switch value[len(value)-1] {
	case 'u':
		constant = time.Microsecond
	case 's':
		constant = time.Second
	case 'm':
		constant = time.Minute
	case 'h':
		constant = time.Hour
	case 'd':
		constant = 24 * time.Hour
	case 'w':
		constant = 7 * 24 * time.Hour
	case 'y':
		constant = 365 * 24 * time.Hour
	default:
		prefixSize = 0
	}

	if value[len(value)-2:] == "ms" {
		constant = time.Millisecond
		prefixSize = 2
	}

	t := big.Rat{}
	timeString := value
	if prefixSize > 0 {
		timeString = value[:len(value)-prefixSize]
	}

	_, err := fmt.Sscan(timeString, &t)
	if err != nil {
		return 0, err
	}

	if prefixSize > 0 {
		c := big.Rat{}
		c.SetFrac64(int64(constant), 1)
		t.Mul(&t, &c)
	}
	if t.IsInt() {
		return t.Num().Int64(), nil
	}
	f, _ := t.Float64()
	return int64(f), nil
}

func GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func StringToSeriesArray(seriesString string, args ...interface{}) ([]*protocol.Series, error) {
	seriesString = fmt.Sprintf(seriesString, args...)
	series := []*protocol.Series{}
	err := json.Unmarshal([]byte(seriesString), &series)
	return series, err
}

func CurrentTime() int64 {
	return time.Now().UnixNano() / int64(1000)
}
