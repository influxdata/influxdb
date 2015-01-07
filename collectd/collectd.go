package collectd

import (
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/kimor79/gollectd"
)

var ErrTimeOutOfBounds = errors.New("The time is to large to store")

//TODO corylanou: This needs to be made a public `main.Point` so we can share this across packages.
type Metrics []Metric
type Metric struct {
	Name      string
	Tags      map[string]string
	Value     interface{}
	Timestamp time.Time
}

func Unmarshal(data *gollectd.Packet) (Metrics, error) {
	// Prefer high resolution timestamp.  TimeHR is 2^-30 seconds, so shift
	// right 30 to get seconds then convert to microseconds for InfluxDB
	unixTime := (data.TimeHR >> 30) * 1000 * 1000

	// Fallback on unix timestamp if high res is 0
	if unixTime == 0 {
		unixTime = data.Time * 1000 * 1000
	}

	// Check to see if the unixTime is too large.  If so, log an error
	if unixTime > math.MaxInt64 {
		log.Println("Collectd timestamp too large for InfluxDB.  Wrapping around to 0.")
		return Metrics{}, ErrTimeOutOfBounds
	}

	// Collectd time is uint64 but influxdb expects int64
	//ts := int64(unixTime % math.MaxInt64)

	var m Metrics
	for i, _ := range data.Values {
		metric := Metric{Name: fmt.Sprintf("%s_%s", data.Plugin, data.Values[i].Name)}
		metric.Value = data.Values[i].Value
		metric.Timestamp = time.Unix(0, int64(unixTime)*int64(time.Millisecond))

		if data.Hostname != "" {
			metric.Tags["host"] = data.Hostname
		}
		if data.PluginInstance != "" {
			metric.Tags["instance"] = data.PluginInstance
		}
		if data.Type != "" {
			metric.Tags["type"] = data.Type
		}
		if data.TypeInstance != "" {
			metric.Tags["type_instance"] = data.TypeInstance
		}
		m = append(m, metric)
	}
	return m, nil
}
