package collectd

import (
	"fmt"
	"time"

	"github.com/kimor79/gollectd"
)

// TODO corylanou: This needs to be made a public `main.Point` so we can share this across packages.
type Metric struct {
	Name      string
	Tags      map[string]string
	Value     interface{}
	Timestamp time.Time
}

func Unmarshal(data *gollectd.Packet) []Metric {
	// Prefer high resolution timestamp.
	var timeStamp time.Time
	if data.TimeHR > 0 {
		// TimeHR is "near" nanosecond measurement, but not exactly nanasecond time
		// Since we store time in microseconds, we round here (mostly so tests will work easier)
		sec := data.TimeHR >> 30
		// Shifting, masking, and dividing by 1 billion to get nanoseconds.
		nsec := ((data.TimeHR & 0x3FFFFFFF) << 30) / 1000 / 1000 / 1000
		timeStamp = time.Unix(int64(sec), int64(nsec)).UTC().Round(time.Microsecond)
	} else {
		// If we don't have high resolution time, fall back to basic unix time
		timeStamp = time.Unix(int64(data.Time), 0).UTC()
	}

	var m []Metric
	for i, _ := range data.Values {
		metric := Metric{Name: fmt.Sprintf("%s_%s", data.Plugin, data.Values[i].Name)}
		metric.Value = data.Values[i].Value
		metric.Timestamp = timeStamp

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
	return m
}
