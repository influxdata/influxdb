package collectd_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdb/influxdb/collectd"
	"github.com/kimor79/gollectd"
)

/*
This is a sample of what data can be represented like in json
[
   {
     "values": [197141504, 175136768],
     "dstypes": ["counter", "counter"],
     "dsnames": ["read", "write"],
     "time": 1251533299,
     "interval": 10,
     "host": "leeloo.lan.home.verplant.org",
     "plugin": "disk",
     "plugin_instance": "sda",
     "type": "disk_octets",
     "type_instance": ""
   },
   …
 ]


*/

func Test_Unmarshal_Metrics(t *testing.T) {
	var tests = []struct {
		name    string
		packet  gollectd.Packet
		metrics []collectd.Metric
	}{
		{
			name: "single value",
			metrics: []collectd.Metric{
				collectd.Metric{Name: "disk_read", Value: float64(1)},
			},
			packet: gollectd.Packet{
				Plugin: "disk",
				Values: []gollectd.Value{
					gollectd.Value{Name: "read", Value: 1},
				},
			},
		},
		{
			name: "multi value",
			metrics: []collectd.Metric{
				collectd.Metric{Name: "disk_read", Value: float64(1)},
				collectd.Metric{Name: "disk_write", Value: float64(5)},
			},
			packet: gollectd.Packet{
				Plugin: "disk",
				Values: []gollectd.Value{
					gollectd.Value{Name: "read", Value: 1},
					gollectd.Value{Name: "write", Value: 5},
				},
			},
		},
		{
			name: "tags",
			metrics: []collectd.Metric{
				collectd.Metric{
					Name:  "disk_read",
					Value: float64(1),
					Tags:  map[string]string{"host": "server01", "instance": "sdk", "type": "disk_octets", "type_instance": "single"},
				},
			},
			packet: gollectd.Packet{
				Plugin:         "disk",
				Hostname:       "server01",
				PluginInstance: "sdk",
				Type:           "disk_octets",
				TypeInstance:   "single",
				Values: []gollectd.Value{
					gollectd.Value{Name: "read", Value: 1},
				},
			},
		},
	}

	for _, test := range tests {
		t.Logf("testing %q", test.name)
		metrics := collectd.Unmarshal(&test.packet)
		if len(metrics) != len(test.metrics) {
			t.Errorf("metric len mismatch. expected %d, got %d", len(test.metrics), len(metrics))
		}
		for i, m := range test.metrics {
			// test name
			name := fmt.Sprintf("%s_%s", test.packet.Plugin, test.packet.Values[i].Name)
			if m.Name != name {
				t.Errorf("metric name mismatch. expected %q, got %q", name, m.Name)
			}
			// test value
			mv := m.Value.(float64)
			pv := test.packet.Values[i].Value
			if mv != pv {
				t.Errorf("metric value mismatch. expected %v, got %v", pv, mv)
			}
			// test tags
			if test.packet.Hostname != m.Tags["host"] {
				t.Errorf(`metric tags["host"] mismatch. expected %q, got %q`, test.packet.Hostname, m.Tags["host"])
			}
			if test.packet.PluginInstance != m.Tags["instance"] {
				t.Errorf(`metric tags["instance"] mismatch. expected %q, got %q`, test.packet.PluginInstance, m.Tags["instance"])
			}
			if test.packet.Type != m.Tags["type"] {
				t.Errorf(`metric tags["type"] mismatch. expected %q, got %q`, test.packet.Type, m.Tags["type"])
			}
			if test.packet.TypeInstance != m.Tags["type_instance"] {
				t.Errorf(`metric tags["type_instance"] mismatch. expected %q, got %q`, test.packet.TypeInstance, m.Tags["type_instance"])
			}
		}
	}
}

func Test_Unmarshal_Time(t *testing.T) {
	testTime := time.Now().UTC().Round(time.Microsecond)
	var timeHR = func(tm time.Time) uint64 {
		sec, nsec := tm.Unix(), tm.UnixNano()%1000000000
		hr := (sec << 30) + (nsec * 1000000000 / 1073741824)
		return uint64(hr)
	}

	var tests = []struct {
		name    string
		packet  gollectd.Packet
		metrics []collectd.Metric
	}{
		{
			name: "Should parse timeHR properly",
			packet: gollectd.Packet{
				TimeHR: timeHR(testTime),
				Values: []gollectd.Value{
					gollectd.Value{
						Value: 1,
					},
				},
			},
			metrics: []collectd.Metric{
				collectd.Metric{Timestamp: testTime},
			},
		},
		{
			name: "Should parse time properly",
			packet: gollectd.Packet{
				Time: uint64(testTime.Round(time.Second).Unix()),
				Values: []gollectd.Value{
					gollectd.Value{
						Value: 1,
					},
				},
			},
			metrics: []collectd.Metric{
				collectd.Metric{
					Timestamp: testTime.Round(time.Second),
				},
			},
		},
	}

	for _, test := range tests {
		t.Logf("testing %q", test.name)
		metrics := collectd.Unmarshal(&test.packet)
		if len(metrics) != len(test.metrics) {
			t.Errorf("metric len mismatch. expected %d, got %d", len(test.metrics), len(metrics))
		}
		for _, m := range metrics {
			if test.packet.TimeHR > 0 {
				if m.Timestamp.Format(time.RFC3339Nano) != testTime.Format(time.RFC3339Nano) {
					t.Errorf("timestamp mis-match, got %v, expected %v", m.Timestamp.Format(time.RFC3339Nano), testTime.Format(time.RFC3339Nano))
				} else if m.Timestamp.Format(time.RFC3339) != testTime.Format(time.RFC3339) {
					t.Errorf("timestamp mis-match, got %v, expected %v", m.Timestamp.Format(time.RFC3339), testTime.Format(time.RFC3339))
				}
			}
		}
	}
}
