package collectd_test

import (
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

func Test_UnmarshalPacket(t *testing.T) {
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
						TypeName: "cpu",
						Value:    1,
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
						TypeName: "cpu",
						Value:    1,
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
