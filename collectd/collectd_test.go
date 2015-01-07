package collectd_test

import (
	"log"
	"testing"

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
	t.Skip()
	var (
		//overflowTimeHR uint64 = (((math.MaxInt64 + 1) / 1000) / 1000) << 30
		//overflowTime   uint64 = (((math.MaxInt64 + 1) / 1000) / 1000) << 30
		overflowTimeHR uint64
		overflowTime   uint64
	)

	var tests = []struct {
		name    string
		packet  gollectd.Packet
		metrics collectd.Metrics
		err     error
	}{
		{
			name: "Should error out if time is to large",
			packet: gollectd.Packet{
				Time:   overflowTime,
				TimeHR: overflowTimeHR,
			},
			metrics: collectd.Metrics{},
			err:     collectd.ErrTimeOutOfBounds,
		},
	}

	for _, test := range tests {
		log.Printf("%+v", test)
		m, err := collectd.Unmarshal(&test.packet)
		if err != test.err {
			t.Errorf("error mismatch, expected %v, got %v", test.err, err)
		}
		_ = m

	}

}
