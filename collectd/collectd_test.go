package collectd_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/influxdb/influxdb/collectd"
	"github.com/kimor79/gollectd"
)

type testServer string

func (testServer) WriteSeries(database, retentionPolicy, name string, tags map[string]string, timestamp time.Time, values map[string]interface{}) error {
	return nil
}

func TestServer_ListenAndServe_ErrBindAddressRequired(t *testing.T) {
	var (
		ts  testServer
		s   = collectd.NewServer(ts, "foo")
		err = collectd.ErrBindAddressRequired
	)

	e := s.ListenAndServe("")
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrDatabaseNotSpecified(t *testing.T) {
	var (
		ts  testServer
		s   = collectd.NewServer(ts, "foo")
		err = collectd.ErrDatabaseNotSpecified
	)

	e := s.ListenAndServe("127.0.0.1:25826")
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrCouldNotParseTypesDBFile(t *testing.T) {
	var (
		ts  testServer
		s   = collectd.NewServer(ts, "foo")
		err = collectd.ErrCouldNotParseTypesDBFile
	)

	s.Database = "foo"
	e := s.ListenAndServe("127.0.0.1:25829")
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_Success(t *testing.T) {
	var (
		ts testServer
		// You can typically find this on your mac here: "/usr/local/Cellar/collectd/5.4.1/share/collectd/types.db"
		s = collectd.NewServer(ts, "./collectd_test.conf")
	)

	s.Database = "counter"
	e := s.ListenAndServe("127.0.0.1:25830")
	defer s.Close()
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrResolveUDPAddr(t *testing.T) {
	var (
		ts  testServer
		s   = collectd.NewServer(ts, "./collectd_test.conf")
		err = collectd.ErrResolveUDPAddr
	)

	s.Database = "counter"
	e := s.ListenAndServe("foo")
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func TestServer_ListenAndServe_ErrListenUDP(t *testing.T) {
	var (
		ts  testServer
		s   = collectd.NewServer(ts, "./collectd_test.conf")
		err = collectd.ErrListenUDP
	)

	//Open a udp listener on the port prior to force it to err
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:25826")
	conn, _ := net.ListenUDP("udp", addr)
	defer conn.Close()

	s.Database = "counter"
	e := s.ListenAndServe("127.0.0.1:25826")
	if e != err {
		t.Fatalf("err does not match.  expected %v, got %v", err, e)
	}
}

func Test_Unmarshal_Metrics(t *testing.T) {
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
	// Its important to remember that collectd stores high resolution time
	// as "near" nanoseconds (2^30) so we have to take that into account
	// when feeding time into the test.
	// Since we only store microseconds, we round it off (mostly to make testing easier)
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
