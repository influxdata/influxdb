package collectd_test

import (
	"encoding/hex"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/collectd"
	"github.com/kimor79/gollectd"
)

type testServer string
type serverResponses []serverResponse
type serverResponse struct {
	database        string
	retentionPolicy string
	points          []influxdb.Point
}

var responses = make(chan *serverResponse, 1024)

func (testServer) WriteSeries(database, retentionPolicy string, points []influxdb.Point) (uint64, error) {
	responses <- &serverResponse{
		database:        database,
		retentionPolicy: retentionPolicy,
		points:          points,
	}
	return 0, nil
}

func (testServer) ResponseN(n int) ([]*serverResponse, error) {
	var a []*serverResponse
	for {
		select {
		case r := <-responses:
			a = append(a, r)
			if len(a) == n {
				return a, nil
			}
		case <-time.After(time.Second):
			return a, fmt.Errorf("unexpected response count: expected: %d, actual: %d", n, len(a))
		}
	}
}

func TestServer_ListenAndServe_ErrBindAddressRequired(t *testing.T) {
	var (
		ts testServer
		s  = collectd.NewServer(ts, "foo")
	)

	e := collectd.ListenAndServe(s, "")
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_ListenAndServe_ErrDatabaseNotSpecified(t *testing.T) {
	var (
		ts testServer
		s  = collectd.NewServer(ts, "foo")
	)

	e := collectd.ListenAndServe(s, "127.0.0.1:25826")
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_ListenAndServe_ErrCouldNotParseTypesDBFile(t *testing.T) {
	var (
		ts testServer
		s  = collectd.NewServer(ts, "foo")
	)

	s.Database = "foo"
	e := collectd.ListenAndServe(s, "127.0.0.1:25829")
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_ListenAndServe_Success(t *testing.T) {
	var (
		ts testServer
		// You can typically find this on your mac here: "/usr/local/Cellar/collectd/5.4.1/share/collectd/types.db"
		s = collectd.NewServer(ts, "./collectd_test.conf")
	)

	s.Database = "counter"
	e := collectd.ListenAndServe(s, "127.0.0.1:25830")
	defer s.Close()
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
}

func TestServer_Close_ErrServerClosed(t *testing.T) {
	var (
		ts testServer
		// You can typically find this on your mac here: "/usr/local/Cellar/collectd/5.4.1/share/collectd/types.db"
		s = collectd.NewServer(ts, "./collectd_test.conf")
	)

	s.Database = "counter"
	e := collectd.ListenAndServe(s, "127.0.0.1:25830")
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	s.Close()
	e = s.Close()
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_ListenAndServe_ErrResolveUDPAddr(t *testing.T) {
	var (
		ts testServer
		s  = collectd.NewServer(ts, "./collectd_test.conf")
	)

	s.Database = "counter"
	e := collectd.ListenAndServe(s, "foo")
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_ListenAndServe_ErrListenUDP(t *testing.T) {
	var (
		ts testServer
		s  = collectd.NewServer(ts, "./collectd_test.conf")
	)

	//Open a udp listener on the port prior to force it to err
	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:25826")
	conn, _ := net.ListenUDP("udp", addr)
	defer conn.Close()

	s.Database = "counter"
	e := collectd.ListenAndServe(s, "127.0.0.1:25826")
	if e == nil {
		t.Fatalf("expected an error, got %v", e)
	}
}

func TestServer_Serve_Success(t *testing.T) {
	// clear any previous responses
	var (
		ts testServer
		// You can typically find this on your mac here: "/usr/local/Cellar/collectd/5.4.1/share/collectd/types.db"
		s    = collectd.NewServer(ts, "./collectd_test.conf")
		addr = "127.0.0.1:25830"
	)

	s.Database = "counter"
	e := collectd.ListenAndServe(s, addr)
	defer s.Close()
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}

	conn, e := net.Dial("udp", addr)
	defer conn.Close()
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}
	buf, e := hex.DecodeString("0000000e6c6f63616c686f7374000008000c1512b2e40f5da16f0009000c00000002800000000002000e70726f636573736573000004000d70735f7374617465000005000c72756e6e696e67000006000f000101000000000000f03f0008000c1512b2e40f5db90f0005000d736c656570696e67000006000f0001010000000000c06f400008000c1512b2e40f5dc4a40005000c7a6f6d62696573000006000f00010100000000000000000008000c1512b2e40f5de10b0005000c73746f70706564000006000f00010100000000000000000008000c1512b2e40f5deac20005000b706167696e67000006000f00010100000000000000000008000c1512b2e40f5df59b0005000c626c6f636b6564000006000f00010100000000000000000008000c1512b2e40f7ee0610004000e666f726b5f726174650000050005000006000f000102000000000004572f0008000c1512b2e68e0635e6000200086370750000030006300000040008637075000005000975736572000006000f0001020000000000204f9c0008000c1512b2e68e0665d6000500096e696365000006000f000102000000000000caa30008000c1512b2e68e06789c0005000b73797374656d000006000f00010200000000000607050008000c1512b2e68e06818e0005000969646c65000006000f0001020000000003b090ae0008000c1512b2e68e068bcf0005000977616974000006000f000102000000000000f6810008000c1512b2e68e069c7d0005000e696e74657272757074000006000f000102000000000000001d0008000c1512b2e68e069fec0005000c736f6674697271000006000f0001020000000000000a2a0008000c1512b2e68e06a2b20005000a737465616c000006000f00010200000000000000000008000c1512b2e68e0708d60003000631000005000975736572000006000f00010200000000001d48c60008000c1512b2e68e070c16000500096e696365000006000f0001020000000000007fe60008000c1512b2e68e0710790005000b73797374656d000006000f00010200000000000667890008000c1512b2e68e0713bb0005000969646c65000006000f00010200000000025d0e470008000c1512b2e68e0717790005000977616974000006000f000102000000000002500e0008000c1512b2e68e071bc00005000e696e74657272757074000006000f00010200000000000000000008000c1512b2e68e071f800005000c736f6674697271000006000f00010200000000000006050008000c1512b2e68e07221e0005000a737465616c000006000f00010200000000000000000008000c1512b2e68e0726eb0003000632000005000975736572000006000f00010200000000001ff3e40008000c1512b2e68e0728cb000500096e696365000006000f000102000000000000ca210008000c1512b2e68e072ae70005000b73797374656d000006000f000102000000000006eabe0008000c1512b2e68e072f2f0005000977616974000006000f000102000000000000c1300008000c1512b2e68e072ccb0005000969646c65000006000f00010200000000025b5abb0008000c1512b2e68e07312c0005000e696e74657272757074000006000f00010200000000000000070008000c1512b2e68e0733520005000c736f6674697271000006000f00010200000000000007260008000c1512b2e68e0735b60005000a737465616c000006000f00010200000000000000000008000c1512b2e68e07828d0003000633000005000975736572000006000f000102000000000020f50a0008000c1512b2e68e0787ac000500096e696365000006000f0001020000000000008368")
	if e != nil {
		t.Fatalf("err from hex.DecodeString does not match.  expected %v, got %v", nil, e)
	}
	_, e = conn.Write(buf)
	if e != nil {
		t.Fatalf("err does not match.  expected %v, got %v", nil, e)
	}

	if _, err := ts.ResponseN(33); err != nil {
		t.Fatal(err)
	}
}

func TestUnmarshal_Points(t *testing.T) {
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
		name   string
		packet gollectd.Packet
		points []influxdb.Point
	}{
		{
			name: "single value",
			packet: gollectd.Packet{
				Plugin: "disk",
				Values: []gollectd.Value{
					{Name: "read", Value: 1},
				},
			},
			points: []influxdb.Point{
				{Name: "disk_read", Fields: map[string]interface{}{"value": float64(1)}},
			},
		},
		{
			name: "multi value",
			packet: gollectd.Packet{
				Plugin: "disk",
				Values: []gollectd.Value{
					{Name: "read", Value: 1},
					{Name: "write", Value: 5},
				},
			},
			points: []influxdb.Point{
				{Name: "disk_read", Fields: map[string]interface{}{"value": float64(1)}},
				{Name: "disk_write", Fields: map[string]interface{}{"value": float64(5)}},
			},
		},
		{
			name: "tags",
			packet: gollectd.Packet{
				Plugin:         "disk",
				Hostname:       "server01",
				PluginInstance: "sdk",
				Type:           "disk_octets",
				TypeInstance:   "single",
				Values: []gollectd.Value{
					{Name: "read", Value: 1},
				},
			},
			points: []influxdb.Point{
				{
					Name:   "disk_read",
					Tags:   map[string]string{"host": "server01", "instance": "sdk", "type": "disk_octets", "type_instance": "single"},
					Fields: map[string]interface{}{"value": float64(1)},
				},
			},
		},
	}

	for _, test := range tests {
		t.Logf("testing %q", test.name)
		points := collectd.Unmarshal(&test.packet)
		if len(points) != len(test.points) {
			t.Errorf("points len mismatch. expected %d, got %d", len(test.points), len(points))
		}
		for i, m := range test.points {
			// test name
			name := fmt.Sprintf("%s_%s", test.packet.Plugin, test.packet.Values[i].Name)
			if m.Name != name {
				t.Errorf("point name mismatch. expected %q, got %q", name, m.Name)
			}
			// test value
			mv := m.Fields["value"].(float64)
			pv := test.packet.Values[i].Value
			if mv != pv {
				t.Errorf("point value mismatch. expected %v, got %v", pv, mv)
			}
			// test tags
			if test.packet.Hostname != m.Tags["host"] {
				t.Errorf(`point tags["host"] mismatch. expected %q, got %q`, test.packet.Hostname, m.Tags["host"])
			}
			if test.packet.PluginInstance != m.Tags["instance"] {
				t.Errorf(`point tags["instance"] mismatch. expected %q, got %q`, test.packet.PluginInstance, m.Tags["instance"])
			}
			if test.packet.Type != m.Tags["type"] {
				t.Errorf(`point tags["type"] mismatch. expected %q, got %q`, test.packet.Type, m.Tags["type"])
			}
			if test.packet.TypeInstance != m.Tags["type_instance"] {
				t.Errorf(`point tags["type_instance"] mismatch. expected %q, got %q`, test.packet.TypeInstance, m.Tags["type_instance"])
			}
		}
	}
}

func TestUnmarshal_Time(t *testing.T) {
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
		name   string
		packet gollectd.Packet
		points []influxdb.Point
	}{
		{
			name: "Should parse timeHR properly",
			packet: gollectd.Packet{
				TimeHR: timeHR(testTime),
				Values: []gollectd.Value{
					{
						Value: 1,
					},
				},
			},
			points: []influxdb.Point{
				{Time: testTime},
			},
		},
		{
			name: "Should parse time properly",
			packet: gollectd.Packet{
				Time: uint64(testTime.Round(time.Second).Unix()),
				Values: []gollectd.Value{
					{
						Value: 1,
					},
				},
			},
			points: []influxdb.Point{
				{Time: testTime.Round(time.Second)},
			},
		},
	}

	for _, test := range tests {
		t.Logf("testing %q", test.name)
		points := collectd.Unmarshal(&test.packet)
		if len(points) != len(test.points) {
			t.Errorf("point len mismatch. expected %d, got %d", len(test.points), len(points))
		}
		for _, p := range points {
			if test.packet.TimeHR > 0 {
				if p.Time.Format(time.RFC3339Nano) != testTime.Format(time.RFC3339Nano) {
					t.Errorf("time mis-match, got %v, expected %v", p.Time.Format(time.RFC3339Nano), testTime.Format(time.RFC3339Nano))
				} else if p.Time.Format(time.RFC3339) != testTime.Format(time.RFC3339) {
					t.Errorf("time mis-match, got %v, expected %v", p.Time.Format(time.RFC3339), testTime.Format(time.RFC3339))
				}
			}
		}
	}
}
