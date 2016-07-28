package client

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
)

var (
	mockMetrics = `aerospike_namespace,aerospike_host=localhost:3000,host=tyrion,namespace=test,node_name=BB9020011AC4202 available_bin_names=32768i,batch_sub_proxy_complete=0i,batch_sub_proxy_error=0i,batch_sub_proxy_timeout=0i,batch_sub_read_error=0i,batch_sub_read_not_found=0i,batch_sub_read_success=0i,batch_sub_read_timeout=0i,batch_sub_tsvc_error=0i,batch_sub_tsvc_timeout=0i,client_delete_error=0i,client_delete_not_found=0i,client_delete_success=0i,client_delete_timeout=0i,client_lang_delete_success=0i,client_lang_error=0i,client_lang_read_success=0i,client_lang_write_success=0i,client_proxy_complete=0i,client_proxy_error=0i,client_proxy_timeout=0i,client_read_error=0i,client_read_not_found=0i,client_read_success=0i,client_read_timeout=0i,client_tsvc_error=0i,client_tsvc_timeout=0i,client_udf_complete=0i,client_udf_error=0i,client_udf_timeout=0i,client_write_error=0i,client_write_success=0i,client_write_timeout=0i,cold_start_evict_ttl=4294967295i,current_time=206400387i,default_ttl=432000i,device_available_pct=99i,device_free_pct=100i,device_total_bytes=4294967296i,device_used_bytes=0i,evict_hist_buckets=10000i,evict_tenths_pct=5i,evict_ttl=0i,evicted_objects=0i,expired_objects=0i,fail_generation=0i,fail_key_busy=0i,fail_record_too_big=0i,fail_xdr_forbidden=0i,geo2dsphere_within.earth_radius_meters=6371000i,geo2dsphere_within.level_mod=1i,geo2dsphere_within.max_cells=12i,geo2dsphere_within.max_level=30i,geo2dsphere_within.min_level=1i,geo_region_query_cells=0i,geo_region_query_falsepos=0i,geo_region_query_points=0i,geo_region_query_reqs=0i,high_water_disk_pct=50i,high_water_memory_pct=60i,ldt_gc_rate=0i,ldt_page_size=8192i,master_objects=0i,master_sub_objects=0i,max_ttl=315360000i,max_void_time=0i,memory_free_pct=100i,memory_size=1073741824i,memory_used_bytes=0i,memory_used_data_bytes=0i,memory_used_index_bytes=0i,memory_used_sindex_bytes=0i,migrate_order=5i,migrate_record_receives=0i,migrate_record_retransmits=0i,migrate_records_skipped=0i,migrate_records_transmitted=0i,migrate_rx_instances=0i,migrate_rx_partitions_active=0i,migrate_rx_partitions_initial=0i,migrate_rx_partitions_remaining=0i,migrate_sleep=1i,migrate_tx_instances=0i,migrate_tx_partitions_active=0i,migrate_tx_partitions_imbalance=0i,migrate_tx_partitions_initial=0i,migrate_tx_partitions_remaining=0i,non_expirable_objects=0i,nsup_cycle_duration=0i,nsup_cycle_sleep_pct=0i,objects=0i,prole_objects=0i,prole_sub_objects=0i,query_agg=0i,query_agg_abort=0i,query_agg_avg_rec_count=0i,query_agg_error=0i,query_agg_success=0i,query_fail=0i,query_long_queue_full=0i 1468704387000000000`
)

func TestUDPClient_Query(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()
	query := Query{}
	_, err = c.Query(query)
	if err == nil {
		t.Error("Querying UDP client should fail")
	}
}

func TestUDPClient_Ping(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	rtt, version, err := c.Ping(0)
	if rtt != 0 || version != "" || err != nil {
		t.Errorf("unexpected error.  expected (%v, '%v', %v), actual (%v, '%v', %v)", 0, "", nil, rtt, version, err)
	}
}

func TestUDPClient_Write(t *testing.T) {
	config := UDPConfig{Addr: "localhost:8089"}
	c, err := NewUDPClient(config)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	fields := make(map[string]interface{})
	fields["value"] = 1.0
	pt, _ := NewPoint("cpu", make(map[string]string), fields)
	bp.AddPoint(pt)

	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestUDPClient_BadAddr(t *testing.T) {
	config := UDPConfig{Addr: "foobar@wahoo"}
	c, err := NewUDPClient(config)
	if err == nil {
		defer c.Close()
		t.Error("Expected resolve error")
	}
}

func TestClient_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_BasicAuth(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()

		if !ok {
			t.Errorf("basic auth error")
		}
		if u != "username" {
			t.Errorf("unexpected username, expected %q, actual %q", "username", u)
		}
		if p != "password" {
			t.Errorf("unexpected password, expected %q, actual %q", "password", p)
		}
		var data Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL, Username: "username", Password: "password"}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	query := Query{}
	_, err := c.Query(query)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Ping(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	_, _, err := c.Ping(0)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_Concurrent_Use(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	n := 1000

	go func() {
		defer wg.Done()
		bp, err := NewBatchPoints(BatchPointsConfig{})
		if err != nil {
			t.Errorf("got error %v", err)
		}

		for i := 0; i < n; i++ {
			if err = c.Write(bp); err != nil {
				t.Fatalf("got error %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		var q Query
		for i := 0; i < n; i++ {
			if _, err := c.Query(q); err != nil {
				t.Fatalf("got error %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			c.Ping(time.Second)
		}
	}()
	wg.Wait()
}

func TestClient_Write(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data Response
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	config := HTTPConfig{Addr: ts.URL}
	c, _ := NewHTTPClient(config)
	defer c.Close()

	bp, err := NewBatchPoints(BatchPointsConfig{})
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
	err = c.Write(bp)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}
}

func TestClient_UserAgent(t *testing.T) {
	receivedUserAgent := ""
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedUserAgent = r.UserAgent()

		var data Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	_, err := http.Get(ts.URL)
	if err != nil {
		t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
	}

	tests := []struct {
		name      string
		userAgent string
		expected  string
	}{
		{
			name:      "Empty user agent",
			userAgent: "",
			expected:  "InfluxDBClient",
		},
		{
			name:      "Custom user agent",
			userAgent: "Test Influx Client",
			expected:  "Test Influx Client",
		},
	}

	for _, test := range tests {

		config := HTTPConfig{Addr: ts.URL, UserAgent: test.userAgent}
		c, _ := NewHTTPClient(config)
		defer c.Close()

		receivedUserAgent = ""
		query := Query{}
		_, err = c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		bp, _ := NewBatchPoints(BatchPointsConfig{})
		err = c.Write(bp)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if !strings.HasPrefix(receivedUserAgent, test.expected) {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}

		receivedUserAgent = ""
		_, err := c.Query(query)
		if err != nil {
			t.Errorf("unexpected error.  expected %v, actual %v", nil, err)
		}
		if receivedUserAgent != test.expected {
			t.Errorf("Unexpected user agent. expected %v, actual %v", test.expected, receivedUserAgent)
		}
	}
}

func TestClient_PointString(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000000000"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	s = "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39 1359849600000"
	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointWithoutTimeString(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	s := "cpu_usage,cpu=cpu-total idle=10.1,system=50.9,user=39"
	if p.String() != s {
		t.Errorf("Point String Error, got %s, expected %s", p.String(), s)
	}

	if p.PrecisionString("ms") != s {
		t.Errorf("Point String Error, got %s, expected %s",
			p.PrecisionString("ms"), s)
	}
}

func TestClient_PointName(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	exp := "cpu_usage"
	if p.Name() != exp {
		t.Errorf("Error, got %s, expected %s",
			p.Name(), exp)
	}
}

func TestClient_PointTags(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	if !reflect.DeepEqual(tags, p.Tags()) {
		t.Errorf("Error, got %v, expected %v",
			p.Tags(), tags)
	}
}

func TestClient_PointUnixNano(t *testing.T) {
	const shortForm = "2006-Jan-02"
	time1, _ := time.Parse(shortForm, "2013-Feb-03")
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields, time1)

	exp := int64(1359849600000000000)
	if p.UnixNano() != exp {
		t.Errorf("Error, got %d, expected %d",
			p.UnixNano(), exp)
	}
}

func TestClient_PointFields(t *testing.T) {
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{"idle": 10.1, "system": 50.9, "user": 39.0}
	p, _ := NewPoint("cpu_usage", tags, fields)

	if !reflect.DeepEqual(fields, p.Fields()) {
		t.Errorf("Error, got %v, expected %v",
			p.Fields(), fields)
	}
}

func TestBatchPoints_PrecisionError(t *testing.T) {
	_, err := NewBatchPoints(BatchPointsConfig{Precision: "foobar"})
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}

	bp, _ := NewBatchPoints(BatchPointsConfig{Precision: "ns"})
	err = bp.SetPrecision("foobar")
	if err == nil {
		t.Errorf("Precision: foobar should have errored")
	}
}

func TestBatchPoints_SettersGetters(t *testing.T) {
	bp, _ := NewBatchPoints(BatchPointsConfig{
		Precision:        "ns",
		Database:         "db",
		RetentionPolicy:  "rp",
		WriteConsistency: "wc",
	})
	if bp.Precision() != "ns" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "ns")
	}
	if bp.Database() != "db" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db")
	}
	if bp.RetentionPolicy() != "rp" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp")
	}
	if bp.WriteConsistency() != "wc" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc")
	}

	bp.SetDatabase("db2")
	bp.SetRetentionPolicy("rp2")
	bp.SetWriteConsistency("wc2")
	err := bp.SetPrecision("s")
	if err != nil {
		t.Errorf("Did not expect error: %s", err.Error())
	}

	if bp.Precision() != "s" {
		t.Errorf("Expected: %s, got %s", bp.Precision(), "s")
	}
	if bp.Database() != "db2" {
		t.Errorf("Expected: %s, got %s", bp.Database(), "db2")
	}
	if bp.RetentionPolicy() != "rp2" {
		t.Errorf("Expected: %s, got %s", bp.RetentionPolicy(), "rp2")
	}
	if bp.WriteConsistency() != "wc2" {
		t.Errorf("Expected: %s, got %s", bp.WriteConsistency(), "wc2")
	}
}

func TestWriteChunks(t *testing.T) {
	var (
		payloadSize = 1500
		UDPAddr     = "localhost:8888"
		metricSize  = int32(len(mockMetrics))
		done        = make(chan struct{})
	)
	addr, err := net.ResolveUDPAddr("udp", UDPAddr)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			buf := make([]byte, 1024*4)
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				t.Fatal(err)
			}
			if n > payloadSize {
				t.Fatalf("payload size is greater than expected: want < %d, got %d", payloadSize, n)
			}
			// run until all metrics chunks are not received
			if atomic.AddInt32(&metricSize, int32(-n)); atomic.LoadInt32(&metricSize) < 0 {
				done <- struct{}{}
			}
		}
	}()

	client, err := NewUDPClient(UDPConfig{
		Addr:        UDPAddr,
		PayloadSize: payloadSize,
	})

	bp, _ := NewBatchPoints(BatchPointsConfig{
		Precision:        "ns",
		Database:         "db",
		RetentionPolicy:  "rp",
		WriteConsistency: "wc",
	})

	points, err := models.ParsePoints([]byte(mockMetrics))
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range points {
		bp.AddPoint(&Point{
			pt: p,
		})
	}

	err = client.Write(bp)
	if err != nil {
		t.Fatal(err)
	}
	<-done
}
