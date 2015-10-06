package runner

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdb/influxdb/client"
)

func TestTimer_StartTimer(t *testing.T) {
	var epoch time.Time

	tmr := &Timer{}

	tmr.StartTimer()

	s := tmr.Start()

	if s == epoch {
		t.Errorf("expected tmr.start to not be %v", s)
	}
}

func TestNewTimer(t *testing.T) {
	var epoch time.Time

	tmr := NewTimer()

	s := tmr.Start()

	if s == epoch {
		t.Errorf("expected tmr.start to not be %v", s)
	}

	e := tmr.End()

	if e != epoch {
		t.Errorf("expected tmr.stop to be %v, got %v", epoch, e)
	}
}

func TestTimer_StopTimer(t *testing.T) {
	var epoch time.Time

	tmr := NewTimer()

	tmr.StopTimer()

	e := tmr.End()

	if e == epoch {
		t.Errorf("expected tmr.stop to not be %v", e)
	}
}

func TestTimer_Elapsed(t *testing.T) {

	tmr := NewTimer()
	time.Sleep(2 * time.Second)
	tmr.StopTimer()

	e := tmr.Elapsed()

	if time.Duration(2*time.Second) > e || e > time.Duration(3*time.Second) {
		t.Errorf("expected around %s got %s", time.Duration(2*time.Second), e)
	}

}

func TestNewResponseTime(t *testing.T) {
	r := NewResponseTime(100)

	if r.Value != 100 {
		t.Errorf("expected Value to be %v, got %v", 100, r.Value)
	}

	var epoch time.Time

	if r.Time == epoch {
		t.Errorf("expected r.Time not to be %v", epoch)
	}

}

func TestMeasurments_Set(t *testing.T) {
	ms := make(Measurements, 0)

	ms.Set("this,is,a,test")

	if ms[0] != "this" {
		t.Errorf("expected value to be %v, got %v", "this", ms[0])
	}

	if ms[1] != "is" {
		t.Errorf("expected value to be %v, got %v", "is", ms[1])
	}

	ms.Set("more,here")

	if ms[4] != "more" {
		t.Errorf("expected value to be %v, got %v", "more", ms[4])
	}

	if len(ms) != 6 {
		t.Errorf("expected the length of ms to be %v, got %v", 6, len(ms))
	}

}

func TestNewSeries(t *testing.T) {
	s := NewSeries("cpu", 1000, 10000)

	if s.PointCount != 1000 {
		t.Errorf("expected value to be %v, got %v", 1000, s.PointCount)
	}

	if s.SeriesCount != 10000 {
		t.Errorf("expected value to be %v, got %v", 10000, s.SeriesCount)
	}

	if s.Measurement != "cpu" {
		t.Errorf("expected value to be %v, got %v", "cpu", s.Measurement)
	}
}

func TestNewConfig(t *testing.T) {
	c := NewConfig()

	if c.Write.BatchSize != 5000 {
		t.Errorf("expected value to be %v, got %v", 5000, c.Write.BatchSize)
	}

}

func TestSeriesIter_Next(t *testing.T) {
	s := NewSeries("cpu", 1000, 10000)
	const shortForm = "2006-Jan-02"
	tm, _ := time.Parse(shortForm, "2013-Feb-03")
	i := s.Iter(10, tm, "n")
	if i.count != -1 {
		t.Errorf("expected value to be %v, go %v", -1, i.count)
	}

	_, ok := i.Next()

	for ok {
		_, ok = i.Next()
	}

	if i.count != s.SeriesCount {
		t.Errorf("expected value to be %v, go %v", s.SeriesCount, i.count)
	}

}

func TestConfig_newClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Influxdb-Version", "x.x")
		return
	}))
	defer ts.Close()

	ms := make(Measurements, 0)
	ms.Set("this,is,a,test")

	url := ts.URL[7:]

	cfg, err := DecodeFile("test.toml")

	if err != nil {
		panic(err)
	}

	cfg.Write.Address = url

	// the client.NewClient method in the influxdb go
	// client never returns an error that is not nil.
	// To test that the client actually gets created without
	// any error, I run the Ping() method to verify that everything
	// is okay.
	c, err := cfg.NewClient()

	// This err will never be nil. See the following URL:
	// https://github.com/influxdb/influxdb/blob/master/client/influxdb.go#L119
	if err != nil {
		t.Error(err)
	}

	// Verify that the client actually gets created correctly
	d, version, err := c.Ping()
	if err != nil {
		t.Fatalf("unexpected error.  expected %v, actual %v", nil, err)
	}
	if d == 0 {
		t.Fatalf("expected a duration greater than zero.  actual %v", d)
	}
	if version != "x.x" {
		t.Fatalf("unexpected version.  expected %s,  actual %v", "x.x", version)
	}
}

func TestRun(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data client.Response
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	ms := make(Measurements, 0)
	ms.Set("this,is,a,test")

	url := ts.URL[7:]

	cfg, _ := DecodeFile("test.toml")

	cfg.Write.Address = url

	d := make(chan struct{})
	timestamp := make(chan time.Time)

	tp, _, rts, tmr := Run(cfg, d, timestamp)

	ps := cfg.Series[0].SeriesCount * cfg.Series[0].PointCount

	if tp != ps {
		t.Fatalf("unexpected error. expected %v, actual %v", ps, tp)
	}

	if len(rts) != ps/cfg.Write.BatchSize {
		t.Fatalf("unexpected error. expected %v, actual %v", ps/cfg.Write.BatchSize, len(rts))
	}

	var epoch time.Time

	if tmr.Start() == epoch {
		t.Errorf("expected trm.start not to be %s", epoch)
	}

	if tmr.End() == epoch {
		t.Errorf("expected trm.end not to be %s", epoch)
	}
}
