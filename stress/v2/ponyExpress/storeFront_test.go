package ponyExpress

import (
	"fmt"
	"testing"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

func NewBlankTestPoint() *influx.Point {
	meas := "measurement"
	tags := map[string]string{"fooTag": "fooTagValue"}
	fields := map[string]interface{}{"value": 5920}
	utc, _ := time.LoadLocation("UTC")
	timestamp := time.Date(2016, time.Month(4), 20, 0, 0, 0, 0, utc)
	pt, _ := influx.NewPoint(meas, tags, fields, timestamp)
	return pt
}

func TestStoreFrontBatcher(t *testing.T) {
	sf, _, _ := NewTestStoreFront()
	bpconf := influx.BatchPointsConfig{
		Database:  fmt.Sprintf("_%v", sf.TestName),
		Precision: "ns",
	}
	bp, _ := influx.NewBatchPoints(bpconf)
	pt := NewBlankTestPoint()
	bp = sf.batcher(pt, bp, bpconf)
	if len(bp.Points()) != 1 {
		t.Fail()
	}
}
