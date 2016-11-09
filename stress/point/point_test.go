package point_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/stress/lineprotocol"
	"github.com/influxdata/influxdb/stress/point"
)

var (
	testTime time.Time = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
)

func TestPoint(t *testing.T) {
	sk := []byte("cpu,host=server")
	ints := []string{"user", "system"}
	floats := []string{"busy", "wait"}

	p := point.New(sk, ints, floats, lineprotocol.Nanosecond)
	p.SetTime(testTime)

	buf := bytes.NewBuffer(nil)

	for i := 0; i < 10; i++ {
		if err := lineprotocol.WritePoint(buf, p); err != nil {
			t.Error(err)
			return
		}

		exp := fmt.Sprintf("cpu,host=server user=%vi,system=%vi,busy=%v,wait=%v %v\n", i, i, i, i, testTime.UnixNano())
		got := string(buf.Bytes())

		if got != exp {
			t.Errorf("Wrong data was written. got %v, exp %v", got, exp)
			return
		}

		p.Update()
		buf.Reset()
	}
}
