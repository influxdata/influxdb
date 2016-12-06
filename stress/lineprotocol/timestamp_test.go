package lineprotocol_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/stress/lineprotocol"
)

func TestTimestamp_SetTime(t *testing.T) {
	ts := lineprotocol.NewTimestamp(lineprotocol.Nanosecond)

	now := time.Now()

	ts.SetTime(&now)

	tsPtr := ts.TimePtr()
	tsTime := *(*time.Time)(*tsPtr)

	if got, exp := tsTime.UnixNano(), now.UnixNano(); got != exp {
		t.Errorf("Wrong time was set. got %v, exp %v", got, exp)
		return
	}
}

func TestTimestamp_WriteTo_Second(t *testing.T) {
	ts := lineprotocol.NewTimestamp(lineprotocol.Second)
	ts.SetTime(&testTime)

	buf := bytes.NewBuffer(nil)
	if _, err := ts.WriteTo(buf); err != nil {
		t.Error(err)
		return
	}

	exp := fmt.Sprintf("%v", testTime.Unix())
	got := string(buf.Bytes())

	if got != exp {
		t.Errorf("Wrong timestamp written. got %v, exp %v", got, exp)
		return
	}
}

func TestTimestamp_WriteTo_Nanosecond(t *testing.T) {
	ts := lineprotocol.NewTimestamp(lineprotocol.Second)
	ts.SetTime(&testTime)

	buf := bytes.NewBuffer(nil)
	if _, err := ts.WriteTo(buf); err != nil {
		t.Error(err)
		return
	}

	exp := fmt.Sprintf("%v", testTime.Unix())
	got := string(buf.Bytes())

	if got != exp {
		t.Errorf("Wrong timestamp written. got %v, exp %v", got, exp)
		return
	}
}
