package lineprotocol_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/stress/lineprotocol"
)

var (
	testTime time.Time = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
)

type mockPoint struct{}

func NewMockPoint() lineprotocol.Point {
	return &mockPoint{}
}

func (p *mockPoint) Series() []byte {
	return []byte("cpu,host=server")
}

func (p *mockPoint) Fields() []lineprotocol.Field {
	i := &lineprotocol.Int{
		Key:   []byte("a"),
		Value: int64(100),
	}
	j := &lineprotocol.Float{
		Key:   []byte("b"),
		Value: float64(10),
	}
	return []lineprotocol.Field{i, j}
}

func (p *mockPoint) Time() *lineprotocol.Timestamp {
	ts := lineprotocol.NewTimestamp(lineprotocol.Nanosecond)
	ts.SetTime(&testTime)
	return ts
}

func (p *mockPoint) SetTime(time.Time) {
	return
}

func (p *mockPoint) Update() {
	return
}

func TestWritePoint_MockPoint(t *testing.T) {
	p := NewMockPoint()

	buf := bytes.NewBuffer(nil)
	if err := lineprotocol.WritePoint(buf, p); err != nil {
		t.Error(err)
		return
	}

	exp := fmt.Sprintf("cpu,host=server a=100i,b=10 %v\n", testTime.UnixNano())
	got := string(buf.Bytes())

	if got != exp {
		t.Errorf("Wrong data was written. got %v, exp %v", got, exp)
		return
	}

}
