package lineprotocol_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/stress/lineprotocol"
)

//func TestInt_WriteTo(t *testing.T) {
//	i := &lineprotocol.Int{
//		Key:   []byte("a"),
//		Value: int64(100),
//	}
//
//	buf := bytes.NewBuffer(nil)
//
//	if _, err := i.WriteTo(buf); err != nil {
//		t.Error(err)
//		return
//	}
//
//	exp := "a=100i"
//	got := string(buf.Bytes())
//
//	if got != exp {
//		t.Errorf("Wrong field data written. got %v, exp %v", got, exp)
//		return
//	}
//}

func TestFloat_WriteTo(t *testing.T) {
	i := &lineprotocol.Float{
		Key:   []byte("a"),
		Value: float64(100),
	}

	buf := bytes.NewBuffer(nil)

	if _, err := i.WriteTo(buf); err != nil {
		t.Error(err)
		return
	}

	exp := "a=100"
	got := string(buf.Bytes())

	if got != exp {
		t.Errorf("Wrong field data written. got %v, exp %v", got, exp)
		return
	}
}
