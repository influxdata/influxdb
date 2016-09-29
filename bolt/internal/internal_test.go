package internal_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/bolt/internal"
)

// Ensure an exploration can be marshaled and unmarshaled.
func TestMarshalExploration(t *testing.T) {
	v := mrfusion.Exploration{
		ID:        12,
		Name:      "Some Exploration",
		UserID:    34,
		Data:      "{\"data\":\"something\"}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	var vv mrfusion.Exploration
	if buf, err := internal.MarshalExploration(&v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalExploration(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("exploration protobuf copy error: got %#v, expected %#v", vv, v)
	}
}
