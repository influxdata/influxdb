package internal_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/store/bolt/internal"
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

	var other mrfusion.Exploration
	if buf, err := internal.MarshalExploration(&v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalExploration(buf, &other); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, other) {
		t.Fatalf("unexpected copy: %#v", other)
	}
}
