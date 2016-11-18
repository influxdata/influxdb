package internal_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/bolt/internal"
)

// Ensure an exploration can be marshaled and unmarshaled.
func TestMarshalExploration(t *testing.T) {
	v := chronograf.Exploration{
		ID:        12,
		Name:      "Some Exploration",
		UserID:    34,
		Data:      "{\"data\":\"something\"}",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	var vv chronograf.Exploration
	if buf, err := internal.MarshalExploration(&v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalExploration(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("exploration protobuf copy error: got %#v, expected %#v", vv, v)
	}
}

func TestMarshalSource(t *testing.T) {
	v := chronograf.Source{
		ID:       12,
		Name:     "Fountain of Truth",
		Type:     "influx",
		Username: "docbrown",
		Password: "1 point twenty-one g1g@w@tts",
		URL:      "http://twin-pines.mall.io:8086",
		Default:  true,
		Telegraf: "telegraf",
	}

	var vv chronograf.Source
	if buf, err := internal.MarshalSource(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalSource(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}
}

func TestMarshalServer(t *testing.T) {
	v := chronograf.Server{
		ID:       12,
		SrcID:    2,
		Name:     "Fountain of Truth",
		Username: "docbrown",
		Password: "1 point twenty-one g1g@w@tts",
		URL:      "http://oldmanpeabody.mall.io:9092",
	}

	var vv chronograf.Server
	if buf, err := internal.MarshalServer(v); err != nil {
		t.Fatal(err)
	} else if err := internal.UnmarshalServer(buf, &vv); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(v, vv) {
		t.Fatalf("source protobuf copy error: got %#v, expected %#v", vv, v)
	}
}
