package data_test

import (
	"testing"

	"github.com/influxdb/influxdb/data"
)

var tags = data.Tags{"foo": "bar", "apple": "orange", "host": "serverA", "region": "uswest"}

func TestMarshal(t *testing.T) {
	got := tags.Marshal()
	if exp := "apple|foo|host|region|orange|bar|serverA|uswest"; string(got) != exp {
		t.Log("got: ", string(got))
		t.Log("exp: ", exp)
		t.Error("invalid match")
	}
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tags.Marshal()
	}
}

func TestHash(t *testing.T) {
	got := tags.Hash()
	if exp := uint64(16026309424388707645); exp != got {
		t.Errorf("got: %d, exp: %d", got, exp)
	}
}

func TestHash_EmptyTags(t *testing.T) {
	tags := data.Tags{}
	got := tags.Hash()
	if exp := uint64(0); exp != got {
		t.Errorf("got: %d, exp: %d", got, exp)
	}
}

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tags.Hash()
	}
}
