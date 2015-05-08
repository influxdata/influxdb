package influxdb_test

import (
	"testing"

	"github.com/influxdb/influxdb"
)

var tags = influxdb.Tags{"foo": "bar", "apple": "orange", "host": "serverA", "region": "uswest"}

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
