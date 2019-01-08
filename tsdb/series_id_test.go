package tsdb

import (
	"math/rand"
	"testing"

	"github.com/influxdata/influxdb/models"
)

func TestSeriesID(t *testing.T) {
	types := []models.FieldType{
		models.Integer,
		models.Float,
		models.Boolean,
		models.String,
		models.Unsigned,
	}

	for i := 0; i < 1000000; i++ {
		id := NewSeriesID(uint64(rand.Int31()))
		for _, typ := range types {
			typed := id.WithType(typ)
			if got := typed.Type(); got != typ {
				t.Fatalf("wanted: %v got: %v", typ, got)
			}
			if got := typed.SeriesID(); id != got {
				t.Fatalf("wanted: %016x got: %016x", id, got)
			}
		}
	}
}
