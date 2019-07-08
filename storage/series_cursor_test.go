package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
)

func Test_NewSeriesCursor_UnexpectedOrg(t *testing.T) {
	makeKey := func(org, bucket uint64) []byte {
		name := tsdb.EncodeName(influxdb.ID(org), influxdb.ID(bucket))
		return tsdb.AppendSeriesKey(nil, name[:], nil)
	}

	nm := tsdb.EncodeName(0x0f0f, 0xb0b0)
	cur := &seriesCursor{
		keys: [][]byte{
			makeKey(0x0f0f, 0xb0b0),
			makeKey(0xffff, 0xb0b0),
		},
		name: nm,
		init: true,
	}
	_, err := cur.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = cur.Next()
	if err == nil {
		t.Fatal("expected error")
	}

	if !cmp.Equal(err.Error(), errUnexpectedOrg.Error()) {
		t.Errorf("unexpected error -got/+exp\n%s", cmp.Diff(err.Error(), errUnexpectedOrg.Error()))
	}
}
