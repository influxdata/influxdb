package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/seriesfile"
)

func Test_NewSeriesCursor_UnexpectedOrg(t *testing.T) {
	makeKey := func(orgID, bucketID influxdb.ID) []byte {
		name := tsdb.EncodeName(orgID, bucketID)
		return seriesfile.AppendSeriesKey(nil, name[:], nil)
	}

	orgID := influxdb.ID(0x0f0f)
	encodedOrgID := tsdb.EncodeOrgName(orgID)
	bucketID := influxdb.ID(0xb0b0)
	cur := &seriesCursor{
		keys: [][]byte{
			makeKey(orgID, bucketID),
			makeKey(influxdb.ID(0xffff), bucketID),
		},
		orgID:        orgID,
		encodedOrgID: encodedOrgID[:],
		init:         true,
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
