package tsdb_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/tsdb"
)

func TestNames(t *testing.T) {
	goodExamples := []struct {
		Org    uint64
		Bucket uint64
		Name   [16]byte
	}{
		{Org: 12345678, Bucket: 87654321, Name: [16]byte{0, 0, 0, 0, 0, 188, 97, 78, 0, 0, 0, 0, 5, 57, 127, 177}},
		{Org: 1234567891011, Bucket: 87654321, Name: [16]byte{0, 0, 1, 31, 113, 251, 8, 67, 0, 0, 0, 0, 5, 57, 127, 177}},
		{Org: 12345678, Bucket: 8765432100000, Name: [16]byte{0, 0, 0, 0, 0, 188, 97, 78, 0, 0, 7, 248, 220, 119, 116, 160}},
		{Org: 123456789929, Bucket: 8765432100000, Name: [16]byte{0, 0, 0, 28, 190, 153, 29, 169, 0, 0, 7, 248, 220, 119, 116, 160}},
	}

	for _, example := range goodExamples {
		t.Run(fmt.Sprintf("%d%d", example.Org, example.Bucket), func(t *testing.T) {

			name := tsdb.EncodeName(platform.ID(example.Org), platform.ID(example.Bucket))

			if got, exp := name, example.Name; got != exp {
				t.Errorf("got name %q, expected %q", got, exp)
			}

			org, bucket := tsdb.DecodeName(name)

			if gotOrg, expOrg := org, example.Org; gotOrg != platform.ID(expOrg) {
				t.Errorf("got organization ID %q, expected %q", gotOrg, expOrg)
			}
			if gotBucket, expBucket := bucket, example.Bucket; gotBucket != platform.ID(expBucket) {
				t.Errorf("got organization ID %q, expected %q", gotBucket, expBucket)
			}
		})
	}
}
