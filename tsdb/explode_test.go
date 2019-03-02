package tsdb_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
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

func TestExplodePoints(t *testing.T) {
	points, err := models.ParsePointsString(`
		cpu,t1=a,t2=q f1=5,f2="f" 9
		mem,t1=b,t2=w f1=6,f2="g",f3=true 8
		cpu,t3=e,t1=c f3=7,f4="h" 7
		mem,t1=d,t2=r,t4=g f1=8,f2="i" 6
	`)
	if err != nil {
		t.Fatal(err)
	}

	org := platform.ID(0x4F4F4F4F4F4F4F4F)
	bucket := platform.ID(0x4242424242424242)
	points, err = tsdb.ExplodePoints(org, bucket, points)
	if err != nil {
		t.Fatal(err)
	}

	var lines []string
	for _, point := range points {
		lines = append(lines, point.String())
	}
	sort.Strings(lines)

	expected := []string{
		`OOOOOOOOBBBBBBBB,_f=f1,_m=cpu,t1=a,t2=q f1=5 9`,
		`OOOOOOOOBBBBBBBB,_f=f1,_m=mem,t1=b,t2=w f1=6 8`,
		`OOOOOOOOBBBBBBBB,_f=f1,_m=mem,t1=d,t2=r,t4=g f1=8 6`,
		`OOOOOOOOBBBBBBBB,_f=f2,_m=cpu,t1=a,t2=q f2="f" 9`,
		`OOOOOOOOBBBBBBBB,_f=f2,_m=mem,t1=b,t2=w f2="g" 8`,
		`OOOOOOOOBBBBBBBB,_f=f2,_m=mem,t1=d,t2=r,t4=g f2="i" 6`,
		`OOOOOOOOBBBBBBBB,_f=f3,_m=cpu,t1=c,t3=e f3=7 7`,
		`OOOOOOOOBBBBBBBB,_f=f3,_m=mem,t1=b,t2=w f3=true 8`,
		`OOOOOOOOBBBBBBBB,_f=f4,_m=cpu,t1=c,t3=e f4="h" 7`,
	}

	if !reflect.DeepEqual(lines, expected) {
		t.Fatal("bad output:\n", cmp.Diff(lines, expected))
	}
}
