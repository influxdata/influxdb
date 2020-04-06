package migrate

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/pkg/slices"
)

func Test_sortShardDirs(t *testing.T) {
	input := []shardMapping{
		{path: "/influxdb/data/db0/autogen/0"},
		{path: "/influxdb/data/db0/rp0/10"},
		{path: "/influxdb/data/db0/autogen/10"},
		{path: "/influxdb/data/db0/autogen/2"},
		{path: "/influxdb/data/db0/autogen/43"},
		{path: "/influxdb/data/apple/rp1/99"},
		{path: "/influxdb/data/apple/rp2/0"},
		{path: "/influxdb/data/db0/autogen/33"},
	}

	expected := []shardMapping{
		{path: "/influxdb/data/apple/rp1/99"},
		{path: "/influxdb/data/apple/rp2/0"},
		{path: "/influxdb/data/db0/autogen/0"},
		{path: "/influxdb/data/db0/autogen/2"},
		{path: "/influxdb/data/db0/autogen/10"},
		{path: "/influxdb/data/db0/autogen/33"},
		{path: "/influxdb/data/db0/autogen/43"},
		{path: "/influxdb/data/db0/rp0/10"},
	}

	if err := sortShardDirs(input); err != nil {
		t.Fatal(err)
	}

	if got, exp := input, expected; !reflect.DeepEqual(got, exp) {
		t.Fatalf("got %v, expected %v", got, expected)
	}

	input = append(input, shardMapping{path: "/influxdb/data/db0/rp0/badformat"})
	if err := sortShardDirs(input); err == nil {
		t.Fatal("expected error, got <nil>")
	}
}

var sep = tsmKeyFieldSeparator1x

func Test_sort1xTSMKeys(t *testing.T) {
	cases := []struct {
		input    [][]byte
		expected [][]byte
	}{
		{
			input: slices.StringsToBytes(
				"cpu"+sep+"a",
				"cpu"+sep+"b",
				"cpu"+sep+"c",
				"disk"+sep+"a",
			),
			expected: slices.StringsToBytes(
				"cpu"+sep+"a",
				"cpu"+sep+"b",
				"cpu"+sep+"c",
				"disk"+sep+"a",
			),
		},
		{
			input: slices.StringsToBytes(
				"cpu"+sep+"c",
				"cpu,region=east"+sep+"b",
				"cpu,region=east,server=a"+sep+"a",
			),
			expected: slices.StringsToBytes(
				"cpu,region=east,server=a"+sep+"a",
				"cpu,region=east"+sep+"b",
				"cpu"+sep+"c",
			),
		},
		{
			input: slices.StringsToBytes(
				"cpu"+sep+"c",
				"cpu,region=east"+sep+"b",
				"cpu,region=east,server=a"+sep+"a",
			),
			expected: slices.StringsToBytes(
				"cpu,region=east,server=a"+sep+"a",
				"cpu,region=east"+sep+"b",
				"cpu"+sep+"c",
			),
		},
		{
			input: slices.StringsToBytes(
				"\xc1\xbd\xd5)x!\a#H\xd4\xf3ç\xde\v\x14,\x00=m0,tag0=value1#!~#v0",
				"\xc1\xbd\xd5)x!\a#H\xd4\xf3ç\xde\v\x14,\x00=m0,tag0=value19,tag1=value999,tag2=value9,tag3=value0#!~#v0",
			),
			expected: slices.StringsToBytes(
				"\xc1\xbd\xd5)x!\a#H\xd4\xf3ç\xde\v\x14,\x00=m0,tag0=value1"+sep+"v0",
				"\xc1\xbd\xd5)x!\a#H\xd4\xf3ç\xde\v\x14,\x00=m0,tag0=value19,tag1=value999,tag2=value9,tag3=value0"+sep+"v0",
			),
		},
	}

	for _, tc := range cases {
		sort1xTSMKeys(tc.input)
		if got, exp := tc.input, tc.expected; !reflect.DeepEqual(got, exp) {
			t.Errorf("got %s, expected %s", got, exp)
		}
	}
}
