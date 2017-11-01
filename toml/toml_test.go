package toml_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	itoml "github.com/influxdata/influxdb/toml"
)

func TestSize_UnmarshalText(t *testing.T) {
	var s itoml.Size
	for _, test := range []struct {
		str  string
		want uint64
	}{
		{"1", 1},
		{"10", 10},
		{"100", 100},
		{"1k", 1 << 10},
		{"10k", 10 << 10},
		{"100k", 100 << 10},
		{"1K", 1 << 10},
		{"10K", 10 << 10},
		{"100K", 100 << 10},
		{"1m", 1 << 20},
		{"10m", 10 << 20},
		{"100m", 100 << 20},
		{"1M", 1 << 20},
		{"10M", 10 << 20},
		{"100M", 100 << 20},
		{"1g", 1 << 30},
		{"1G", 1 << 30},
		{fmt.Sprint(uint64(math.MaxUint64) - 1), math.MaxUint64 - 1},
	} {
		if err := s.UnmarshalText([]byte(test.str)); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if s != itoml.Size(test.want) {
			t.Fatalf("wanted: %d got: %d", test.want, s)
		}
	}

	for _, str := range []string{
		fmt.Sprintf("%dk", uint64(math.MaxUint64-1)),
		"10000000000000000000g",
		"abcdef",
		"1KB",
		"√m",
		"a1",
		"",
	} {
		if err := s.UnmarshalText([]byte(str)); err == nil {
			t.Fatalf("input should have failed: %s", str)
		}
	}
}

func TestConfig_Encode(t *testing.T) {
	var c run.Config
	c.Coordinator.WriteTimeout = itoml.Duration(time.Minute)
	buf := new(bytes.Buffer)
	if err := toml.NewEncoder(buf).Encode(&c); err != nil {
		t.Fatal("Failed to encode: ", err)
	}
	got, search := buf.String(), `write-timeout = "1m0s"`
	if !strings.Contains(got, search) {
		t.Fatalf("Encoding config failed.\nfailed to find %s in:\n%s\n", search, got)
	}
}
