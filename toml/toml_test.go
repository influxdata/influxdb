package toml_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/go-cmp/cmp"
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

func TestEnvOverride_Builtins(t *testing.T) {
	envMap := map[string]string{
		"X_STRING":        "a string",
		"X_DURATION":      "1m1s",
		"X_INT":           "1",
		"X_INT8":          "2",
		"X_INT16":         "3",
		"X_INT32":         "4",
		"X_INT64":         "5",
		"X_UINT":          "6",
		"X_UINT8":         "7",
		"X_UINT16":        "8",
		"X_UINT32":        "9",
		"X_UINT64":        "10",
		"X_BOOL":          "true",
		"X_FLOAT32":       "11.5",
		"X_FLOAT64":       "12.5",
		"X_NESTED_STRING": "a nested string",
		"X_NESTED_INT":    "13",
		"X_ES":            "an embedded string",
		"X__":             "-1", // This value should not be applied to the "ignored" field with toml tag -.
	}

	env := func(s string) string {
		return envMap[s]
	}

	type nested struct {
		Str string `toml:"string"`
		Int int    `toml:"int"`
	}
	type Embedded struct {
		ES string `toml:"es"`
	}
	type all struct {
		Str     string         `toml:"string"`
		Dur     itoml.Duration `toml:"duration"`
		Int     int            `toml:"int"`
		Int8    int8           `toml:"int8"`
		Int16   int16          `toml:"int16"`
		Int32   int32          `toml:"int32"`
		Int64   int64          `toml:"int64"`
		Uint    uint           `toml:"uint"`
		Uint8   uint8          `toml:"uint8"`
		Uint16  uint16         `toml:"uint16"`
		Uint32  uint32         `toml:"uint32"`
		Uint64  uint64         `toml:"uint64"`
		Bool    bool           `toml:"bool"`
		Float32 float32        `toml:"float32"`
		Float64 float64        `toml:"float64"`
		Nested  nested         `toml:"nested"`

		Embedded

		Ignored int `toml:"-"`
	}

	var got all
	if err := itoml.ApplyEnvOverrides(env, "X", &got); err != nil {
		t.Fatal(err)
	}

	exp := all{
		Str:     "a string",
		Dur:     itoml.Duration(time.Minute + time.Second),
		Int:     1,
		Int8:    2,
		Int16:   3,
		Int32:   4,
		Int64:   5,
		Uint:    6,
		Uint8:   7,
		Uint16:  8,
		Uint32:  9,
		Uint64:  10,
		Bool:    true,
		Float32: 11.5,
		Float64: 12.5,
		Nested: nested{
			Str: "a nested string",
			Int: 13,
		},
		Embedded: Embedded{
			ES: "an embedded string",
		},
		Ignored: 0,
	}

	if diff := cmp.Diff(got, exp); diff != "" {
		t.Fatal(diff)
	}
}
