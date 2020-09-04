package toml_test

import (
	"fmt"
	"math"
	"os/user"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	itoml "github.com/influxdata/influxdb/v2/toml"
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
		{"1kib", 1 << 10},
		{"10kib", 10 << 10},
		{"100kib", 100 << 10},
		{"1Kib", 1 << 10},
		{"10Kib", 10 << 10},
		{"100Kib", 100 << 10},
		{"1mib", 1 << 20},
		{"10mib", 10 << 20},
		{"100mib", 100 << 20},
		{"1Mib", 1 << 20},
		{"10Mib", 10 << 20},
		{"100Mib", 100 << 20},
		{"1gib", 1 << 30},
		{"1Gib", 1 << 30},
		{"100Gib", 100 << 30},
		{"1tib", 1 << 40},
	} {
		if err := s.UnmarshalText([]byte(test.str)); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if s != itoml.Size(test.want) {
			t.Errorf("wanted: %d got: %d", test.want, s)
		}
	}

	for _, str := range []string{
		fmt.Sprintf("%dk", uint64(math.MaxUint64-1)),
		"10000000000000000000g",
		"abcdef",
		"√m",
		"a1",
		"",
	} {
		if err := s.UnmarshalText([]byte(str)); err == nil {
			t.Errorf("input should have failed: %s", str)
		}
	}
}

func TestFileMode_MarshalText(t *testing.T) {
	for _, test := range []struct {
		mode int
		want string
	}{
		{mode: 0755, want: `0755`},
		{mode: 0777, want: `0777`},
		{mode: 01777, want: `1777`},
	} {
		mode := itoml.FileMode(test.mode)
		if got, err := mode.MarshalText(); err != nil {
			t.Errorf("unexpected error: %s", err)
		} else if test.want != string(got) {
			t.Errorf("wanted: %v got: %v", test.want, string(got))
		}
	}
}

func TestFileMode_UnmarshalText(t *testing.T) {
	for _, test := range []struct {
		str  string
		want uint32
	}{
		{str: ``, want: 0},
		{str: `0777`, want: 0777},
		{str: `777`, want: 0777},
		{str: `1777`, want: 01777},
		{str: `0755`, want: 0755},
	} {
		var mode itoml.FileMode
		if err := mode.UnmarshalText([]byte(test.str)); err != nil {
			t.Errorf("unexpected error: %s", err)
		} else if mode != itoml.FileMode(test.want) {
			t.Errorf("wanted: %04o got: %04o", test.want, mode)
		}
	}
}

func TestGroup_UnmarshalTOML(t *testing.T) {
	// Skip this test on windows since it does not support setting the group anyway.
	if runtime.GOOS == "windows" {
		t.Skip("unsupported on windows")
	}

	// Find the current user ID so we can use that group name.
	u, err := user.Current()
	if err != nil {
		t.Skipf("unable to find the current user: %s", err)
	}

	// Lookup the group by the group id.
	gr, err := user.LookupGroupId(u.Gid)
	if err == nil {
		var group itoml.Group
		if err := group.UnmarshalTOML(gr.Name); err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := u.Gid, strconv.Itoa(int(group)); got != want {
			t.Fatalf("unexpected group id: %s != %s", got, want)
		}
	}

	// Attempt to convert the group to an integer so we can test reading an integer.
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		t.Fatalf("group id is not an integer: %s", err)
	}

	var group itoml.Group
	if err := group.UnmarshalTOML(int64(gid)); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if int(group) != gid {
		t.Fatalf("unexpected group id: %d != %d", gid, int(group))
	}
}

func TestConfig_Encode(t *testing.T) {
	t.Skip("TODO(jsternberg): rewrite this test to use something from platform")
	//var c run.Config
	//c.Coordinator.WriteTimeout = itoml.Duration(time.Minute)
	//buf := new(bytes.Buffer)
	//if err := toml.NewEncoder(buf).Encode(&c); err != nil {
	//	t.Fatal("Failed to encode: ", err)
	//}
	//got, search := buf.String(), `write-timeout = "1m0s"`
	//if !strings.Contains(got, search) {
	//	t.Fatalf("Encoding config failed.\nfailed to find %s in:\n%s\n", search, got)
	//}
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
