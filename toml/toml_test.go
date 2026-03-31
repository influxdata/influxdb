package toml_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	itoml "github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
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
		require.NoError(t, s.UnmarshalText([]byte(test.str)))
		require.Equal(t, itoml.Size(test.want), s)
	}

	for idx, tc := range []struct {
		str         string
		err         error
		errContains string
	}{
		{fmt.Sprintf("%dk", uint64(math.MaxUint64-1)), itoml.ErrSizeOverflow, fmt.Sprintf("%dk", uint64(math.MaxUint64-1))},
		{"10000000000000000000g", itoml.ErrSizeOverflow, "10000000000000000000g"},
		{"abcdef", itoml.ErrSizeBadSuffix, ": f (expected k, m, or g)"},
		{"1KB", itoml.ErrSizeBadSuffix, ": B (expected k, m, or g)"},
		{"√m", itoml.ErrSizeParse, "invalid size: strconv.ParseUint: parsing \"√\": invalid syntax"},
		{"a1", itoml.ErrSizeParse, "invalid size: strconv.ParseUint: parsing \"a1\": invalid syntax"},
		{"", itoml.ErrSizeEmpty, ""},
	} {
		t.Run(fmt.Sprintf("Err_%d", idx), func(t *testing.T) {
			var s itoml.Size
			err := s.UnmarshalText([]byte(tc.str))
			require.ErrorIs(t, err, tc.err)
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
			}
		})
	}
}

func TestFileMode_MarshalText(t *testing.T) {
	for idx, tc := range []struct {
		mode int
		want string
	}{
		{mode: 0, want: ""},
		{mode: 0755, want: `0755`},
		{mode: 0777, want: `0777`},
		{mode: 01777, want: `1777`},
		{mode: math.MaxUint32, want: `37777777777`}, // nonsense, but correct as defined
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			mode := itoml.FileMode(tc.mode)
			s, err := mode.MarshalText()
			require.NoError(t, err, "marshaling FileMode should never return an error")
			require.Equal(t, tc.want, string(s))
		})
	}
}

func TestFileMode_UnmarshalText(t *testing.T) {
	for idx, tc := range []struct {
		str    string
		want   uint32
		errStr string
	}{
		{str: ``, want: 0},
		{str: `0777`, want: 0777},
		{str: `777`, want: 0777},
		{str: `1777`, want: 01777},
		{str: `0755`, want: 0755},
		{str: `37777777777`, want: math.MaxUint32}, // nonsense, but correct as defined
		{str: `joey`, want: 0, errStr: "invalid syntax"},
		{str: `0`, want: 0, errStr: "file mode cannot be zero"},
		{str: `0000`, want: 0, errStr: "file mode cannot be zero"},
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var mode itoml.FileMode
			err := mode.UnmarshalText([]byte(tc.str))
			if tc.errStr == "" {
				require.NoError(t, err, "unexpected error during unmarshaling")
				require.Equal(t, itoml.FileMode(tc.want), mode)
			} else {
				require.Errorf(t, err, "unmarshaling %q should have generated an error", tc.str)
				require.ErrorContains(t, err, tc.errStr)
				require.Zero(t, mode, "mode should remain zero after failed unmarshaling")
			}
		})
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

	// Convert Gid to an integer
	gid, err := strconv.Atoi(u.Gid)
	require.NoError(t, err, "group ID is not an integer")

	// Lookup the group by the group id.
	t.Run("by group name", func(t *testing.T) {
		gr, err := user.LookupGroupId(u.Gid)
		unkGID := user.UnknownGroupIdError(u.Gid)
		if errors.Is(err, unkGID) {
			t.Skipf("skipping because LookupGroupId failed for %q: %s", u.Gid, err)
		}
		require.NoError(t, err, "LookupGroupId failed with error other than user.UnknownGroupIdErr")
		var group itoml.Group
		require.NoError(t, group.UnmarshalTOML(gr.Name))
		require.Equal(t, itoml.Group(gid), group)
	})

	t.Run("by numeric GID", func(t *testing.T) {
		var group itoml.Group
		require.NoError(t, group.UnmarshalTOML(int64(gid)))
		require.Equal(t, itoml.Group(gid), group)
	})

	t.Run("by numeric GID string", func(t *testing.T) {
		var group itoml.Group
		require.NoError(t, group.UnmarshalTOML(fmt.Sprintf("%d", gid)))
		require.Equal(t, itoml.Group(gid), group)
	})

	t.Run("by invalid group name", func(t *testing.T) {
		var group itoml.Group
		fakeGroup := "ThereIsNoWaySomebodyMadeThisARealGroupName_LLC"
		expErr := user.UnknownGroupError(fakeGroup)
		require.ErrorIs(t, group.UnmarshalTOML(fakeGroup), expErr)
		require.Zero(t, group, "group should remain zero after failed unmarshal")
	})

	t.Run("empty string fails", func(t *testing.T) {
		var group itoml.Group
		emptyGroup := ""
		expErr := user.UnknownGroupError(emptyGroup)
		require.ErrorIs(t, group.UnmarshalTOML(emptyGroup), expErr)
		require.Zero(t, group)
	})
}

func TestDuration_Encode(t *testing.T) {
	tcs := []struct {
		d   time.Duration
		exp string
	}{
		{0, "0s"},
		{60, "60ns"},
		{150 * time.Millisecond, "150ms"},
		{2 * time.Second, "2s"},
		{4 * time.Minute, "4m0s"},
		{8 * time.Hour, "8h0m0s"},
		{16 * 24 * time.Hour, "384h0m0s"},
		{16*24*time.Hour + 4*time.Minute + 2*time.Second + 150*time.Millisecond, "384h4m2.15s"},
		{math.MaxInt64, "2562047h47m16.854775807s"},
		{-60, "-60ns"},
		{math.MinInt64, "-2562047h47m16.854775808s"},
	}
	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			td := itoml.Duration(tc.d)
			b, err := td.MarshalText()
			require.NoError(t, err, "MarshalText should never return an error")
			require.Equal(t, tc.exp, string(b))
		})
	}
}

func TestDuration_Decode(t *testing.T) {
	tcs := []struct {
		s      string
		exp    time.Duration
		errStr string
	}{
		{"", 0, ""},
		{"0s", 0, ""},
		{"0", 0, ""},
		{"60ns", 60, ""},
		{"150ms", 150 * time.Millisecond, ""},
		{"2s", 2 * time.Second, ""},
		{"4m0s", 4 * time.Minute, ""},
		{"8h0m0s", 8 * time.Hour, ""},
		{"384h0m0s", 16 * 24 * time.Hour, ""},
		{"384h4m2.15s", 16*24*time.Hour + 4*time.Minute + 2*time.Second + 150*time.Millisecond, ""},
		{"2562047h47m16.854775807s", math.MaxInt64, ""},
		{"-60ns", -60, ""},
		{"-2562047h47m16.854775808s", math.MinInt64, ""},
		{"60", 0, "missing unit in duration"},
		{"bob", 0, `invalid duration "bob"`},
		{"1d", 0, `unknown unit "d" in duration "1d"`},
		{" ", 0, `invalid duration " "`},
	}
	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var d itoml.Duration
			err := d.UnmarshalText([]byte(tc.s))
			if tc.errStr == "" {
				require.NoErrorf(t, err, "unexpected error unmarshaling %q", tc.s)
				require.Equalf(t, itoml.Duration(tc.exp), d, "unexpected result unmarshaling %q", tc.s)
			} else {
				require.Errorf(t, err, "unmarshaling %q should have failed", tc.s)
				require.ErrorContains(t, err, tc.errStr, "incorrect error during unmarshaling")
				require.Equalf(t, itoml.Duration(0), d, "d should not be changed on failed unmarshaling of %q", tc.s)
			}
		})
	}
}

func TestConfig_Encode(t *testing.T) {
	var c *run.Config = run.NewConfig()
	c.Coordinator.WriteTimeout = itoml.Duration(time.Minute)
	buf := new(bytes.Buffer)
	require.NoError(t, toml.NewEncoder(buf).Encode(c))
	require.Contains(t, buf.String(), `write-timeout = "1m0s"`, "Encoding config failed (did not find expected substring)")
}

type stringUnmarshaler struct {
	Text string
}

func (s *stringUnmarshaler) UnmarshalText(data []byte) error {
	s.Text = string(data)
	return nil
}

func currentUserGroup() (int, string, error) {
	curUser, err := user.Current()
	if err != nil {
		return 0, "", fmt.Errorf("error getting current user: %w", err)
	}
	groupID, err := strconv.Atoi(curUser.Gid)
	if err != nil {
		return 0, "", fmt.Errorf("error converting GID (%q) to an integer: %w", curUser.Gid, err)
	}
	group, err := user.LookupGroupId(curUser.Gid)
	if err != nil {
		return 0, "", fmt.Errorf("error lookup group by ID (%q): %w", curUser.Gid, err)
	}
	return groupID, group.Name, nil
}

func TestEnvOverride_Builtins(t *testing.T) {
	// If we run on a platform that doesn't support groups or not in a way resembling
	// Unix groups, then we will use an empty groupOverride and the expected groupID will
	// be 0. This will pass the test. There is also a test case (groupempty) that explicitly
	// tests this on all platforms to be sure it will work on other platforms.
	// On platforms without a concept of group numbers (e.g. Windows), we leave groupnumeric
	// as empty string, which we leave groupNumeric unchanged as 0 and pass the test.
	groupID, groupName, err := currentUserGroup()
	var groupOverride string
	var groupNumeric string
	if err == nil {
		groupNumeric = fmt.Sprintf("%d", groupID)
		if groupName != "" {
			groupOverride = groupName
		} else {
			t.Logf("could not get current user's group name, using GID instead")
			groupOverride = groupNumeric
		}
	} else {
		t.Logf("could not get current user's group: %s", err)
		groupID = 0
	}

	envMap := map[string]string{
		"X_STRING":           "a string",
		"X_HYPHEN_STRING":    "a hyphenated string",
		"X_DURATION":         "1m1s",
		"X_INT":              "1",
		"X_INTEMPTY":         " ",
		"X_INT8":             "2",
		"X_INT16":            "3",
		"X_INT32":            "4",
		"X_INT64":            "5",
		"X_UINT":             "6",
		"X_UINTEMPTY":        " ",
		"X_UINT8":            " 7 ", // spaces
		"X_UINT16":           "8",
		"X_UINT32":           "9",
		"X_UINT64":           "10",
		"X_BOOL":             "true",
		"X_BOOLEMPTY":        " ",
		"X_FLOAT32":          "11.5",
		"X_FLOAT64":          "12.5",
		"X_FLOAT64EMPTY":     " ",
		"X_NESTED_STRING":    "a nested string",
		"X_NESTED_INT":       "13",
		"X_NESTEDPTR_STRING": "a nested pointer string",
		"X_NESTEDPTR_INT":    "14",
		"X_NILPTR_STRING":    "should be ignored",
		"X_NILPTR_INT":       "99",
		"X_ES":               "an embedded string",
		"X__":                "-1", // This value should not be applied to the "ignored" field with toml tag -.
		"X_STRINGS_1":        "c",
		"X_LOGLEVEL":         "warn",
		"X_MAXSIZE":          "128M",
		"X_GROUP":            groupOverride,
		"X_GROUPNUMERIC":     groupNumeric,
		"X_GROUPEMPTY":       "",
		"X_STRSLICE_0":       "alice",
		"X_STRSLICE_1":       "bob",
		"X_STRSLICE_2":       "carol",
		"X_STRSLICE2":        "eve,mallory",
		"X_STRSLICE3":        "eve",     // default value
		"X_STRSLICE3_1":      "mallory", // override for element 1
		"X_STRSLICE3_3":      "trudy",   // appended
		"X_STRSLICE3_5":      "oscar",   // ignored because it's out of bounds because there is no element 4
		"X_INTSLICE_0":       "1",
		"X_INTSLICE_1":       "2",
		"X_INTSLICE_2":       "3",
		"X_INTSLICE2":        "4,5, 6",
		"X_SIZESLICE_0":      "128m",
		"X_SIZESLICE_1":      "256m",
		"X_SIZESLICE2":       "512m, 1g", // with space
		"X_DURATIONSLICE_0":  "60s",
		"X_DURATIONSLICE_1":  "120s",
		"X_DURATIONSLICE2":   "5m, 60m", // with space
		// #5: slice of plain structs (not TextUnmarshaler)
		"X_NESTEDSLICE_0_STRING": "first",
		"X_NESTEDSLICE_0_INT":    "100",
		"X_NESTEDSLICE_1_STRING": "second",
		"X_NESTEDSLICE_1_INT":    "200",
		"X_NESTEDSLICE_2_STRING": "third",
		"X_NESTEDSLICE_2_INT":    "300",
		"X_NESTEDSLICE_3_STRING": "fourth",
		"X_NESTEDSLICE_3_INT":    "400",
		// #1: partially-set struct in extended slice (only string, no int)
		"X_NESTEDSLICE_4_STRING": "fifth",
		// Slice of structs with embedded field, extended via embedded field's env var
		"X_EMBEDSLICE_0_NAME": "first",
		"X_EMBEDSLICE_0_ES":   "embedded0",
		"X_EMBEDSLICE_1_ES":   "embedded1",
		// #3: extend TextUnmarshaler slice beyond initial length
		"X_SIZESLICE3_0": "64m",
		"X_SIZESLICE3_1": "128m",
		// #6: override existing []int elements by index
		"X_INTSLICE3_1":       "99",
		"X_MULTI_HYPHEN_NAME": "bobby",
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
	type nestedWithEmbed struct {
		Name string `toml:"name"`
		Embedded
	}
	type testConfig struct {
		Str             string              `toml:"string"`
		HyphenStr       string              `toml:"hyphen-string"`
		Str2            string              `toml:"string2"`
		Dur             itoml.Duration      `toml:"duration"`
		Int             int                 `toml:"int"`
		IntEmpty        int                 `toml:"intempty"`
		Int8            int8                `toml:"int8"`
		Int16           int16               `toml:"int16"`
		Int32           int32               `toml:"int32"`
		Int64           int64               `toml:"int64"`
		Uint            uint                `toml:"uint"`
		UintEmpty       uint                `toml:"uintempty"`
		Uint8           uint8               `toml:"uint8"`
		Uint16          uint16              `toml:"uint16"`
		Uint32          uint32              `toml:"uint32"`
		Uint64          uint64              `toml:"uint64"`
		Bool            bool                `toml:"bool"`
		BoolEmpty       bool                `toml:"boolempty"`
		Float32         float32             `toml:"float32"`
		Float64         float64             `toml:"float64"`
		Float64Empty    float64             `toml:"float64empty"`
		Nested          nested              `toml:"nested"`
		NestedPtr       *nested             `toml:"nestedptr"`
		NilPtr          *nested             `toml:"nilptr"`
		UnmarshalSlice  []stringUnmarshaler `toml:"strings"`
		LogLevel        zapcore.Level       `toml:"loglevel"`
		MaxSize         itoml.Size          `toml:"maxSize"`
		Group           itoml.Group         `toml:"group"`
		GroupNumeric    itoml.Group         `toml:"groupnumeric"`
		GroupEmpty      itoml.Group         `toml:"groupempty"`
		StrSlice        []string            `toml:"strslice"`
		StrSlice2       []string            `toml:"strslice2"`
		StrSlice3       []string            `toml:"strslice3"`
		IntSlice        []int               `toml:"intslice"`
		IntSlice2       []int               `toml:"intslice2"`
		SizeSlice       []itoml.Size        `toml:"sizeslice"`
		SizeSlice2      []itoml.Size        `toml:"sizeslice2"`
		SizeSlice3      []itoml.Size        `toml:"sizeslice3"`
		DurationSlice   []itoml.Duration    `toml:"durationslice"`
		DurationSlice2  []itoml.Duration    `toml:"durationslice2"`
		NestedSlice     []nested            `toml:"nestedslice"`
		EmbedSlice      []nestedWithEmbed   `toml:"embedslice"`
		IntSlice3       []int               `toml:"intslice3"`
		MultiHyphenName string              `toml:"multi-hyphen-name"`

		Embedded

		Ignored int `toml:"-"`
	}

	got := testConfig{
		IntEmpty:     42,
		UintEmpty:    6,
		BoolEmpty:    true,
		Float64Empty: 9.0,
		Str2:         "b string", // this should not be overwritten because the corresponding env is empty
		NestedPtr:    &nested{},
		UnmarshalSlice: []stringUnmarshaler{
			{Text: "a"},
			{Text: "b"},
		},
		StrSlice3: []string{"alice", "bob", "carol"},
		NestedSlice: []nested{
			{Str: "original0", Int: 0},
			{Str: "original1", Int: 0},
		},
		SizeSlice3: []itoml.Size{32 * 1024 * 1024},
		IntSlice3:  []int{10, 20, 30},
	}

	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &got))

	exp := testConfig{
		Str:          "a string",
		HyphenStr:    "a hyphenated string",
		Str2:         "b string",
		Dur:          itoml.Duration(time.Minute + time.Second),
		Int:          1,
		IntEmpty:     42,
		Int8:         2,
		Int16:        3,
		Int32:        4,
		Int64:        5,
		Uint:         6,
		UintEmpty:    6,
		Uint8:        7,
		Uint16:       8,
		Uint32:       9,
		Uint64:       10,
		Bool:         true,
		BoolEmpty:    true,
		Float32:      11.5,
		Float64:      12.5,
		Float64Empty: 9.0,
		Nested: nested{
			Str: "a nested string",
			Int: 13,
		},
		NestedPtr: &nested{
			Str: "a nested pointer string",
			Int: 14,
		},
		Embedded: Embedded{
			ES: "an embedded string",
		},
		UnmarshalSlice: []stringUnmarshaler{
			{Text: "a"},
			{Text: "c"},
		},
		LogLevel:       zapcore.WarnLevel,
		MaxSize:        128 * 1024 * 1024,
		Group:          itoml.Group(groupID),
		GroupNumeric:   itoml.Group(groupID),
		StrSlice:       []string{"alice", "bob", "carol"},
		StrSlice2:      []string{"eve", "mallory"},
		StrSlice3:      []string{"eve", "mallory", "eve", "trudy"},
		IntSlice:       []int{1, 2, 3},
		IntSlice2:      []int{4, 5, 6},
		SizeSlice:      []itoml.Size{128 * 1024 * 1024, 256 * 1024 * 1024},
		SizeSlice2:     []itoml.Size{512 * 1024 * 1024, 1024 * 1024 * 1024},
		SizeSlice3:     []itoml.Size{64 * 1024 * 1024, 128 * 1024 * 1024},
		DurationSlice:  []itoml.Duration{itoml.Duration(60 * time.Second), itoml.Duration(120 * time.Second)},
		DurationSlice2: []itoml.Duration{itoml.Duration(5 * time.Minute), itoml.Duration(60 * time.Minute)},
		NestedSlice: []nested{
			{Str: "first", Int: 100},
			{Str: "second", Int: 200},
			{Str: "third", Int: 300},
			{Str: "fourth", Int: 400},
			{Str: "fifth"},
		},
		EmbedSlice: []nestedWithEmbed{
			{Name: "first", Embedded: Embedded{ES: "embedded0"}},
			{Embedded: Embedded{ES: "embedded1"}},
		},
		IntSlice3:       []int{10, 99, 30},
		MultiHyphenName: "bobby",
		Ignored:         0,
	}

	require.Equal(t, exp, got, "environmental override failed")
}

func TestEnvOverride_Errors(t *testing.T) {
	type config struct {
		Int      int            `toml:"int"`
		Uint     uint           `toml:"uint"`
		Float    float64        `toml:"float"`
		Bool     bool           `toml:"bool"`
		Duration itoml.Duration `toml:"duration"`
		Size     itoml.Size     `toml:"size"`
		Ints     []int          `toml:"ints"`
	}

	for idx, tc := range []struct {
		envKey string
		envVal string
		errStr string
	}{
		{"X_INT", "not_an_int", "failed to apply X_INT to Int using type int and value 'not_an_int': strconv.ParseInt: parsing \"not_an_int\": invalid syntax"},
		{"X_UINT", "not_a_uint", "failed to apply X_UINT to Uint using type uint and value 'not_a_uint': strconv.ParseUint: parsing \"not_a_uint\": invalid syntax"},
		{"X_UINT", "-1", "failed to apply X_UINT to Uint using type uint and value '-1': strconv.ParseUint: parsing \"-1\": invalid syntax"},
		{"X_FLOAT", "not_a_float", "failed to apply X_FLOAT to Float using type float64 and value 'not_a_float': strconv.ParseFloat: parsing \"not_a_float\": invalid syntax"},
		{"X_BOOL", "not_a_bool", "failed to apply X_BOOL to Bool using type bool and value 'not_a_bool': strconv.ParseBool: parsing \"not_a_bool\": invalid syntax"},
		{"X_DURATION", "not_a_duration", "failed to apply X_DURATION to Duration using TextUnmarshaler toml.Duration and value 'not_a_duration': time: invalid duration \"not_a_duration\""},
		{"X_SIZE", "not_a_size", "failed to apply X_SIZE to Size using TextUnmarshaler toml.Size and value 'not_a_size': unknown size suffix: e (expected k, m, or g)"},
		// Indexed slice element with invalid value
		{"X_INTS_0", "bad", "failed to apply X_INTS_0 to Ints using type int and value 'bad': strconv.ParseInt: parsing \"bad\": invalid syntax"},
		// Unindexed (comma-separated) slice with invalid value
		{"X_INTS", "1,bad,3", "failed to apply X_INTS to Ints using type int and value 'bad': strconv.ParseInt: parsing \"bad\": invalid syntax"},
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var c config
			env := func(s string) string {
				if s == tc.envKey {
					return tc.envVal
				}
				return ""
			}
			err := itoml.ApplyEnvOverrides(env, "X", &c)
			require.EqualError(t, err, tc.errStr)
		})
	}
}

func TestEnvOverride_FieldWithoutTomlTag(t *testing.T) {
	// A field without a toml tag should be reachable by its field name,
	// not by a trailing underscore in the env key.
	type config struct {
		Name string
	}

	env := func(s string) string {
		if s == "X_NAME" {
			return "alice"
		}
		return ""
	}

	var c config
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	require.Equal(t, "alice", c.Name)
}

func TestEnvOverride_DefaultAppliedToNewSliceElements(t *testing.T) {
	// When a default (unindexed) env var is set for a struct slice field,
	// it should be applied to elements appended beyond the initial slice
	// length, just as it is for existing elements.
	type item struct {
		Host string `toml:"host"`
		Port int    `toml:"port"`
	}
	type config struct {
		Items []item `toml:"items"`
	}

	env := func(s string) string {
		switch s {
		// Default host for all elements
		case "X_ITEMS_HOST":
			return "localhost"
		// Element 0 gets just the default host (no indexed overrides)
		// Element 1 is appended and gets an indexed port override
		case "X_ITEMS_1_PORT":
			return "9999"
		default:
			return ""
		}
	}

	c := config{Items: []item{{Port: 80}}}
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	// Element 0 (existing): default host "localhost" applied
	// Element 1 (appended): default host "localhost" should be applied, then port overridden to 9999
	require.Equal(t, []item{
		{Host: "localhost", Port: 80},
		{Host: "localhost", Port: 9999},
	}, c.Items)
}

func TestEnvOverride_PointerToTextUnmarshaler(t *testing.T) {
	// A pointer-to-TextUnmarshaler field should be overridable when non-nil.
	type config struct {
		Dur *itoml.Duration `toml:"dur"`
	}

	env := func(s string) string {
		if s == "X_DUR" {
			return "5m"
		}
		return ""
	}

	dur := itoml.Duration(0)
	c := config{Dur: &dur}
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	require.Equal(t, itoml.Duration(5*time.Minute), *c.Dur)
}

func TestEnvOverride_CommaSeparatedTextUnmarshaler(t *testing.T) {
	// Comma-separated values should work for TextUnmarshaler slice types
	// when starting from an empty slice.
	type config struct {
		Durations []itoml.Duration `toml:"durations"`
		Sizes     []itoml.Size     `toml:"sizes"`
	}

	env := func(s string) string {
		switch s {
		case "X_DURATIONS":
			return "1m,2m,3m"
		case "X_SIZES":
			return "128m, 256m"
		default:
			return ""
		}
	}

	var c config
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	require.Equal(t, []itoml.Duration{
		itoml.Duration(1 * time.Minute),
		itoml.Duration(2 * time.Minute),
		itoml.Duration(3 * time.Minute),
	}, c.Durations)
	require.Equal(t, []itoml.Size{128 * 1024 * 1024, 256 * 1024 * 1024}, c.Sizes)
}

func TestEnvOverride_IndexedOverridesTakePrecedenceOverCommaSeparated(t *testing.T) {
	// When both indexed env vars (X_VALS_0) and a comma-separated env var (X_VALS)
	// exist, indexed overrides take precedence and the comma-separated fallback
	// is not used (because the slice is no longer empty after indexed processing).
	type config struct {
		Vals []string `toml:"vals"`
	}

	env := func(s string) string {
		switch s {
		case "X_VALS":
			return "a,b,c"
		case "X_VALS_0":
			return "override"
		default:
			return ""
		}
	}

	var c config
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	// Indexed env var X_VALS_0 grows the slice to length 1.
	// The comma-separated fallback is skipped because element.Len() != 0.
	// X_VALS acts as a default applied to element 0 first, then X_VALS_0 overwrites it.
	require.Equal(t, []string{"override"}, c.Vals)
}

func TestEnvOverride_SparseIndexedSlice(t *testing.T) {
	// Sparse indexed env vars (e.g. only X_VALS_2 with no _0 or _1) are not
	// reachable: the growth loop starts at index 0 and stops at the first
	// missing index.
	type config struct {
		Vals []string `toml:"vals"`
	}

	env := func(s string) string {
		if s == "X_VALS_2" {
			return "unreachable"
		}
		return ""
	}

	var c config
	require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
	require.Empty(t, c.Vals)
}

func TestEnvOverride_SliceGrowthLimit(t *testing.T) {
	type config struct {
		Ints    []int    `toml:"ints"`
		Strings []string `toml:"strings"`
	}

	t.Run("indexed overflow", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth+1; i++ {
			envMap[fmt.Sprintf("X_INTS_%d", i)] = fmt.Sprintf("%d", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env override X_INTS_%d would grow slice beyond maximum of %d appended elements",
			itoml.MaxEnvSliceGrowth, itoml.MaxEnvSliceGrowth))
	})

	t.Run("comma-separated overflow", func(t *testing.T) {
		parts := make([]string, itoml.MaxEnvSliceGrowth+1)
		for i := range parts {
			parts[i] = fmt.Sprintf("item%d", i)
		}
		env := func(s string) string {
			if s == "X_STRINGS" {
				return strings.Join(parts, ",")
			}
			return ""
		}

		var c config
		err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env override X_STRINGS has %d comma-separated values, exceeding maximum of %d",
			itoml.MaxEnvSliceGrowth+1, itoml.MaxEnvSliceGrowth))
	})

	t.Run("at limit is ok", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth; i++ {
			envMap[fmt.Sprintf("X_INTS_%d", i)] = fmt.Sprintf("%d", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		require.NoError(t, itoml.ApplyEnvOverrides(env, "X", &c))
		require.Len(t, c.Ints, itoml.MaxEnvSliceGrowth)
	})
}
