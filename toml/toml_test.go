package toml_test

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os/user"
	"runtime"
	"slices"
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
	for _, tc := range []struct {
		str  string
		want itoml.Size
	}{
		// Raw bytes
		{"0", 0},
		{"1", 1},
		{"10", 10},
		{"100", 100},
		// Kibibytes (lower and upper)
		{"1k", 1 << 10},
		{"10k", 10 << 10},
		{"100k", 100 << 10},
		{"1K", 1 << 10},
		{"10K", 10 << 10},
		{"100K", 100 << 10},
		// Mebibytes (lower and upper)
		{"1m", 1 << 20},
		{"10m", 10 << 20},
		{"100m", 100 << 20},
		{"1M", 1 << 20},
		{"10M", 10 << 20},
		{"100M", 100 << 20},
		// Gibibytes (lower and upper)
		{"1g", 1 << 30},
		{"1G", 1 << 30},
		{"10g", 10 << 30},
		// Extreme values
		{fmt.Sprint(uint64(math.MaxUint64)), itoml.Size(math.MaxUint64)},
		{fmt.Sprint(uint64(math.MaxUint64) - 1), itoml.Size(math.MaxUint64 - 1)},
		// Max values that fit with multipliers
		{fmt.Sprintf("%dk", uint64(math.MaxUint64>>10)), itoml.Size(uint64(math.MaxUint64>>10) << 10)},
		{fmt.Sprintf("%dm", uint64(math.MaxUint64>>20)), itoml.Size(uint64(math.MaxUint64>>20) << 20)},
		{fmt.Sprintf("%dg", uint64(math.MaxUint64>>30)), itoml.Size(uint64(math.MaxUint64>>30) << 30)},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.Size
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	for _, tc := range []struct {
		name   string
		str    string
		err    error
		errStr string
	}{
		{"overflow_k", fmt.Sprintf("%dk", uint64(math.MaxUint64-1)), itoml.ErrSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of a uint64: %dk", uint64(math.MaxUint64), uint64(math.MaxUint64-1))},
		{"overflow_g", "10000000000000000000g", itoml.ErrSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of a uint64: 10000000000000000000g", uint64(math.MaxUint64))},
		{"bad_suffix_f", "abcdef", itoml.ErrSizeBadSuffix, "unknown size suffix: f (expected k, m, or g)"},
		{"bad_suffix_B", "1KB", itoml.ErrSizeBadSuffix, "unknown size suffix: B (expected k, m, or g)"},
		{"bad_suffix_t", "1t", itoml.ErrSizeBadSuffix, "unknown size suffix: t (expected k, m, or g)"},
		{"non_numeric", "√m", itoml.ErrSizeParse,
			`invalid size: error parsing "√m": strconv.ParseUint: parsing "√": invalid syntax`},
		{"alpha_numeric", "a1", itoml.ErrSizeParse,
			`invalid size: error parsing "a1": strconv.ParseUint: parsing "a1": invalid syntax`},
		{"empty", "", itoml.ErrSizeEmpty, "size was empty"},
		{"negative", "-1", itoml.ErrSizeParse, "invalid size: negative value not allowed"},
		{"negative_suffix", "-1m", itoml.ErrSizeParse, "invalid size: negative value not allowed"},
		{"parse_overflow", "99999999999999999999", itoml.ErrSizeParse,
			`invalid size: error parsing "99999999999999999999": strconv.ParseUint: parsing "99999999999999999999": value out of range`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s itoml.Size
			err := s.UnmarshalText([]byte(tc.str))
			require.ErrorIs(t, err, tc.err)
			require.EqualError(t, err, tc.errStr)
			require.Zero(t, s, "Size should remain zero after failed unmarshal")
		})
	}
}

func TestSize_MarshalText(t *testing.T) {
	for _, tc := range []struct {
		size itoml.Size
		want string
	}{
		{0, "0"},
		{1, "1"},
		{512, "512"},
		{1023, "1023"},
		// Exact KiB
		{1 << 10, "1k"},
		{10 << 10, "10k"},
		{512 << 10, "512k"},
		// Exact MiB
		{1 << 20, "1m"},
		{10 << 20, "10m"},
		{256 << 20, "256m"},
		// Exact GiB
		{1 << 30, "1g"},
		{10 << 30, "10g"},
		// >= 1g, not %g, but %m → suffix m
		{1<<30 + 1<<20, "1025m"},
		// >= 1g, not %g, not %m, but %k → suffix k
		{1<<30 + 1<<10, "1048577k"},
		// >= 1g, not %g, not %m, not %k → raw bytes
		{1<<30 + 1, "1073741825"},
		// >= 1m, not %m, but %k → suffix k
		{1<<20 + 1<<10, "1025k"},
		// >= 1m, not %m, not %k → raw bytes
		{1<<20 + 1, "1048577"},
		// >= 1k, not %k → raw bytes
		{1<<10 + 1, "1025"},
		// Large values not divisible by any suffix → raw bytes
		{1<<40 + 1, "1099511627777"},
		// Large values not divisible by g but divisible by m → suffix m
		{1<<40 + 1<<20, "1048577m"},
		// Large values not divisible by g or m but divisible by k → suffix k
		{1<<40 + 1<<10, "1073741825k"},
		// Max uint64 (odd, not divisible by any suffix)
		{itoml.Size(math.MaxUint64), fmt.Sprint(uint64(math.MaxUint64))},
	} {
		t.Run(tc.want, func(t *testing.T) {
			b, err := tc.size.MarshalText()
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

func TestSize_RoundTrip(t *testing.T) {
	for _, size := range []itoml.Size{
		0, 1, 1023,
		1 << 10, 100 << 10,
		1 << 20, 100 << 20,
		1 << 30, 100 << 30,
		itoml.Size(math.MaxUint64),
	} {
		t.Run(fmt.Sprint(uint64(size)), func(t *testing.T) {
			b, err := size.MarshalText()
			require.NoError(t, err)
			var got itoml.Size
			require.NoError(t, got.UnmarshalText(b))
			require.Equal(t, size, got)
		})
	}
}

func TestSSize_UnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want itoml.SSize
	}{
		// Zero
		{"0", 0},
		// Positive raw bytes
		{"1", 1},
		{"100", 100},
		// Negative raw bytes
		{"-1", -1},
		{"-100", -100},
		// Positive with suffixes (lower and upper)
		{"1k", itoml.SSize(1 << 10)},
		{"1K", itoml.SSize(1 << 10)},
		{"10k", itoml.SSize(10 << 10)},
		{"1m", itoml.SSize(1 << 20)},
		{"1M", itoml.SSize(1 << 20)},
		{"10m", itoml.SSize(10 << 20)},
		{"1g", itoml.SSize(1 << 30)},
		{"1G", itoml.SSize(1 << 30)},
		{"10g", itoml.SSize(10 << 30)},
		// Negative with suffixes (lower and upper)
		{"-1k", itoml.SSize(-1 << 10)},
		{"-1K", itoml.SSize(-1 << 10)},
		{"-10k", itoml.SSize(-10 << 10)},
		{"-1m", itoml.SSize(-1 << 20)},
		{"-1M", itoml.SSize(-1 << 20)},
		{"-10m", itoml.SSize(-10 << 20)},
		{"-1g", itoml.SSize(-1 << 30)},
		{"-1G", itoml.SSize(-1 << 30)},
		{"-10g", itoml.SSize(-10 << 30)},
		// Extreme positive
		{fmt.Sprint(int64(math.MaxInt64)), itoml.SSize(math.MaxInt64)},
		{fmt.Sprintf("%dk", int64(math.MaxInt64>>10)), itoml.SSize(int64(math.MaxInt64>>10) << 10)},
		{fmt.Sprintf("%dm", int64(math.MaxInt64>>20)), itoml.SSize(int64(math.MaxInt64>>20) << 20)},
		{fmt.Sprintf("%dg", int64(math.MaxInt64>>30)), itoml.SSize(int64(math.MaxInt64>>30) << 30)},
		// Extreme negative
		{fmt.Sprintf("-%d", int64(math.MaxInt64)), itoml.SSize(-math.MaxInt64)},
		{fmt.Sprintf("-%dk", int64(math.MaxInt64>>10)), itoml.SSize(-int64(math.MaxInt64>>10) << 10)},
		{fmt.Sprintf("-%dm", int64(math.MaxInt64>>20)), itoml.SSize(-int64(math.MaxInt64>>20) << 20)},
		{fmt.Sprintf("-%dg", int64(math.MaxInt64>>30)), itoml.SSize(-int64(math.MaxInt64>>30) << 30)},
		// MinInt64: abs(MinInt64) = MaxInt64+1, valid for negative signed values
		{fmt.Sprint(int64(math.MinInt64)), itoml.SSize(math.MinInt64)},
		{fmt.Sprintf("-%dk", uint64(math.MaxInt64>>10)+1), itoml.SSize(math.MinInt64)},
		{fmt.Sprintf("-%dm", uint64(math.MaxInt64>>20)+1), itoml.SSize(math.MinInt64)},
		{fmt.Sprintf("-%dg", uint64(math.MaxInt64>>30)+1), itoml.SSize(math.MinInt64)},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.SSize
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	for _, tc := range []struct {
		name   string
		str    string
		err    error
		errStr string
	}{
		{"empty", "", itoml.ErrSizeEmpty, "size was empty"},
		{"negative_empty", "-", itoml.ErrSizeEmpty, "size was empty"},
		{"bad_suffix", "1t", itoml.ErrSizeBadSuffix, "unknown size suffix: t (expected k, m, or g)"},
		{"negative_bad_suffix", "-1t", itoml.ErrSizeBadSuffix, "unknown size suffix: t (expected k, m, or g)"},
		{"non_numeric", "abc", itoml.ErrSizeBadSuffix, "unknown size suffix: c (expected k, m, or g)"},
		{"negative_non_numeric", "-abc", itoml.ErrSizeBadSuffix, "unknown size suffix: c (expected k, m, or g)"},
		{"alpha_numeric", "a1", itoml.ErrSizeParse,
			`invalid size: error parsing "a1": strconv.ParseUint: parsing "a1": invalid syntax`},
		{"negative_alpha_numeric", "-a1", itoml.ErrSizeParse,
			`invalid size: error parsing "a1": strconv.ParseUint: parsing "a1": invalid syntax`},
		// Overflow: positive value exceeds MaxInt64
		{"overflow_raw", fmt.Sprint(uint64(math.MaxInt64) + 1), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %d", int64(math.MaxInt64), uint64(math.MaxInt64)+1)},
		// Overflow: fits in uint64 but not int64 after multiply
		{"overflow_k", fmt.Sprintf("%dk", uint64(math.MaxInt64>>10)+1), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dk", int64(math.MaxInt64), uint64(math.MaxInt64>>10)+1)},
		{"overflow_m", fmt.Sprintf("%dm", uint64(math.MaxInt64>>20)+1), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dm", int64(math.MaxInt64), uint64(math.MaxInt64>>20)+1)},
		{"overflow_g", fmt.Sprintf("%dg", uint64(math.MaxInt64>>30)+1), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dg", int64(math.MaxInt64), uint64(math.MaxInt64>>30)+1)},
		// Negative overflow: exceeds abs(MinInt64)
		{"negative_overflow_raw", fmt.Sprintf("-%d", uint64(math.MaxInt64)+2), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %d", int64(math.MaxInt64), uint64(math.MaxInt64)+2)},
		{"negative_overflow_k", fmt.Sprintf("-%dk", uint64(math.MaxInt64>>10)+2), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dk", int64(math.MaxInt64), uint64(math.MaxInt64>>10)+2)},
		{"negative_overflow_m", fmt.Sprintf("-%dm", uint64(math.MaxInt64>>20)+2), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dm", int64(math.MaxInt64), uint64(math.MaxInt64>>20)+2)},
		{"negative_overflow_g", fmt.Sprintf("-%dg", uint64(math.MaxInt64>>30)+2), itoml.ErrSSizeOverflow,
			fmt.Sprintf("size would overflow the max size (%d) of an int64: %dg", int64(math.MaxInt64), uint64(math.MaxInt64>>30)+2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var s itoml.SSize
			err := s.UnmarshalText([]byte(tc.str))
			require.ErrorIs(t, err, tc.err)
			require.EqualError(t, err, tc.errStr)
			require.Zero(t, s, "SSize should remain zero after failed unmarshal")
		})
	}
}

func TestSSize_MarshalText(t *testing.T) {
	for _, tc := range []struct {
		size itoml.SSize
		want string
	}{
		{0, "0"},
		{1, "1"},
		{-1, "-1"},
		{512, "512"},
		{-512, "-512"},
		{1023, "1023"},
		{-1023, "-1023"},
		// Exact KiB
		{1 << 10, "1k"},
		{-(1 << 10), "-1k"},
		{10 << 10, "10k"},
		{-(10 << 10), "-10k"},
		// Exact MiB
		{1 << 20, "1m"},
		{-(1 << 20), "-1m"},
		{256 << 20, "256m"},
		{-(256 << 20), "-256m"},
		// Exact GiB
		{1 << 30, "1g"},
		{-(1 << 30), "-1g"},
		{10 << 30, "10g"},
		{-(10 << 30), "-10g"},
		// >= 1g, not %g, but %m → suffix m
		{1<<30 + 1<<20, "1025m"},
		{-(1<<30 + 1<<20), "-1025m"},
		// >= 1g, not %g, not %m, but %k → suffix k
		{1<<30 + 1<<10, "1048577k"},
		{-(1<<30 + 1<<10), "-1048577k"},
		// >= 1g, not %g, not %m, not %k → raw bytes
		{1<<30 + 1, "1073741825"},
		{-(1<<30 + 1), "-1073741825"},
		// >= 1m, not %m, but %k → suffix k
		{1<<20 + 1<<10, "1025k"},
		{-(1<<20 + 1<<10), "-1025k"},
		// >= 1m, not %m, not %k → raw bytes
		{1<<20 + 1, "1048577"},
		{-(1<<20 + 1), "-1048577"},
		// >= 1k, not %k → raw bytes
		{1<<10 + 1, "1025"},
		{-(1<<10 + 1), "-1025"},
		// Large values not divisible by any suffix → raw bytes
		{1<<40 + 1, "1099511627777"},
		{-(1<<40 + 1), "-1099511627777"},
		// Large, not %g but %m → suffix m
		{1<<40 + 1<<20, "1048577m"},
		{-(1<<40 + 1<<20), "-1048577m"},
		// Large, not %g, not %m, but %k → suffix k
		{1<<40 + 1<<10, "1073741825k"},
		{-(1<<40 + 1<<10), "-1073741825k"},
		// Extreme values
		{itoml.SSize(math.MaxInt64), fmt.Sprint(int64(math.MaxInt64))},
		{itoml.SSize(-math.MaxInt64), fmt.Sprint(int64(-math.MaxInt64))},
		{itoml.SSize(math.MinInt64), "-8589934592g"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			b, err := tc.size.MarshalText()
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

func TestSSize_RoundTrip(t *testing.T) {
	for _, size := range []itoml.SSize{
		0, 1, -1, 1023, -1023,
		1 << 10, -(1 << 10), 100 << 10, -(100 << 10),
		1 << 20, -(1 << 20), 100 << 20, -(100 << 20),
		1 << 30, -(1 << 30), 100 << 30, -(100 << 30),
		itoml.SSize(math.MaxInt64), itoml.SSize(-math.MaxInt64),
		itoml.SSize(math.MinInt64),
	} {
		t.Run(fmt.Sprint(int64(size)), func(t *testing.T) {
			b, err := size.MarshalText()
			require.NoError(t, err)
			var got itoml.SSize
			require.NoError(t, got.UnmarshalText(b))
			require.Equal(t, size, got)
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
		{str: `joey`, want: 0, errStr: `strconv.ParseUint: parsing "joey": invalid syntax`},
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
				require.EqualError(t, err, tc.errStr)
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
		{"60", 0, `time: missing unit in duration "60"`},
		{"bob", 0, `time: invalid duration "bob"`},
		{"1d", 0, `time: unknown unit "d" in duration "1d"`},
		{" ", 0, `time: invalid duration " "`},
	}
	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var d itoml.Duration
			err := d.UnmarshalText([]byte(tc.s))
			if tc.errStr == "" {
				require.NoErrorf(t, err, "unexpected error unmarshaling %q", tc.s)
				require.Equalf(t, itoml.Duration(tc.exp), d, "unexpected result unmarshaling %q", tc.s)
			} else {
				require.EqualError(t, err, tc.errStr)
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
		// SSize: single value, negative
		"X_SSIZE": "-128m",
		// SSize: indexed slice with negative values
		"X_SSIZESLICE_0": "-64m",
		"X_SSIZESLICE_1": "256m",
		// SSize: comma-separated slice with negative values
		"X_SSIZESLICE2": "-1g, 512m, -256k",
		// SSize: extend beyond initial length
		"X_SSIZESLICE3_0": "-32m",
		"X_SSIZESLICE3_1": "64m",
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
		SSize           itoml.SSize         `toml:"ssize"`
		SSizeSlice      []itoml.SSize       `toml:"ssizeslice"`
		SSizeSlice2     []itoml.SSize       `toml:"ssizeslice2"`
		SSizeSlice3     []itoml.SSize       `toml:"ssizeslice3"`
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
		SizeSlice3:  []itoml.Size{32 * 1024 * 1024},
		SSizeSlice3: []itoml.SSize{16 * 1024 * 1024},
		IntSlice3:   []int{10, 20, 30},
	}

	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &got)
	require.NoError(t, err)
	// appliedVars assertion is deferred until after we verify the config is correct.

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
		SSize:           itoml.SSize(-128 * 1024 * 1024),
		SSizeSlice:      []itoml.SSize{-64 * 1024 * 1024, 256 * 1024 * 1024},
		SSizeSlice2:     []itoml.SSize{-1024 * 1024 * 1024, 512 * 1024 * 1024, -256 * 1024},
		SSizeSlice3:     []itoml.SSize{-32 * 1024 * 1024, 64 * 1024 * 1024},
		MultiHyphenName: "bobby",
		Ignored:         0,
	}

	require.Equal(t, exp, got, "environmental override failed")
	require.Equal(t, []string{
		"X_BOOL",
		"X_DURATION",
		"X_DURATIONSLICE2",
		"X_DURATIONSLICE_0", "X_DURATIONSLICE_1",
		"X_EMBEDSLICE_0_ES", "X_EMBEDSLICE_0_NAME",
		"X_EMBEDSLICE_1_ES",
		"X_ES",
		"X_FLOAT32", "X_FLOAT64",
		"X_GROUP", "X_GROUPNUMERIC",
		"X_HYPHEN_STRING",
		"X_INT", "X_INT16", "X_INT32", "X_INT64", "X_INT8",
		"X_INTSLICE2",
		"X_INTSLICE3_1",
		"X_INTSLICE_0", "X_INTSLICE_1", "X_INTSLICE_2",
		"X_LOGLEVEL",
		"X_MAXSIZE",
		"X_MULTI_HYPHEN_NAME",
		"X_NESTEDPTR_INT", "X_NESTEDPTR_STRING",
		"X_NESTEDSLICE_0_INT", "X_NESTEDSLICE_0_STRING",
		"X_NESTEDSLICE_1_INT", "X_NESTEDSLICE_1_STRING",
		"X_NESTEDSLICE_2_INT", "X_NESTEDSLICE_2_STRING",
		"X_NESTEDSLICE_3_INT", "X_NESTEDSLICE_3_STRING",
		"X_NESTEDSLICE_4_STRING",
		"X_NESTED_INT", "X_NESTED_STRING",
		"X_SIZESLICE2",
		"X_SIZESLICE3_0", "X_SIZESLICE3_1",
		"X_SIZESLICE_0", "X_SIZESLICE_1",
		"X_SSIZE",
		"X_SSIZESLICE2",
		"X_SSIZESLICE3_0", "X_SSIZESLICE3_1",
		"X_SSIZESLICE_0", "X_SSIZESLICE_1",
		"X_STRING", "X_STRINGS_1",
		"X_STRSLICE2", "X_STRSLICE3",
		"X_STRSLICE3_1", "X_STRSLICE3_3",
		"X_STRSLICE_0", "X_STRSLICE_1", "X_STRSLICE_2",
		"X_UINT", "X_UINT16", "X_UINT32", "X_UINT64", "X_UINT8",
	}, appliedVars)
}

func TestEnvOverride_Errors(t *testing.T) {
	type config struct {
		Int      int            `toml:"int"`
		Uint     uint           `toml:"uint"`
		Float    float64        `toml:"float"`
		Bool     bool           `toml:"bool"`
		Duration itoml.Duration `toml:"duration"`
		Size     itoml.Size     `toml:"size"`
		SSize    itoml.SSize    `toml:"ssize"`
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
		{"X_SSIZE", "not_a_size", "failed to apply X_SSIZE to SSize using TextUnmarshaler toml.SSize and value 'not_a_size': unknown size suffix: e (expected k, m, or g)"},
		// Indexed slice element with invalid value
		{"X_INTS_0", "bad", "failed to apply X_INTS_0 to Ints[0] using type int and value 'bad': strconv.ParseInt: parsing \"bad\": invalid syntax"},
		// Unindexed (comma-separated) slice with invalid value
		{"X_INTS", "1,bad,3", "failed to apply X_INTS to Ints[1] using type int and value 'bad': strconv.ParseInt: parsing \"bad\": invalid syntax"},
	} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var c config
			env := func(s string) string {
				if s == tc.envKey {
					return tc.envVal
				}
				return ""
			}
			appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
			require.EqualError(t, err, tc.errStr)
			require.Empty(t, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	t.Logf("appliedVars: %v", appliedVars)
	require.Equal(t, "alice", c.Name)
	require.Equal(t, []string{"X_NAME"}, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// Element 0 (existing): default host "localhost" applied
	// Element 1 (appended): default host "localhost" should be applied, then port overridden to 9999
	require.Equal(t, []item{
		{Host: "localhost", Port: 80},
		{Host: "localhost", Port: 9999},
	}, c.Items)
	require.Equal(t, []string{
		"X_ITEMS_1_PORT",
		"X_ITEMS_HOST",
	}, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, itoml.Duration(5*time.Minute), *c.Dur)
	require.Equal(t, []string{"X_DUR"}, appliedVars)
}

func TestEnvOverride_PointerToSSize(t *testing.T) {
	// A pointer-to-SSize field should be overridable with negative values.
	type config struct {
		Limit *itoml.SSize `toml:"limit"`
	}

	env := func(s string) string {
		if s == "X_LIMIT" {
			return "-512m"
		}
		return ""
	}

	limit := itoml.SSize(0)
	c := config{Limit: &limit}
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, itoml.SSize(-512*1024*1024), *c.Limit)
	require.Equal(t, []string{"X_LIMIT"}, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, []itoml.Duration{
		itoml.Duration(1 * time.Minute),
		itoml.Duration(2 * time.Minute),
		itoml.Duration(3 * time.Minute),
	}, c.Durations)
	require.Equal(t, []itoml.Size{128 * 1024 * 1024, 256 * 1024 * 1024}, c.Sizes)
	require.Equal(t, []string{"X_DURATIONS", "X_SIZES"}, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// Indexed env var X_VALS_0 grows the slice to length 1.
	// The comma-separated fallback is skipped because element.Len() != 0.
	// X_VALS acts as a default applied to element 0 first, then X_VALS_0 overwrites it.
	require.Equal(t, []string{"override"}, c.Vals)
	require.Equal(t, []string{"X_VALS_0"}, appliedVars)
}

func TestEnvOverride_GrowNestedStructMixedDefaultAndIndexed(t *testing.T) {
	type configSub struct {
		A string `toml:"a"`
		B string `toml:"b"`
	}

	type config struct {
		Sub []configSub `toml:"sub"`
	}

	env := func(s string) string {
		switch s {
		case "X_SUB_0_A":
			return "override [0].a"
		case "X_SUB_B":
			return "default b"
		}
		return ""
	}

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t,
		config{
			Sub: []configSub{
				configSub{A: "override [0].a", B: "default b"},
			},
		},
		c)
	// X_SUB_B appears twice: applied as struct default for element 0 (growth)
	// and element 1 (growth, before discovering no indexed vars).
	require.Equal(t, []string{"X_SUB_0_A", "X_SUB_B"}, appliedVars)
}

func TestEnvOverride_GrowReversedNestedStructMixedDefaultAndIndexed(t *testing.T) {
	type configSub struct {
		B string `toml:"b"`
		A string `toml:"a"`
	}

	type config struct {
		Sub []configSub `toml:"sub"`
	}

	env := func(s string) string {
		switch s {
		case "X_SUB_0_A":
			return "override [0].a"
		case "X_SUB_B":
			return "default b"
		}
		return ""
	}

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t,
		config{
			Sub: []configSub{
				configSub{A: "override [0].a", B: "default b"},
			},
		},
		c)
	require.Equal(t, []string{"X_SUB_0_A", "X_SUB_B"}, appliedVars)
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
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Empty(t, c.Vals)
	require.Empty(t, appliedVars)
}

func TestEnvOverride_SliceGrowthLimit(t *testing.T) {
	type subConfig struct {
		Str   string     `toml:"str"`
		Int   int        `toml:"int"`
		Uint  uint       `toml:"uint"`
		Size  itoml.Size `toml:"size"`
		Bool  bool       `toml:"bool"`
		Float float64    `toml:"float"`
	}

	type config struct {
		Ints    []int       `toml:"ints"`
		Strings []string    `toml:"strings"`
		Sub     []subConfig `toml:"sub"`
	}

	t.Run("indexed overflow", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth+1; i++ {
			envMap[fmt.Sprintf("X_INTS_%d", i)] = fmt.Sprintf("%d", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env override X_INTS_%d would grow slice beyond maximum of %d appended elements",
			itoml.MaxEnvSliceGrowth, itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
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
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env override X_STRINGS has %d comma-separated values, exceeding maximum of %d",
			itoml.MaxEnvSliceGrowth+1, itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
	})

	t.Run("at limit is ok", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth; i++ {
			envMap[fmt.Sprintf("X_INTS_%d", i)] = fmt.Sprintf("%d", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.NoError(t, err)
		require.Len(t, c.Ints, itoml.MaxEnvSliceGrowth)
		var expectedVars []string
		for i := 0; i < itoml.MaxEnvSliceGrowth; i++ {
			expectedVars = append(expectedVars, fmt.Sprintf("X_INTS_%d", i))
		}
		slices.Sort(expectedVars)
		require.Equal(t, expectedVars, appliedVars)
	})

	t.Run("overflow in single slice struct member", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth+1; i++ {
			envMap[fmt.Sprintf("X_SUB_%d_STR", i)] = fmt.Sprintf("%d", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env override X_SUB_%d_STR would grow slice beyond maximum of %d appended elements",
			itoml.MaxEnvSliceGrowth, itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
	})

	t.Run("overflow in multiple slice struct members", func(t *testing.T) {
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth+1; i++ {
			envMap[fmt.Sprintf("X_SUB_%d_STR", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_INT", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_UINT", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_SIZE", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_BOOL", i)] = "true"
			envMap[fmt.Sprintf("X_SUB_%d_FLOAT", i)] = fmt.Sprintf("%d.0", i)
		}
		env := func(s string) string { return envMap[s] }

		var c config
		// Suffixes sorted alphabetically to match sorted IndexedVars in the error message.
		envVarSuffixes := []string{
			"BOOL",
			"FLOAT",
			"INT",
			"SIZE",
			"STR",
			"UINT",
		}
		var envVars []string
		for _, suffix := range envVarSuffixes {
			envVars = append(envVars, fmt.Sprintf("X_SUB_%d_%s", itoml.MaxEnvSliceGrowth, suffix))
		}
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env overrides %s would grow slice beyond maximum of %d appended elements",
			strings.Join(envVars, ","), itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
	})

	t.Run("overflow in multiple slice struct members with default", func(t *testing.T) {
		// The default variable (X_SUB_BOOL) will not be included in the grow overflow error
		// message because it is not one of the environment variables that would force growth
		// of []sub.
		envMap := make(map[string]string)
		for i := 0; i < itoml.MaxEnvSliceGrowth+1; i++ {
			envMap[fmt.Sprintf("X_SUB_%d_STR", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_INT", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_UINT", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_SIZE", i)] = fmt.Sprintf("%d", i)
			envMap[fmt.Sprintf("X_SUB_%d_FLOAT", i)] = fmt.Sprintf("%d.0", i)
		}
		envMap["X_SUB_BOOL"] = "true"
		env := func(s string) string { return envMap[s] }

		var c config
		// Suffixes sorted alphabetically (minus bool, which is a default), to match sorted IndexedVars.
		envVarSuffixes := []string{
			"FLOAT",
			"INT",
			"SIZE",
			"STR",
			"UINT",
		}
		var envVars []string
		for _, suffix := range envVarSuffixes {
			envVars = append(envVars, fmt.Sprintf("X_SUB_%d_%s", itoml.MaxEnvSliceGrowth, suffix))
		}
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, fmt.Sprintf(
			"env overrides %s would grow slice beyond maximum of %d appended elements",
			strings.Join(envVars, ","), itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
	})

}
