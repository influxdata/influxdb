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
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	itoml "github.com/influxdata/influxdb/v2/toml"
)

// mapEnv returns a getenv-style lookup closure over m. Keys not in m map
// to "". Used throughout the env-override tests to replace ad-hoc switch
// statements with a declarative fixture.
func mapEnv(m map[string]string) func(string) string {
	return func(s string) string { return m[s] }
}

func TestSizeV1_UnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want itoml.SizeV1
	}{
		// Raw bytes
		{"0", 0},
		{"1", 1},
		{"10", 10},
		{"100", 100},
		// 1.x bare-letter IEC binary suffixes (k/K = KiB, m/M = MiB, g/G = GiB).
		// The V1 alias rewrites these to "kib"/"mib"/"gib" before delegating
		// to humanize, overriding humanize's default SI-decimal interpretation.
		{"1k", 1 << 10},
		{"10k", 10 << 10},
		{"100k", 100 << 10},
		{"1K", 1 << 10},
		{"100K", 100 << 10},
		{"1m", 1 << 20},
		{"100m", 100 << 20},
		{"1M", 1 << 20},
		{"100M", 100 << 20},
		{"1g", 1 << 30},
		{"1G", 1 << 30},
		{"10g", 10 << 30},
		// Whitespace between number and bare suffix is accepted (trimmed during
		// rewrite) so "1 k" still means 1 KiB, not 1 KB via humanize.
		{"1 k", 1 << 10},
		{"1 M", 1 << 20},
		{"  2  G ", 2 << 30},
		// Extended humanize vocabulary, explicit IEC (binary).
		{"1kib", 1 << 10},
		{"1KiB", 1 << 10},
		{"1 KiB", 1 << 10},
		{"1mib", 1 << 20},
		{"1MiB", 1 << 20},
		{"1gib", 1 << 30},
		{"1GiB", 1 << 30},
		{"1tib", 1 << 40},
		// Extended humanize vocabulary, SI (decimal). "kb"/"mb"/"gb" and their
		// variants use powers of 1000 — distinct from the bare-letter forms.
		{"1kb", 1000},
		{"1KB", 1000},
		{"1mb", 1000 * 1000},
		{"1MB", 1000 * 1000},
		{"1gb", 1000 * 1000 * 1000},
		{"1GB", 1000 * 1000 * 1000},
		// Humanize accepts fractional values on explicit suffixes.
		{"1.5kib", 1536},
		{"1.5 MiB", 1024 * 1024 * 3 / 2},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.SizeV1
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	// Error cases: parsing is delegated to humanize, which produces its own
	// error messages. We only assert that an error occurs and the receiver
	// is left unmodified; exact message text is humanize's concern.
	for _, name := range []string{
		"empty", "non_numeric", "alpha_numeric", "negative", "negative_suffix",
		"unknown_suffix", "parse_overflow",
	} {
		input := map[string]string{
			"empty":           "",
			"non_numeric":     "abcdef",
			"alpha_numeric":   "a1",
			"negative":        "-1",
			"negative_suffix": "-1m",
			// Humanize rejects suffixes it doesn't recognize.
			"unknown_suffix": "1xyz",
			// Clearly exceeds uint64 range.
			"parse_overflow": "999999999999999999999999",
		}[name]
		t.Run(name, func(t *testing.T) {
			var s itoml.SizeV1
			require.Error(t, s.UnmarshalText([]byte(input)))
			require.Zero(t, s, "Size should remain zero after failed unmarshal")
		})
	}
}

// TestSizeV1_SupersetOf1x pins the guarantee that SizeV1 parses every input
// 1.x's Size parser accepted, to the same value. The cases below are the
// ones where humanize's float64 path diverges from 1.x's exact integer
// arithmetic (precision loss above 2^53, overflow check at MaxUint64). If
// any of these regress, the V1 fast-path (sizeV1Pattern + strconv) is
// broken and 1.x config files may parse differently on this branch.
func TestSizeV1_SupersetOf1x(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want itoml.SizeV1
	}{
		// Pure decimal at the top of uint64 range.
		{fmt.Sprint(uint64(math.MaxUint64)), math.MaxUint64},
		{fmt.Sprint(uint64(math.MaxUint64) - 1), math.MaxUint64 - 1},
		// Odd integer above 2^53 — float64 would round to 2^53.
		{"9007199254740993", 1<<53 + 1},
		// Same odd value with a suffix.
		{"9007199254740993k", 1<<63 + 1<<10},
		// (2^54-1) * 2^10 = 2^64 - 2^10, fits uint64 but humanize's
		// "too large" check would reject (float64(MaxUint64) == 2^64).
		{"18014398509481983k", math.MaxUint64 - (1 << 10) + 1},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var s itoml.SizeV1
			require.NoError(t, s.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.want, s)
		})
	}
	// Master-1.x ParseUint rejects leading signs and decimal fractions; the
	// V1 fast path must match that rejection even though humanize accepts.
	for _, in := range []string{"+1", "+1k"} {
		t.Run("reject_"+in, func(t *testing.T) {
			var s itoml.SizeV1
			require.Error(t, s.UnmarshalText([]byte(in)))
		})
	}
}

func TestSizeV1_MarshalText(t *testing.T) {
	for _, tc := range []struct {
		size itoml.SizeV1
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
		{itoml.SizeV1(math.MaxUint64), fmt.Sprint(uint64(math.MaxUint64))},
	} {
		t.Run(tc.want, func(t *testing.T) {
			b, err := tc.size.MarshalText()
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

func TestSizeV1_RoundTrip(t *testing.T) {
	// Values stay within float64's exact-integer range (<= 2^53). Humanize
	// uses strconv.ParseFloat internally; values above 2^53 lose precision
	// on parse even though MarshalText writes them exactly. Real influxd
	// configs never approach 2^53 bytes (~9 PiB).
	for _, size := range []itoml.SizeV1{
		0, 1, 1023,
		1 << 10, 100 << 10,
		1 << 20, 100 << 20,
		1 << 30, 100 << 30,
		1 << 40, 100 << 40,
		1 << 50,
	} {
		t.Run(fmt.Sprint(uint64(size)), func(t *testing.T) {
			b, err := size.MarshalText()
			require.NoError(t, err)
			var got itoml.SizeV1
			require.NoError(t, got.UnmarshalText(b))
			require.Equal(t, size, got)
		})
	}
}

func TestSSizeV1_UnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want itoml.SSizeV1
	}{
		{"0", 0},
		// Positive / negative raw bytes
		{"1", 1},
		{"100", 100},
		{"-1", -1},
		{"-100", -100},
		// 1.x bare-letter IEC binary suffixes, positive
		{"1k", itoml.SSizeV1(1 << 10)},
		{"1K", itoml.SSizeV1(1 << 10)},
		{"10k", itoml.SSizeV1(10 << 10)},
		{"1m", itoml.SSizeV1(1 << 20)},
		{"1M", itoml.SSizeV1(1 << 20)},
		{"1g", itoml.SSizeV1(1 << 30)},
		{"1G", itoml.SSizeV1(1 << 30)},
		{"10g", itoml.SSizeV1(10 << 30)},
		// 1.x bare-letter suffixes, negative
		{"-1k", itoml.SSizeV1(-1 << 10)},
		{"-1K", itoml.SSizeV1(-1 << 10)},
		{"-1m", itoml.SSizeV1(-1 << 20)},
		{"-1g", itoml.SSizeV1(-1 << 30)},
		// Whitespace-tolerant forms
		{"1 k", itoml.SSizeV1(1 << 10)},
		{"-1 G", itoml.SSizeV1(-1 << 30)},
		// Extended humanize vocabulary, explicit IEC
		{"1kib", itoml.SSizeV1(1 << 10)},
		{"-1KiB", itoml.SSizeV1(-1 << 10)},
		{"1mib", itoml.SSizeV1(1 << 20)},
		{"1gib", itoml.SSizeV1(1 << 30)},
		{"1tib", itoml.SSizeV1(1 << 40)},
		// Extended humanize vocabulary, SI decimal
		{"1kb", 1000},
		{"-1MB", -1000 * 1000},
		{"1GB", 1000 * 1000 * 1000},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.SSizeV1
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	// Error cases: humanize produces the error messages; we only assert the
	// failure and that the receiver is left unmodified.
	for _, name := range []string{
		"empty", "lone_sign", "non_numeric", "alpha_numeric",
		"unknown_suffix", "overflow_positive",
	} {
		input := map[string]string{
			"empty":             "",
			"lone_sign":         "-",
			"non_numeric":       "abc",
			"alpha_numeric":     "a1",
			"unknown_suffix":    "1xyz",
			"overflow_positive": "99999999999999999999",
		}[name]
		t.Run(name, func(t *testing.T) {
			var s itoml.SSizeV1
			require.Error(t, s.UnmarshalText([]byte(input)))
			require.Zero(t, s, "SSize should remain zero after failed unmarshal")
		})
	}
}

// TestSSizeV1_SupersetOf1x pins the guarantee that SSizeV1 parses every
// input 1.x's SSize parser accepted, to the same value. See
// TestSizeV1_SupersetOf1x for rationale. In addition to the precision
// cases, this exercises 1.x's ParseInt behaviors that humanize
// does not support: leading '+' sign and exact MinInt64.
func TestSSizeV1_SupersetOf1x(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want itoml.SSizeV1
	}{
		// Leading '+' — ParseInt accepts, humanize rejects.
		{"+1", 1},
		{"+1k", 1 << 10},
		{"+1m", 1 << 20},
		{"+1g", 1 << 30},
		// MaxInt64 decimal — float64 would round to 2^63 (out of int64 range).
		{fmt.Sprint(int64(math.MaxInt64)), math.MaxInt64},
		{fmt.Sprint(int64(math.MaxInt64) - 1), math.MaxInt64 - 1},
		// MinInt64 decimal — ParseInt handles this exactly.
		{fmt.Sprint(int64(math.MinInt64)), math.MinInt64},
		{fmt.Sprint(int64(math.MinInt64) + 1), math.MinInt64 + 1},
		// Odd integer above 2^53 — float64 would round to 2^53. (No
		// bare-suffix case for SSize here: any N > 2^53 with a suffix
		// overflows int64 and 1.x rejects it too.)
		{"9007199254740993", (1 << 53) + 1},
		{"-9007199254740993", -((1 << 53) + 1)},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var s itoml.SSizeV1
			require.NoError(t, s.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.want, s)
		})
	}
}

func TestSSizeV1_MarshalText(t *testing.T) {
	for _, tc := range []struct {
		size itoml.SSizeV1
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
		{itoml.SSizeV1(math.MaxInt64), fmt.Sprint(int64(math.MaxInt64))},
		{itoml.SSizeV1(-math.MaxInt64), fmt.Sprint(int64(-math.MaxInt64))},
		{itoml.SSizeV1(math.MinInt64), "-8589934592g"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			b, err := tc.size.MarshalText()
			require.NoError(t, err)
			require.Equal(t, tc.want, string(b))
		})
	}
}

func TestSSizeV1_RoundTrip(t *testing.T) {
	// Values stay within float64's exact-integer range — see TestSizeV1_RoundTrip.
	for _, size := range []itoml.SSizeV1{
		0, 1, -1, 1023, -1023,
		1 << 10, -(1 << 10), 100 << 10, -(100 << 10),
		1 << 20, -(1 << 20), 100 << 20, -(100 << 20),
		1 << 30, -(1 << 30), 100 << 30, -(100 << 30),
		1 << 40, -(1 << 40), 1 << 50, -(1 << 50),
	} {
		t.Run(fmt.Sprint(int64(size)), func(t *testing.T) {
			b, err := size.MarshalText()
			require.NoError(t, err)
			var got itoml.SSizeV1
			require.NoError(t, got.UnmarshalText(b))
			require.Equal(t, size, got)
		})
	}
}

func TestSizeV1_ToInt(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   itoml.SizeV1
		want int
		ok   bool
	}{
		{"zero", 0, 0, true},
		{"small", 1024, 1024, true},
		{"maxInt", itoml.SizeV1(math.MaxInt), math.MaxInt, true},
		{"maxInt_plus_one", itoml.SizeV1(math.MaxInt) + 1, 0, false},
		{"maxUint64", itoml.SizeV1(math.MaxUint64), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.ToInt()
			if !tc.ok {
				require.Error(t, err)
				require.ErrorIs(t, err, itoml.ErrSizeOutOfRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSizeV1_ToInt64(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   itoml.SizeV1
		want int64
		ok   bool
	}{
		{"zero", 0, 0, true},
		{"small", 1024, 1024, true},
		{"maxInt64", itoml.SizeV1(math.MaxInt64), math.MaxInt64, true},
		{"maxInt64_plus_one", itoml.SizeV1(math.MaxInt64) + 1, 0, false},
		{"maxUint64", itoml.SizeV1(math.MaxUint64), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.ToInt64()
			if !tc.ok {
				require.Error(t, err)
				require.ErrorIs(t, err, itoml.ErrSizeOutOfRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSSizeV1_ToInt(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   itoml.SSizeV1
		want int
		ok   bool
	}{
		{"zero", 0, 0, true},
		{"small_positive", 1024, 1024, true},
		{"small_negative", -1024, -1024, true},
		{"maxInt", itoml.SSizeV1(math.MaxInt), math.MaxInt, true},
		{"minInt", itoml.SSizeV1(math.MinInt), math.MinInt, true},
		// These cases only trigger on 32-bit platforms where int == int32.
		// On 64-bit (int == int64) they fit, so skip the failure assertion.
		{"above_int_range_32bit_only", itoml.SSizeV1(math.MaxInt32) + 1, math.MaxInt32 + 1, runtime.GOARCH != "386" && runtime.GOARCH != "arm"},
		{"below_int_range_32bit_only", itoml.SSizeV1(math.MinInt32) - 1, math.MinInt32 - 1, runtime.GOARCH != "386" && runtime.GOARCH != "arm"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.ToInt()
			if !tc.ok {
				require.Error(t, err)
				require.ErrorIs(t, err, itoml.ErrSizeOutOfRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSSizeV1_ToUint64(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   itoml.SSizeV1
		want uint64
		ok   bool
	}{
		{"zero", 0, 0, true},
		{"small_positive", 1024, 1024, true},
		{"maxInt64", itoml.SSizeV1(math.MaxInt64), math.MaxInt64, true},
		{"negative_one", -1, 0, false},
		{"minInt64", itoml.SSizeV1(math.MinInt64), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.in.ToUint64()
			if !tc.ok {
				require.Error(t, err)
				require.ErrorIs(t, err, itoml.ErrSizeOutOfRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
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

	t.Run("by group name with surrounding whitespace", func(t *testing.T) {
		gr, err := user.LookupGroupId(u.Gid)
		unkGID := user.UnknownGroupIdError(u.Gid)
		if errors.Is(err, unkGID) {
			t.Skipf("skipping because LookupGroupId failed for %q: %s", u.Gid, err)
		}
		require.NoError(t, err, "LookupGroupId failed with error other than user.UnknownGroupIdErr")
		// Wrap the valid group name with leading/trailing whitespace to verify
		// that unmarshalGroupName trims the input before lookup.
		var group itoml.Group
		require.NoError(t, group.UnmarshalTOML("  "+gr.Name+"\t"))
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

	t.Run("by numeric GID string with surrounding whitespace", func(t *testing.T) {
		// Wrap the numeric GID with whitespace to verify that the numeric
		// fallback path (LookupGroup fails -> Atoi -> LookupGroupId) also
		// benefits from trimming. Neither Atoi nor LookupGroupId strip
		// whitespace themselves.
		var group itoml.Group
		require.NoError(t, group.UnmarshalTOML(fmt.Sprintf("  %d\t", gid)))
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
		require.EqualError(t, group.UnmarshalTOML(""), "group name is empty")
		require.Zero(t, group)
	})

	t.Run("whitespace-only string fails", func(t *testing.T) {
		var group itoml.Group
		require.EqualError(t, group.UnmarshalTOML("   "), "group name is empty")
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

// defaulterSub is used to verify that ApplyEnvOverrides calls ApplyDefaults on
// freshly grown slice elements.
type defaulterSub struct {
	A string `toml:"a"`
	B string `toml:"b"`
	C string `toml:"c"`
}

func (d *defaulterSub) ApplyDefaults() {
	d.A = "default-a"
	d.B = "default-b"
	d.C = "default-c"
}

// Compile-time check that defaulterSub implements itoml.Defaulter.
var _ itoml.Defaulter = (*defaulterSub)(nil)

// nonDefaulterSub is intentionally missing an ApplyDefaults method, used to
// verify that VerifyConfigType reports missing-Defaulter violations.
type nonDefaulterSub struct {
	X string `toml:"x"`
}

// cyclicRoot and cyclicNode form a self-referential type graph used to verify
// that VerifyConfigType detects cycles. Such a type could not be loaded from
// TOML, but we want the test-time check to catch it first.
type cyclicRoot struct {
	Node cyclicNode `toml:"node"`
}

type cyclicNode struct {
	Next *cyclicNode `toml:"next"`
}

func TestUnmatchedEnvVars_Basic(t *testing.T) {
	environ := []string{
		"X_FOO=1",
		"X_BAR=2",
		"X_BAZ=3",
		"OTHER=ignore",
	}
	applied := []string{"X_FOO", "X_BAZ"}
	require.Equal(t, []string{"X_BAR"}, itoml.UnmatchedEnvVars(environ, "X", applied))
}

func TestUnmatchedEnvVars_AllApplied(t *testing.T) {
	environ := []string{"X_FOO=1", "X_BAR=2"}
	applied := []string{"X_BAR", "X_FOO"}
	require.Empty(t, itoml.UnmatchedEnvVars(environ, "X", applied))
}

func TestUnmatchedEnvVars_NoneApplied(t *testing.T) {
	environ := []string{"X_FOO=1", "X_BAR=2"}
	require.Equal(t, []string{"X_BAR", "X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_IgnoresNonPrefixVars(t *testing.T) {
	environ := []string{
		"X_FOO=1",
		"Y_FOO=2",
		"PATH=/usr/bin",
	}
	require.Equal(t, []string{"X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_BarePrefixIgnored(t *testing.T) {
	// A bare "INFLUXDB" without underscore is not in the namespace and is not
	// reported. Users don't set the bare prefix in practice.
	environ := []string{"X=1", "X_FOO=2"}
	require.Equal(t, []string{"X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_PrefixWithUnderscoreInName(t *testing.T) {
	// Vars sharing a prefix character but not the full prefix are not matched.
	// E.g., for prefix "X", "XX_FOO" should NOT be considered in the namespace.
	environ := []string{"X_FOO=1", "XX_FOO=2"}
	require.Equal(t, []string{"X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_SortedOutput(t *testing.T) {
	environ := []string{"X_C=1", "X_A=2", "X_B=3"}
	require.Equal(t, []string{"X_A", "X_B", "X_C"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_SkipsEntriesWithoutEquals(t *testing.T) {
	environ := []string{"X_FOO=1", "X_BAR", "X_BAZ=3"}
	require.Equal(t, []string{"X_BAZ", "X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_EmptyValue(t *testing.T) {
	// An env var set to empty string is still in the namespace and should be
	// reported as unmatched if not in the applied list. ApplyEnvOverrides treats
	// empty values as "not set" and never reports them in applied, so this is
	// the expected behavior.
	environ := []string{"X_FOO="}
	require.Equal(t, []string{"X_FOO"}, itoml.UnmatchedEnvVars(environ, "X", nil))
}

func TestUnmatchedEnvVars_EmptyPrefix(t *testing.T) {
	require.Nil(t, itoml.UnmatchedEnvVars([]string{"X_FOO=1"}, "", nil))
}

func TestUnmatchedEnvVars_EmptyEnviron(t *testing.T) {
	require.Empty(t, itoml.UnmatchedEnvVars(nil, "X", nil))
}

func TestVerifyConfigType_AllImplemented(t *testing.T) {
	type config struct {
		Subs []defaulterSub  `toml:"subs"`
		Ptrs []*defaulterSub `toml:"ptrs"`
		// Primitive slices and TextUnmarshaler slices are exempt.
		Strs  []string         `toml:"strs"`
		Sizes []itoml.SizeV1   `toml:"sizes"`
		Durs  []itoml.Duration `toml:"durs"`
	}
	require.NoError(t, itoml.VerifyConfigType(config{}))
	require.NoError(t, itoml.VerifyConfigType(&config{}))
}

func TestVerifyConfigType_MissingDefaulter(t *testing.T) {
	type config struct {
		Bad []nonDefaulterSub `toml:"bad"`
	}
	err := itoml.VerifyConfigType(config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonDefaulterSub must implement toml.Defaulter")
}

func TestVerifyConfigType_MissingDefaulterInPointerSlice(t *testing.T) {
	type config struct {
		Bad []*nonDefaulterSub `toml:"bad"`
	}
	err := itoml.VerifyConfigType(config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nonDefaulterSub must implement toml.Defaulter")
}

func TestVerifyConfigType_NestedSliceField(t *testing.T) {
	// VerifyConfigType should descend into struct fields to find nested slices.
	type inner struct {
		Bad []nonDefaulterSub `toml:"bad"`
	}
	type outer struct {
		Inner inner `toml:"inner"`
	}
	err := itoml.VerifyConfigType(outer{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "outer.Inner.Bad")
}

func TestVerifyConfigType_SkipsTomlDashTag(t *testing.T) {
	// Fields tagged toml:"-" are skipped by ApplyEnvOverrides, so they should
	// also be skipped by VerifyConfigType.
	type config struct {
		Bad []nonDefaulterSub `toml:"-"`
	}
	require.NoError(t, itoml.VerifyConfigType(config{}))
}

func TestVerifyConfigType_SkipsUnexportedFields(t *testing.T) {
	type config struct {
		bad []nonDefaulterSub
	}
	require.NoError(t, itoml.VerifyConfigType(config{}))
}

func TestVerifyConfigType_NilArgument(t *testing.T) {
	require.Error(t, itoml.VerifyConfigType(nil))
}

func TestVerifyConfigType_DetectsCycle(t *testing.T) {
	err := itoml.VerifyConfigType(cyclicRoot{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "forms a cycle")
}

func TestVerifyConfigType_SharedTypeNotReported(t *testing.T) {
	// Two sibling fields referencing the same struct type are walked twice
	// but don't form a cycle, so no violation should be reported.
	type sub struct {
		X string `toml:"x"`
	}
	type config struct {
		A sub `toml:"a"`
		B sub `toml:"b"`
	}
	require.NoError(t, itoml.VerifyConfigType(config{}))
}

type nestedConfig struct {
	Str string `toml:"string"`
	Int int    `toml:"int"`
}

func (nc *nestedConfig) ApplyDefaults() {}

type EmbeddedConfig struct {
	ES string `toml:"es"`
}
type nestedWithEmbedConfig struct {
	Name string `toml:"name"`
	EmbeddedConfig
}

func (c *nestedWithEmbedConfig) ApplyDefaults() {}

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

	type testConfig struct {
		Str             string                  `toml:"string"`
		HyphenStr       string                  `toml:"hyphen-string"`
		Str2            string                  `toml:"string2"`
		Dur             itoml.Duration          `toml:"duration"`
		Int             int                     `toml:"int"`
		IntEmpty        int                     `toml:"intempty"`
		Int8            int8                    `toml:"int8"`
		Int16           int16                   `toml:"int16"`
		Int32           int32                   `toml:"int32"`
		Int64           int64                   `toml:"int64"`
		Uint            uint                    `toml:"uint"`
		UintEmpty       uint                    `toml:"uintempty"`
		Uint8           uint8                   `toml:"uint8"`
		Uint16          uint16                  `toml:"uint16"`
		Uint32          uint32                  `toml:"uint32"`
		Uint64          uint64                  `toml:"uint64"`
		Bool            bool                    `toml:"bool"`
		BoolEmpty       bool                    `toml:"boolempty"`
		Float32         float32                 `toml:"float32"`
		Float64         float64                 `toml:"float64"`
		Float64Empty    float64                 `toml:"float64empty"`
		Nested          nestedConfig            `toml:"nested"`
		NestedPtr       *nestedConfig           `toml:"nestedptr"`
		NilPtr          *nestedConfig           `toml:"nilptr"`
		UnmarshalSlice  []stringUnmarshaler     `toml:"strings"`
		LogLevel        zapcore.Level           `toml:"loglevel"`
		MaxSize         itoml.SizeV1            `toml:"maxSize"`
		Group           itoml.Group             `toml:"group"`
		GroupNumeric    itoml.Group             `toml:"groupnumeric"`
		GroupEmpty      itoml.Group             `toml:"groupempty"`
		StrSlice        []string                `toml:"strslice"`
		StrSlice2       []string                `toml:"strslice2"`
		StrSlice3       []string                `toml:"strslice3"`
		IntSlice        []int                   `toml:"intslice"`
		IntSlice2       []int                   `toml:"intslice2"`
		SizeSlice       []itoml.SizeV1          `toml:"sizeslice"`
		SizeSlice2      []itoml.SizeV1          `toml:"sizeslice2"`
		SizeSlice3      []itoml.SizeV1          `toml:"sizeslice3"`
		DurationSlice   []itoml.Duration        `toml:"durationslice"`
		DurationSlice2  []itoml.Duration        `toml:"durationslice2"`
		NestedSlice     []nestedConfig          `toml:"nestedslice"`
		EmbedSlice      []nestedWithEmbedConfig `toml:"embedslice"`
		IntSlice3       []int                   `toml:"intslice3"`
		SSize           itoml.SSizeV1           `toml:"ssize"`
		SSizeSlice      []itoml.SSizeV1         `toml:"ssizeslice"`
		SSizeSlice2     []itoml.SSizeV1         `toml:"ssizeslice2"`
		SSizeSlice3     []itoml.SSizeV1         `toml:"ssizeslice3"`
		MultiHyphenName string                  `toml:"multi-hyphen-name"`

		EmbeddedConfig

		Ignored int `toml:"-"`
	}

	got := testConfig{
		IntEmpty:     42,
		UintEmpty:    6,
		BoolEmpty:    true,
		Float64Empty: 9.0,
		Str2:         "b string", // this should not be overwritten because the corresponding env is empty
		NestedPtr:    &nestedConfig{},
		UnmarshalSlice: []stringUnmarshaler{
			{Text: "a"},
			{Text: "b"},
		},
		StrSlice3: []string{"alice", "bob", "carol"},
		NestedSlice: []nestedConfig{
			{Str: "original0", Int: 0},
			{Str: "original1", Int: 0},
		},
		SizeSlice3:  []itoml.SizeV1{32 * 1024 * 1024},
		SSizeSlice3: []itoml.SSizeV1{16 * 1024 * 1024},
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
		Nested: nestedConfig{
			Str: "a nested string",
			Int: 13,
		},
		NestedPtr: &nestedConfig{
			Str: "a nested pointer string",
			Int: 14,
		},
		EmbeddedConfig: EmbeddedConfig{
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
		StrSlice3:      []string{"alice", "mallory", "carol", "trudy"},
		IntSlice:       []int{1, 2, 3},
		IntSlice2:      []int{4, 5, 6},
		SizeSlice:      []itoml.SizeV1{128 * 1024 * 1024, 256 * 1024 * 1024},
		SizeSlice2:     []itoml.SizeV1{512 * 1024 * 1024, 1024 * 1024 * 1024},
		SizeSlice3:     []itoml.SizeV1{64 * 1024 * 1024, 128 * 1024 * 1024},
		DurationSlice:  []itoml.Duration{itoml.Duration(60 * time.Second), itoml.Duration(120 * time.Second)},
		DurationSlice2: []itoml.Duration{itoml.Duration(5 * time.Minute), itoml.Duration(60 * time.Minute)},
		NestedSlice: []nestedConfig{
			{Str: "first", Int: 100},
			{Str: "second", Int: 200},
			{Str: "third", Int: 300},
			{Str: "fourth", Int: 400},
			{Str: "fifth"},
		},
		EmbedSlice: []nestedWithEmbedConfig{
			{Name: "first", EmbeddedConfig: EmbeddedConfig{ES: "embedded0"}},
			{EmbeddedConfig: EmbeddedConfig{ES: "embedded1"}},
		},
		IntSlice3:       []int{10, 99, 30},
		SSize:           itoml.SSizeV1(-128 * 1024 * 1024),
		SSizeSlice:      []itoml.SSizeV1{-64 * 1024 * 1024, 256 * 1024 * 1024},
		SSizeSlice2:     []itoml.SSizeV1{-1024 * 1024 * 1024, 512 * 1024 * 1024, -256 * 1024},
		SSizeSlice3:     []itoml.SSizeV1{-32 * 1024 * 1024, 64 * 1024 * 1024},
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
		"X_STRSLICE2",
		"X_STRSLICE3_1", "X_STRSLICE3_3",
		"X_STRSLICE_0", "X_STRSLICE_1", "X_STRSLICE_2",
		"X_UINT", "X_UINT16", "X_UINT32", "X_UINT64", "X_UINT8",
	}, appliedVars)
}

func TestEnvOverride_Errors(t *testing.T) {
	type noDefaulter struct {
		Val int `toml:"val"`
	}
	type config struct {
		Int          int            `toml:"int"`
		Uint         uint           `toml:"uint"`
		Float        float64        `toml:"float"`
		Bool         bool           `toml:"bool"`
		Duration     itoml.Duration `toml:"duration"`
		Size         itoml.SizeV1   `toml:"size"`
		SSize        itoml.SSizeV1  `toml:"ssize"`
		Ints         []int          `toml:"ints"`
		NoDefaulters []noDefaulter  `toml:"nodefaulter"`
	}

	for idx, tc := range []struct {
		envKey string
		envVal string
		errStr string
	}{
		{"X_INT", "not_an_int", `failed to apply X_INT to Int using type int and value "not_an_int": strconv.ParseInt: parsing "not_an_int": invalid syntax`},
		{"X_UINT", "not_a_uint", `failed to apply X_UINT to Uint using type uint and value "not_a_uint": strconv.ParseUint: parsing "not_a_uint": invalid syntax`},
		{"X_UINT", "-1", `failed to apply X_UINT to Uint using type uint and value "-1": strconv.ParseUint: parsing "-1": invalid syntax`},
		{"X_FLOAT", "not_a_float", `failed to apply X_FLOAT to Float using type float64 and value "not_a_float": strconv.ParseFloat: parsing "not_a_float": invalid syntax`},
		{"X_BOOL", "not_a_bool", `failed to apply X_BOOL to Bool using type bool and value "not_a_bool": strconv.ParseBool: parsing "not_a_bool": invalid syntax`},
		{"X_DURATION", "not_a_duration", `failed to apply X_DURATION to Duration using TextUnmarshaler toml.Duration and value "not_a_duration": time: invalid duration "not_a_duration"`},
		// Indexed slice element with invalid value
		{"X_INTS_0", "bad", `failed to apply X_INTS_0 to Ints[0] using type int and value "bad": strconv.ParseInt: parsing "bad": invalid syntax`},
		// Unindexed (comma-separated) slice with invalid value
		{"X_INTS", "1,bad,3", `failed to apply X_INTS to Ints[1] using type int and value "bad": strconv.ParseInt: parsing "bad": invalid syntax`},
		{"X_NODEFAULTER_0_VAL", "1", "NoDefaulters: slice element type toml_test.noDefaulter does not implement toml.Defaulter"},
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

	t.Run("nodefaultptr", func(t *testing.T) {
		type config2 struct {
			NoDefaulterPtrs []noDefaulter `toml:"nodefaulterptr"`
		}
		var c config2
		env := func(s string) string {
			if s == "X_NODEFAULTPTR_0_VAL" {
				return "1"
			}
			return ""
		}
		appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.EqualError(t, err, "NoDefaulterPtrs: slice element type toml_test.noDefaulter does not implement toml.Defaulter")
		require.Empty(t, appliedVars)
	})

	// Invalid size / ssize values delegate to humanize, whose error messages
	// are not part of its public API (constructed inline with fmt.Errorf,
	// not exported as sentinels). Pin only the env-override wrapper prefix —
	// code we own — so these assertions survive humanize or strconv churn.
	for _, tc := range []struct {
		envKey, envVal, wrapperPrefix string
	}{
		{"X_SIZE", "not_a_size", `failed to apply X_SIZE to Size using TextUnmarshaler toml.SizeV1 and value "not_a_size":`},
		{"X_SSIZE", "not_a_size", `failed to apply X_SSIZE to SSize using TextUnmarshaler toml.SSizeV1 and value "not_a_size":`},
	} {
		t.Run(tc.envKey, func(t *testing.T) {
			var c config
			env := func(s string) string {
				if s == tc.envKey {
					return tc.envVal
				}
				return ""
			}
			appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
			require.ErrorContains(t, err, tc.wrapperPrefix)
			require.Empty(t, appliedVars)
		})
	}
}

// TestEnvOverride_NumericPrefixes verifies that integer and unsigned-integer
// env var values support C-style numeric prefixes via strconv.ParseInt/ParseUint
// with base 0:
//
//   - 0x / 0X for hex
//   - 0o / 0O for octal
//   - bare 0 prefix for octal (e.g., "010" is 8)
//   - 0b / 0B for binary
//   - otherwise decimal
//
// This is intentionally more permissive than TOML's integer syntax.
func TestEnvOverride_NumericPrefixes(t *testing.T) {
	type config struct {
		Int  int  `toml:"int"`
		Uint uint `toml:"uint"`
	}

	for _, tc := range []struct {
		name     string
		envValue string
		wantInt  int
		wantUint uint
		signedTC bool // if true, only test the signed Int field (negative values)
	}{
		// Decimal
		{name: "decimal", envValue: "42", wantInt: 42, wantUint: 42},
		{name: "decimal_zero", envValue: "0", wantInt: 0, wantUint: 0},

		// Hex (lower and upper case prefix)
		{name: "hex_lower", envValue: "0x10", wantInt: 16, wantUint: 16},
		{name: "hex_upper", envValue: "0X10", wantInt: 16, wantUint: 16},
		{name: "hex_mixed_digits", envValue: "0xDeadBeef", wantInt: 0xDEADBEEF, wantUint: 0xDEADBEEF},

		// Octal with explicit 0o prefix
		{name: "octal_o_lower", envValue: "0o10", wantInt: 8, wantUint: 8},
		{name: "octal_o_upper", envValue: "0O10", wantInt: 8, wantUint: 8},

		// Octal with bare leading zero (C-style)
		{name: "octal_bare", envValue: "010", wantInt: 8, wantUint: 8},
		{name: "octal_bare_755", envValue: "0755", wantInt: 0755, wantUint: 0755},

		// Binary
		{name: "binary_lower", envValue: "0b1010", wantInt: 10, wantUint: 10},
		{name: "binary_upper", envValue: "0B1010", wantInt: 10, wantUint: 10},

		// Negative values (signed only — uint rejects negatives)
		{name: "negative_decimal", envValue: "-42", wantInt: -42, signedTC: true},
		{name: "negative_hex", envValue: "-0x10", wantInt: -16, signedTC: true},
		{name: "negative_octal_bare", envValue: "-010", wantInt: -8, signedTC: true},
		{name: "negative_binary", envValue: "-0b1010", wantInt: -10, signedTC: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Test Int field.
			t.Run("int", func(t *testing.T) {
				env := func(s string) string {
					if s == "X_INT" {
						return tc.envValue
					}
					return ""
				}
				var c config
				_, err := itoml.ApplyEnvOverrides(env, "X", &c)
				require.NoError(t, err)
				require.Equal(t, tc.wantInt, c.Int)
			})

			// Test Uint field, skipping cases that target signed-only behavior.
			if tc.signedTC {
				return
			}
			t.Run("uint", func(t *testing.T) {
				env := func(s string) string {
					if s == "X_UINT" {
						return tc.envValue
					}
					return ""
				}
				var c config
				_, err := itoml.ApplyEnvOverrides(env, "X", &c)
				require.NoError(t, err)
				require.Equal(t, tc.wantUint, c.Uint)
			})
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

type hostConfig struct {
	Host string `toml:"host"`
	Port int    `toml:"port"`
}

func (hc *hostConfig) ApplyDefaults() {
	hc.Host = "127.0.0.1"
}

func TestEnvOverride_DefaultAppliedToNewSliceElements(t *testing.T) {
	// When a default (unindexed) env var is set for a struct slice field,
	// it should be applied to elements appended beyond the initial slice
	// length, just as it is for existing elements.
	type config struct {
		Items []hostConfig `toml:"items"`
	}

	// X_ITEMS_HOST is the default host for all elements; element 0 gets just
	// that default (no indexed overrides) while element 1 is appended and
	// also gets an indexed port override.
	env := mapEnv(map[string]string{
		"X_ITEMS_HOST":   "localhost",
		"X_ITEMS_1_PORT": "9999",
	})

	c := config{Items: []hostConfig{{Port: 80}}}
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// Element 0 (existing): default host "localhost" applied
	// Element 1 (appended): default host "localhost" should be applied, then port overridden to 9999
	require.Equal(t, []hostConfig{
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
		Limit *itoml.SSizeV1 `toml:"limit"`
	}

	env := func(s string) string {
		if s == "X_LIMIT" {
			return "-512m"
		}
		return ""
	}

	limit := itoml.SSizeV1(0)
	c := config{Limit: &limit}
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, itoml.SSizeV1(-512*1024*1024), *c.Limit)
	require.Equal(t, []string{"X_LIMIT"}, appliedVars)
}

func TestEnvOverride_CommaSeparatedTextUnmarshaler(t *testing.T) {
	// Comma-separated values should work for TextUnmarshaler slice types
	// when starting from an empty slice.
	type config struct {
		Durations []itoml.Duration `toml:"durations"`
		Sizes     []itoml.SizeV1   `toml:"sizes"`
	}

	env := mapEnv(map[string]string{
		"X_DURATIONS": "1m,2m,3m",
		"X_SIZES":     "128m, 256m",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, []itoml.Duration{
		itoml.Duration(1 * time.Minute),
		itoml.Duration(2 * time.Minute),
		itoml.Duration(3 * time.Minute),
	}, c.Durations)
	require.Equal(t, []itoml.SizeV1{128 * 1024 * 1024, 256 * 1024 * 1024}, c.Sizes)
	require.Equal(t, []string{"X_DURATIONS", "X_SIZES"}, appliedVars)
}

func TestEnvOverride_LeafTypesCannotMixIndexedAndUnindexed(t *testing.T) {
	// For leaf type slices, it is an error to have both indexed and unindexed
	// environment variables.
	type config struct {
		Vals []string `toml:"vals"`
	}

	env := mapEnv(map[string]string{
		"X_VALS":   "a,b,c",
		"X_VALS_0": "override",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.EqualError(t, err, "unindexed env override X_VALS would conflict with indexed overrides (X_VALS_0). Use either indexed or unindexed only for this config")
	require.Empty(t, appliedVars)
}

func TestEnvOverride_LeafTypeUnindexedErasesExisting(t *testing.T) {
	// For leaf type slices, unindexed environment variables will
	// completely overwrite existing values instead of appending.
	type config struct {
		Vals []string `toml:"vals"`
	}

	env := mapEnv(map[string]string{"X_VALS": "a,b,c"})

	for _, tc := range []struct {
		name string
		c    config
	}{
		{"no slice", config{}},
		{"empty slice", config{Vals: []string{}}},
		{"nil slice", config{Vals: nil}},
		{"slice grows", config{Vals: []string{"alice", "bob"}}},
		{"slice shrinks", config{Vals: []string{"alice", "bob", "carol", "dave"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.c
			appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
			require.NoError(t, err)
			require.Equal(t, config{Vals: []string{"a", "b", "c"}}, c)
			require.Equal(t, []string{"X_VALS"}, appliedVars)
		})
	}
}

// TestEnvOverride_UnexportedSliceField is a regression test: the struct-field
// walker let unexported slice fields through, so the slice code paths that
// call reflect.Value.Set (Append for indexed growth, MakeSlice for unindexed
// CSV) would panic on a non-settable value.
func TestEnvOverride_UnexportedSliceField(t *testing.T) {
	type cfg struct {
		vals []string
	}

	for _, tc := range []struct {
		name    string
		envName string
	}{
		{"unindexed CSV triggers MakeSlice", "X_VALS"},
		{"indexed append triggers Append", "X_VALS_0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := func(s string) string {
				if s == tc.envName {
					return "a,b,c"
				}
				return ""
			}
			var c cfg
			var appliedVars []string
			var err error
			require.NotPanics(t, func() {
				appliedVars, err = itoml.ApplyEnvOverrides(env, "X", &c)
			})
			require.NoError(t, err)
			require.Empty(t, appliedVars, "unexported field should not be reported as applied")
			require.Nil(t, c.vals, "unexported field must not be modified")
		})
	}
}

func TestEnvOverride_GrowNestedStructMixedDefaultAndIndexed(t *testing.T) {
	type config struct {
		Sub []defaulterSub `toml:"sub"`
	}

	env := mapEnv(map[string]string{
		"X_SUB_0_A": "override [0].a",
		"X_SUB_B":   "default b",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t,
		config{
			Sub: []defaulterSub{
				defaulterSub{A: "override [0].a", B: "default b", C: "default-c"},
			},
		},
		c)
	// X_SUB_B appears twice: applied as struct default for element 0 (growth)
	// and element 1 (growth, before discovering no indexed vars).
	require.Equal(t, []string{"X_SUB_0_A", "X_SUB_B"}, appliedVars)
}

type reverseConfigSub struct {
	B string `toml:"b"`
	A string `toml:"a"`
}

func (c *reverseConfigSub) ApplyDefaults() {}

func TestEnvOverride_GrowReversedNestedStructMixedDefaultAndIndexed(t *testing.T) {
	type config struct {
		Sub []reverseConfigSub `toml:"sub"`
	}

	env := mapEnv(map[string]string{
		"X_SUB_0_A": "override [0].a",
		"X_SUB_B":   "default b",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t,
		config{
			Sub: []reverseConfigSub{
				reverseConfigSub{A: "override [0].a", B: "default b"},
			},
		},
		c)
	require.Equal(t, []string{"X_SUB_0_A", "X_SUB_B"}, appliedVars)
}

func TestEnvOverride_GrowAppliesBuiltinDefaults(t *testing.T) {
	// When a slice element type implements Defaulter, grown elements should be
	// seeded with ApplyDefaults() before env overrides are applied. Fields not set
	// by env vars should retain their built-in defaults.
	type config struct {
		Sub []defaulterSub `toml:"sub"`
	}

	env := func(s string) string {
		if s == "X_SUB_0_A" {
			return "override-a"
		}
		return ""
	}

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// Element 0 grown: ApplyDefaults seeds A/B/C with defaults, then X_SUB_0_A overrides A.
	require.Equal(t, []defaulterSub{
		{A: "override-a", B: "default-b", C: "default-c"},
	}, c.Sub)
	require.Equal(t, []string{"X_SUB_0_A"}, appliedVars)
}

func TestEnvOverride_GrowIndexedOverridesBuiltinDefaults(t *testing.T) {
	// All three precedence layers exercised: built-in defaults (weakest), unindexed
	// env defaults (broadcast), indexed env vars (strongest).
	type config struct {
		Sub []defaulterSub `toml:"sub"`
	}

	env := mapEnv(map[string]string{
		"X_SUB_B":   "broadcast-b", // unindexed broadcast — overrides built-in B
		"X_SUB_0_C": "indexed-c",   // indexed override — wins over both
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// A: from ApplyDefaults (no env). B: from broadcast (overrides built-in). C: from indexed.
	require.Equal(t, []defaulterSub{
		{A: "default-a", B: "broadcast-b", C: "indexed-c"},
	}, c.Sub)
	require.Equal(t, []string{"X_SUB_0_C", "X_SUB_B"}, appliedVars)
}

func TestEnvOverride_GrowApplyDefaultsNotCalledOnExisting(t *testing.T) {
	// ApplyDefaults must not be called on elements that already exist in the slice.
	// Doing so would overwrite values set by TOML parsing.
	type config struct {
		Sub []defaulterSub `toml:"sub"`
	}

	env := func(s string) string { return "" }

	c := config{Sub: []defaulterSub{{A: "from-toml", B: "from-toml", C: "from-toml"}}}
	_, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	// The existing element must be untouched.
	require.Equal(t, []defaulterSub{{A: "from-toml", B: "from-toml", C: "from-toml"}}, c.Sub)
}

func TestEnvOverride_GrowApplyDefaultsForPointerSlice(t *testing.T) {
	// Pointer slice elements ([]*T) should also get ApplyDefaults called when grown.
	type config struct {
		Sub []*defaulterSub `toml:"sub"`
	}

	env := func(s string) string {
		if s == "X_SUB_0_A" {
			return "override-a"
		}
		return ""
	}

	var c config
	_, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Len(t, c.Sub, 1)
	require.Equal(t, &defaulterSub{A: "override-a", B: "default-b", C: "default-c"}, c.Sub[0])
}

func TestEnvOverride_AllocatesNilPointerToTextUnmarshaler(t *testing.T) {
	// A nil *toml.Duration field should be auto-allocated when an env var is
	// set for it. Without this, the field would be silently skipped because the
	// pointer is nil.
	type config struct {
		Dur *itoml.Duration `toml:"dur"`
	}

	env := func(s string) string {
		if s == "X_DUR" {
			return "5m"
		}
		return ""
	}

	var c config // c.Dur is nil
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.NotNil(t, c.Dur)
	require.Equal(t, itoml.Duration(5*time.Minute), *c.Dur)
	require.Equal(t, []string{"X_DUR"}, appliedVars)
}

func TestEnvOverride_NilPointerToTextUnmarshalerSkippedWhenNoValue(t *testing.T) {
	// Without an env var, a nil *toml.Duration field stays nil.
	type config struct {
		Dur *itoml.Duration `toml:"dur"`
	}

	env := func(s string) string { return "" }

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Nil(t, c.Dur)
	require.Empty(t, appliedVars)
}

func TestEnvOverride_AllocatesNilPointerToScalar(t *testing.T) {
	// A nil pointer to a primitive scalar should also be auto-allocated.
	type config struct {
		Count *int `toml:"count"`
	}

	env := func(s string) string {
		if s == "X_COUNT" {
			return "42"
		}
		return ""
	}

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.NotNil(t, c.Count)
	require.Equal(t, 42, *c.Count)
	require.Equal(t, []string{"X_COUNT"}, appliedVars)
}

func TestEnvOverride_AllocatesPointerWhenGrowingScalarSlice(t *testing.T) {
	// Entries should be allocated when growing a slice of pointers to scalars.
	type config struct {
		Names  []*string `toml:"names"`
		Counts []*int    `toml:"counts"`
	}

	// We'll use indexed env vars to grow names, and unindexed csv to grow counts
	// to check both code paths.
	env := mapEnv(map[string]string{
		"X_NAMES_0": "Alice",
		"X_NAMES_1": "Bob",
		"X_NAMES_2": "Carol",
		"X_COUNTS":  "100,200,300",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)

	alice := "Alice"
	bob := "Bob"
	carol := "Carol"
	n100 := 100
	n200 := 200
	n300 := 300
	require.EqualValues(t, &config{
		Names:  []*string{&alice, &bob, &carol},
		Counts: []*int{&n100, &n200, &n300},
	}, &c)

	require.ElementsMatch(t, []string{
		"X_NAMES_0", "X_NAMES_1", "X_NAMES_2",
		"X_COUNTS",
	}, appliedVars)
}

func TestEnvOverride_AllocatesPointerWhenGrowingTextUnmarshalerSlice(t *testing.T) {
	// Entries should be allocated when growing a slice of pointers to TextUnmarshalers.
	type config struct {
		Durations []*itoml.Duration `toml:"durations"`
		Sizes     []*itoml.SizeV1   `toml:"sizes"`
	}

	// We'll use indexed env vars to grow durations, and unindexed csv to grow sizes
	// to check both code paths.
	env := mapEnv(map[string]string{
		"X_DURATIONS_0": "30s",
		"X_DURATIONS_1": "30m",
		"X_DURATIONS_2": "4h",
		"X_SIZES":       "128,1k,1m",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)

	dur30s := itoml.Duration(30 * time.Second)
	dur30m := itoml.Duration(30 * time.Minute)
	dur4h := itoml.Duration(4 * time.Hour)
	size128 := itoml.SizeV1(128)
	size1k := itoml.SizeV1(1024)
	size1m := itoml.SizeV1(1024 * 1024)
	require.EqualValues(t, &config{
		Durations: []*itoml.Duration{&dur30s, &dur30m, &dur4h},
		Sizes:     []*itoml.SizeV1{&size128, &size1k, &size1m},
	}, &c)

	require.ElementsMatch(t, []string{
		"X_DURATIONS_0", "X_DURATIONS_1", "X_DURATIONS_2",
		"X_SIZES",
	}, appliedVars)
}

func TestEnvOverride_AllocatesPointerWhenGrowingStructSlice(t *testing.T) {
	// Entries should be allocated when growing a slice of pointers to structs.
	type config struct {
		Subs []*defaulterSub `toml:"sub"`
	}

	// Structs only support indexed env var overrides.
	// to check both code paths.
	env := mapEnv(map[string]string{
		"X_SUB_0_A": "my a",
		"X_SUB_1_B": "my b",
		"X_SUB_2_C": "my c",
		"X_SUB_C":   "new default c",
	})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)

	require.Equal(t, &config{
		Subs: []*defaulterSub{
			&defaulterSub{A: "my a", B: "default-b", C: "new default c"},
			&defaulterSub{A: "default-a", B: "my b", C: "new default c"},
			&defaulterSub{A: "default-a", B: "default-b", C: "my c"},
		},
	}, &c)
	require.ElementsMatch(t, []string{
		"X_SUB_0_A", "X_SUB_1_B", "X_SUB_2_C",
		"X_SUB_C",
	}, appliedVars)
}

func TestEnvOverride_NilPointerToStructSkipped(t *testing.T) {
	// A nil pointer to a struct is NOT auto-allocated. Struct env vars target
	// the struct's fields, not the struct itself, so there's no single value
	// to trigger allocation. Users must initialize such pointers in NewConfig
	// or via the TOML file.
	type sub struct {
		A string `toml:"a"`
	}
	type config struct {
		Sub *sub `toml:"sub"`
	}

	env := func(s string) string {
		if s == "X_SUB_A" {
			return "value"
		}
		return ""
	}

	var c config
	_, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Nil(t, c.Sub)
}

func TestEnvOverride_FalseGrowFromDefault(t *testing.T) {
	// Verify correct when result when there is a false slice growth
	// from a default enviroment variable, but not one from an
	// indexed environment variable.
	type config struct {
		Sub []defaulterSub `toml:"sub"`
	}

	env := mapEnv(map[string]string{"X_SUB_B": "default b"})

	var c config
	appliedVars, err := itoml.ApplyEnvOverrides(env, "X", &c)
	require.NoError(t, err)
	require.Equal(t, config{}, c)
	require.Empty(t, appliedVars)
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

type subLeafConfig struct {
	Str   string       `toml:"str"`
	Int   int          `toml:"int"`
	Uint  uint         `toml:"uint"`
	Size  itoml.SizeV1 `toml:"size"`
	Bool  bool         `toml:"bool"`
	Float float64      `toml:"float"`
}

func (sc *subLeafConfig) ApplyDefaults() {
}

func TestEnvOverride_SliceGrowthLimit(t *testing.T) {

	type config struct {
		Ints    []int           `toml:"ints"`
		Strings []string        `toml:"strings"`
		Sub     []subLeafConfig `toml:"sub"`
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
			"env override X_INTS_%d would append more than %d elements",
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
			"env override X_SUB_%d_STR would append more than %d elements",
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
			"env overrides %s would append more than %d elements",
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
			"env overrides %s would append more than %d elements",
			strings.Join(envVars, ","), itoml.MaxEnvSliceGrowth))
		require.Empty(t, appliedVars)
	})

}

// TestPflagValue exercises Duration, Size, and SSize as pflag.Value
// implementations end-to-end: register as flags, parse argv, confirm the
// parsed value. The Set/Type/String methods themselves are trivial delegations
// (UnmarshalText, a constant, marshalSize) already covered by their own
// tests; this is the integration smoke test.
func TestPflagValue(t *testing.T) {
	var d itoml.Duration
	var s itoml.SizeV1
	var ss itoml.SSizeV1
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Var(&d, "timeout", "")
	fs.Var(&s, "cache", "")
	fs.Var(&ss, "buffer", "")
	require.NoError(t, fs.Parse([]string{"--timeout", "2m", "--cache", "512m", "--buffer", "-1m"}))
	require.Equal(t, itoml.Duration(2*time.Minute), d)
	require.Equal(t, itoml.SizeV1(512<<20), s)
	require.Equal(t, itoml.SSizeV1(-1<<20), ss)
	require.Equal(t, "Duration", d.Type())
	require.Equal(t, "Size", s.Type())
	require.Equal(t, "SSize", ss.Type())
}

// TestDuration_SetEmpty pins that Duration.Set rejects the empty string,
// diverging from UnmarshalText's TOML-friendly empty-is-absent behavior.
// The specific error text mirrors what time.ParseDuration("") returns so
// callers see a consistent message regardless of which path produced it.
func TestDuration_SetEmpty(t *testing.T) {
	var d itoml.Duration
	require.EqualError(t, d.Set(""), `time: invalid duration ""`)
	require.Equal(t, itoml.Duration(0), d, "receiver must remain unchanged on error")
}

// --- SizeV2 / SSizeV2 tests ---
//
// SizeV2 uses pure humanize semantics: bare "k"/"m"/"g" are SI decimal
// (1000-based), IEC binary units require explicit "kib"/"mib"/"gib".
// String() renders the humanize.IBytes form ("1.0 KiB" etc.), while TOML
// serialization remains a raw integer because SizeV2 does not implement
// encoding.TextMarshaler. These tests verify the parsing behavior and
// contrast it with the V1 rewrite path.

func TestSizeV2_UnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want itoml.SizeV2
	}{
		{"0", 0},
		{"1", 1},
		{"100", 100},
		// Bare single letters: SI decimal under V2 — the critical distinction
		// from V1. These must NOT be 1024/1048576/etc.
		{"1k", 1000},
		{"1K", 1000},
		{"1m", 1000 * 1000},
		{"1M", 1000 * 1000},
		{"1g", 1000 * 1000 * 1000},
		{"1G", 1000 * 1000 * 1000},
		// Explicit IEC suffixes: binary.
		{"1kib", 1 << 10},
		{"1KiB", 1 << 10},
		{"1 KiB", 1 << 10},
		{"1mib", 1 << 20},
		{"1gib", 1 << 30},
		{"1tib", 1 << 40},
		// Explicit SI suffixes: decimal.
		{"1kb", 1000},
		{"1KB", 1000},
		{"1mb", 1000 * 1000},
		{"1gb", 1000 * 1000 * 1000},
		// Fractional values on explicit suffixes.
		{"1.5kib", 1536},
		{"1.5 MiB", 1024 * 1024 * 3 / 2},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.SizeV2
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	for _, name := range []string{
		"empty", "non_numeric", "unknown_suffix", "negative",
	} {
		input := map[string]string{
			"empty":          "",
			"non_numeric":    "abc",
			"unknown_suffix": "1xyz",
			// humanize has no notion of signed values; "-1" fails to parse.
			"negative": "-1",
		}[name]
		t.Run(name, func(t *testing.T) {
			var s itoml.SizeV2
			require.Error(t, s.UnmarshalText([]byte(input)))
			require.Zero(t, s)
		})
	}
}

// TestSizeV2_Matches2x pins the guarantee that SizeV2 produces the same
// parsed value as 2.x's Size for every input 2.x accepts. The 2.x parser
// is a thin wrapper over humanize.ParseBytes with an explicit empty check;
// SizeV2 has the same delegation (empty is rejected by humanize itself,
// with a different error message but the same "reject" outcome). The cases
// below cover 2.x's full vocabulary — IEC binary, SI decimal, mixed case,
// whitespace, fractions — so any regression in SizeV2's humanize pass-through
// will be caught.
func TestSizeV2_Matches2x(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want itoml.SizeV2
	}{
		{"0", 0},
		{"1", 1},
		// Bare letters (SI decimal per humanize).
		{"1k", 1000},
		{"1K", 1000},
		{"1m", 1_000_000},
		{"1g", 1_000_000_000},
		// Explicit IEC.
		{"1kib", 1 << 10},
		{"1KiB", 1 << 10},
		{"1mib", 1 << 20},
		{"1gib", 1 << 30},
		{"1tib", 1 << 40},
		// Explicit SI.
		{"1kb", 1000},
		{"1MB", 1_000_000},
		{"1GB", 1_000_000_000},
		// Whitespace and fractional.
		{"1 KiB", 1 << 10},
		{"1.5 KiB", 1536},
		{"1.5kib", 1536},
		{"256KiB", 256 << 10},
		{"10 GiB", 10 << 30},
		{"10 GB", 10_000_000_000},
		{"1 PiB", 1 << 50},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var s itoml.SizeV2
			require.NoError(t, s.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.want, s)
		})
	}
	// Inputs 2.x rejects must also be rejected by SizeV2.
	for _, in := range []string{"", "abc", "+1", "-1", "1xyz"} {
		t.Run("reject_"+in, func(t *testing.T) {
			var s itoml.SizeV2
			require.Error(t, s.UnmarshalText([]byte(in)))
			require.Zero(t, s)
		})
	}
}

// TestSizeV2_TOMLEncode pins the wire format: SizeV2 must encode as a raw
// TOML integer, byte-identical to what a plain uint64 field produces. This
// is the property that keeps v2.8.0-and-earlier config files forward- and
// backward-compatible across versions. See the doc comment next to
// SizeV2.String for why MarshalText is intentionally absent.
func TestSizeV2_TOMLEncode(t *testing.T) {
	type cfg struct {
		V    itoml.SizeV2 `toml:"v"`
		Peer uint64       `toml:"peer"` // encoded alongside to compare byte-for-byte
	}
	// Deliberately include values that humanize.IBytes would mangle.
	for _, v := range []uint64{
		0, 1, 1024, 1 << 20, 1 << 30,
		25_000_000,    // default [http] max-body-size
		26_214_400,    // storage-cache-snapshot-memory-size default
		50_331_648,    // storage-compact-throughput-burst default
		1_073_741_825, // non-power-of-2: would lose precision through humanize
		math.MaxUint64,
	} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, toml.NewEncoder(&buf).Encode(cfg{V: itoml.SizeV2(v), Peer: v}))
			// SizeV2's line must look identical to the plain uint64 line.
			require.Contains(t, buf.String(), fmt.Sprintf("v = %d\n", v))
			require.Contains(t, buf.String(), fmt.Sprintf("peer = %d\n", v))
		})
	}
}

// TestSizeV2_TOMLRoundTrip exercises marshal → unmarshal via the real
// BurntSushi/toml encoder and decoder, on values that historically lost
// precision when MarshalText emitted humanize.IBytes output. Every value
// must come back exact.
//
// Values stay ≤ MaxInt64 because TOML integers are 64-bit signed per the
// spec — BurntSushi/toml's decoder rejects uint64 values above MaxInt64
// as out-of-range. (Its encoder permissively emits them anyway, which is
// why TestSizeV2_TOMLEncode can include MaxUint64.) Real influxd size
// configs never approach 2^63 bytes (~9 EiB).
func TestSizeV2_TOMLRoundTrip(t *testing.T) {
	type cfg struct {
		V itoml.SizeV2 `toml:"v"`
	}
	for _, v := range []uint64{
		0, 1, 1024, 1 << 20, 1 << 30, 1 << 50,
		25_000_000,
		26_214_400,
		50_331_648,
		1_073_741_825,
	} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, toml.NewEncoder(&buf).Encode(cfg{V: itoml.SizeV2(v)}))
			var got cfg
			_, err := toml.Decode(buf.String(), &got)
			require.NoError(t, err)
			require.Equal(t, itoml.SizeV2(v), got.V)
		})
	}
}

// TestSizeV2_ReadsV28Format verifies that config files from v2.8.0 and
// earlier — which wrote Size fields as raw integers — still parse correctly
// with SizeV2 on this branch. Dropping MarshalText preserves this
// backward-compatibility property.
func TestSizeV2_ReadsV28Format(t *testing.T) {
	type cfg struct {
		V itoml.SizeV2 `toml:"v"`
	}
	for _, tc := range []struct {
		line string
		want uint64
	}{
		// Raw integers — the only form v2.8.0 wrote.
		{"v = 0", 0},
		{"v = 1024", 1024},
		{"v = 25000000", 25_000_000},
		{"v = 1073741824", 1 << 30},
		// v2.8.0 users could also hand-write humanize-vocabulary strings;
		// the TOML string type routes to UnmarshalText, which still accepts them.
		{`v = "1 KiB"`, 1 << 10},
		{`v = "25 MiB"`, 25 << 20},
	} {
		t.Run(tc.line, func(t *testing.T) {
			var got cfg
			_, err := toml.Decode(tc.line, &got)
			require.NoError(t, err)
			require.Equal(t, itoml.SizeV2(tc.want), got.V)
		})
	}
}

func TestSSizeV2_UnmarshalText(t *testing.T) {
	for _, tc := range []struct {
		str  string
		want itoml.SSizeV2
	}{
		{"0", 0},
		{"1", 1},
		{"-1", -1},
		// Bare letters: SI decimal for both positive and negative.
		{"1k", 1000},
		{"-1k", -1000},
		{"1M", 1000 * 1000},
		{"-1M", -1000 * 1000},
		{"1g", 1000 * 1000 * 1000},
		// Explicit IEC suffixes: binary.
		{"1kib", 1 << 10},
		{"-1KiB", -(1 << 10)},
		{"1mib", 1 << 20},
		{"1gib", 1 << 30},
		// Explicit SI suffixes.
		{"1kb", 1000},
		{"-1MB", -1000 * 1000},
	} {
		t.Run(tc.str, func(t *testing.T) {
			var s itoml.SSizeV2
			require.NoError(t, s.UnmarshalText([]byte(tc.str)))
			require.Equal(t, tc.want, s)
		})
	}

	for _, name := range []string{
		"empty", "lone_sign", "non_numeric", "unknown_suffix",
	} {
		input := map[string]string{
			"empty":          "",
			"lone_sign":      "-",
			"non_numeric":    "abc",
			"unknown_suffix": "1xyz",
		}[name]
		t.Run(name, func(t *testing.T) {
			var s itoml.SSizeV2
			require.Error(t, s.UnmarshalText([]byte(input)))
			require.Zero(t, s)
		})
	}
}

// TestSSizeV2_Matches2x pins the guarantee that, for every non-negative
// input 2.x's Size accepts whose value fits in int64, SSizeV2 parses it
// to the same value. The 2.x branch has no signed-size type; SSizeV2
// extends 2.x semantics by adding a leading '-' to negate the humanize-
// parsed magnitude. This test verifies the non-negative correspondence
// plus a sanity check on the sign-handling extension.
func TestSSizeV2_Matches2x(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want itoml.SSizeV2
	}{
		{"0", 0},
		{"1", 1},
		// Bare letters (SI decimal).
		{"1k", 1000},
		{"1M", 1_000_000},
		{"1g", 1_000_000_000},
		// Explicit IEC.
		{"1kib", 1 << 10},
		{"1MiB", 1 << 20},
		{"1gib", 1 << 30},
		{"1tib", 1 << 40},
		// Explicit SI.
		{"1kb", 1000},
		{"1MB", 1_000_000},
		// Whitespace and fractional.
		{"1.5 KiB", 1536},
		{"256KiB", 256 << 10},
		{"10 GiB", 10 << 30},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var s itoml.SSizeV2
			require.NoError(t, s.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.want, s)
		})
	}
	// Sign handling — SSizeV2's extension over 2.x Size.
	for _, tc := range []struct {
		in   string
		want itoml.SSizeV2
	}{
		{"-1", -1},
		{"-1k", -1000},
		{"-1kib", -(1 << 10)},
		{"-1 GiB", -(1 << 30)},
		{"-1.5kib", -1536},
	} {
		t.Run(tc.in, func(t *testing.T) {
			var s itoml.SSizeV2
			require.NoError(t, s.UnmarshalText([]byte(tc.in)))
			require.Equal(t, tc.want, s)
		})
	}
	// Values 2.x Size accepts that exceed MaxInt64 must be rejected by
	// SSizeV2 (signed range). 10 EiB = 10 << 60 overflows int64.
	for _, in := range []string{"10 EiB", "16 EiB"} {
		t.Run("overflow_"+in, func(t *testing.T) {
			var s itoml.SSizeV2
			require.Error(t, s.UnmarshalText([]byte(in)))
			require.Zero(t, s)
		})
	}
}

// TestSSizeV2_TOMLEncode is the SSizeV2 counterpart to TestSizeV2_TOMLEncode.
// Same rationale — see the comment next to SSizeV2.String.
func TestSSizeV2_TOMLEncode(t *testing.T) {
	type cfg struct {
		V    itoml.SSizeV2 `toml:"v"`
		Peer int64         `toml:"peer"`
	}
	for _, v := range []int64{
		0, 1, -1, 1024, -1024, 1 << 30, -(1 << 30),
		25_000_000, -25_000_000,
		1_073_741_825, -1_073_741_825,
		math.MaxInt64, math.MinInt64,
	} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, toml.NewEncoder(&buf).Encode(cfg{V: itoml.SSizeV2(v), Peer: v}))
			require.Contains(t, buf.String(), fmt.Sprintf("v = %d\n", v))
			require.Contains(t, buf.String(), fmt.Sprintf("peer = %d\n", v))
		})
	}
}

// TestSSizeV2_TOMLRoundTrip — marshal → unmarshal through the real toml
// encoder and decoder, checking every value survives exactly.
//
// Values stay within float64's exact-integer range (≤ 2^53) plus the one
// special case MinInt64 (which parseSigned recognises explicitly). Humanize
// uses strconv.ParseFloat internally, so odd values ≥ 2^53 round up to the
// next representable float and fail the parseSigned range check.
func TestSSizeV2_TOMLRoundTrip(t *testing.T) {
	type cfg struct {
		V itoml.SSizeV2 `toml:"v"`
	}
	for _, v := range []int64{
		0, 1, -1, 1024, -1024, 1 << 30, -(1 << 30), 1 << 50, -(1 << 50),
		25_000_000, -25_000_000,
		1_073_741_825, -1_073_741_825,
		math.MinInt64,
	} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, toml.NewEncoder(&buf).Encode(cfg{V: itoml.SSizeV2(v)}))
			var got cfg
			_, err := toml.Decode(buf.String(), &got)
			require.NoError(t, err)
			require.Equal(t, itoml.SSizeV2(v), got.V)
		})
	}
}

// TestSSizeV2_StringNegative covers the negative branch of SSizeV2.String
// and the MinInt64 special case in negAsUint64 (which avoids int64 overflow
// when negating the absolute value).
func TestSSizeV2_StringNegative(t *testing.T) {
	require.Equal(t, "-1.0 KiB", itoml.SSizeV2(-(1 << 10)).String())
	require.Equal(t, "-8.0 EiB", itoml.SSizeV2(math.MinInt64).String())
}

// TestSizeErrorsIncludeInput pins the invariant that every error returned
// by a size-type UnmarshalText includes the original input text in its
// wrapper. Guards the wrap in parseBytesUnsigned and parseBytesSigned —
// without it, humanize errors like `strconv.ParseFloat: parsing "":
// invalid syntax` would reach the caller with no reference to the input.
func TestSizeErrorsIncludeInput(t *testing.T) {
	unmarshalers := []struct {
		name string
		um   func([]byte) error
	}{
		{"SizeV1", func(b []byte) error { var s itoml.SizeV1; return s.UnmarshalText(b) }},
		{"SizeV2", func(b []byte) error { var s itoml.SizeV2; return s.UnmarshalText(b) }},
		{"SSizeV1", func(b []byte) error { var s itoml.SSizeV1; return s.UnmarshalText(b) }},
		{"SSizeV2", func(b []byte) error { var s itoml.SSizeV2; return s.UnmarshalText(b) }},
	}

	// Inputs that take the humanize path and fail there. All four types
	// wrap with `invalid size %q:` and keep the inner error reachable.
	for _, in := range []string{
		"not_a_size",               // humanize digit-scan failure
		"1.5xyz",                   // humanize unknown suffix
		"99999999999999999999 EiB", // humanize "too large"
	} {
		want := fmt.Sprintf("invalid size %q", in)
		for _, u := range unmarshalers {
			t.Run(u.name+"/"+in, func(t *testing.T) {
				err := u.um([]byte(in))
				require.ErrorContains(t, err, want)
				require.NotNil(t, errors.Unwrap(err),
					"inner humanize/strconv error must remain reachable via errors.Unwrap")
			})
		}
	}

	// Signed-only: a value humanize parses successfully but that exceeds
	// int64 range goes through parseBytesSigned's own error branch, which
	// uses a different wrapper shape ("size %q exceeds signed int64 range")
	// and does not wrap an inner error.
	signed := []struct {
		name string
		um   func([]byte) error
	}{
		{"SSizeV1", func(b []byte) error { var s itoml.SSizeV1; return s.UnmarshalText(b) }},
		{"SSizeV2", func(b []byte) error { var s itoml.SSizeV2; return s.UnmarshalText(b) }},
	}
	for _, in := range []string{"10 EiB", "-10 EiB"} {
		want := fmt.Sprintf("size %q exceeds signed int64 range", in)
		for _, u := range signed {
			t.Run(u.name+"/overflow/"+in, func(t *testing.T) {
				require.ErrorContains(t, u.um([]byte(in)), want)
			})
		}
	}
}

// TestApplyEnvOverrides_V2 pins that ApplyEnvOverrides works with SizeV2 /
// SSizeV2 fields and routes values through humanize semantics (SI decimal
// for bare k/m/g, as opposed to V1's binary interpretation). The env-override
// framework reaches these types via their TextUnmarshaler interface, so this
// mostly exercises the TextUnmarshaler branch of applyEnvOverrides coupled
// with the V2-specific parse semantics.
func TestApplyEnvOverrides_V2(t *testing.T) {
	type config struct {
		Size  itoml.SizeV2  `toml:"size"`
		SSize itoml.SSizeV2 `toml:"ssize"`
	}
	t.Run("bare letter uses SI decimal", func(t *testing.T) {
		env := mapEnv(map[string]string{
			"X_SIZE":  "1k",  // V2 semantics: 1000, not 1024
			"X_SSIZE": "-1m", // V2 semantics: -1_000_000
		})
		var c config
		applied, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.NoError(t, err)
		require.Equal(t, itoml.SizeV2(1000), c.Size)
		require.Equal(t, itoml.SSizeV2(-1_000_000), c.SSize)
		require.ElementsMatch(t, []string{"X_SIZE", "X_SSIZE"}, applied)
	})
	t.Run("explicit IEC suffix", func(t *testing.T) {
		env := mapEnv(map[string]string{"X_SIZE": "1 GiB"})
		var c config
		_, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.NoError(t, err)
		require.Equal(t, itoml.SizeV2(1<<30), c.Size)
	})
	t.Run("invalid value surfaces error", func(t *testing.T) {
		env := mapEnv(map[string]string{"X_SIZE": "nope"})
		var c config
		_, err := itoml.ApplyEnvOverrides(env, "X", &c)
		require.ErrorContains(t, err, "X_SIZE")
	})
}

// TestPflagValueV2 is the V2 counterpart to TestPflagValue. It exercises
// SizeV2 / SSizeV2 through a pflag.FlagSet end-to-end, pinning the
// pflag.Value contract (Set, Type) on the V2 implementations. Explicit IEC
// suffixes avoid any ambiguity over V1's binary vs V2's SI semantics for
// bare-letter forms.
func TestPflagValueV2(t *testing.T) {
	var s itoml.SizeV2
	var ss itoml.SSizeV2
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.Var(&s, "cache", "")
	fs.Var(&ss, "buffer", "")
	require.NoError(t, fs.Parse([]string{"--cache", "1KiB", "--buffer", "-1KiB"}))
	require.Equal(t, itoml.SizeV2(1<<10), s)
	require.Equal(t, itoml.SSizeV2(-(1 << 10)), ss)
	require.Equal(t, "Size", s.Type())
	require.Equal(t, "SSize", ss.Type())
}
