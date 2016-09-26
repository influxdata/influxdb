package models_test

import (
	"strconv"
	"testing"
	"testing/quick"

	"github.com/influxdata/influxdb/models"
)

func TestParseIntBytesEquivalenceFuzz(t *testing.T) {
	f := func(b []byte, base int, bitSize int) bool {
		wantI, wantErr := strconv.ParseInt(string(b), base, bitSize)
		gotI, gotErr := models.ParseIntBytes(b, base, bitSize)

		pred := wantI == gotI

		// error objects are heap allocated so naive equality checking
		// won't work here. naive pointer dereferencing will panic
		// in the case of a nil error.
		if wantErr != nil && gotErr == nil {
			pred = false
		} else if wantErr == nil && gotErr != nil {
			pred = false
		} else if wantErr != nil && gotErr != nil {
			if wantErr.Error() != gotErr.Error() {
				pred = false
			}
		}

		return pred
	}
	cfg := &quick.Config{
		MaxCount: 10000,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestParseIntBytesValid64bitBase10EquivalenceFuzz(t *testing.T) {
	buf := []byte{}
	f := func(n int64) bool {
		buf = strconv.AppendInt(buf[:0], n, 10)

		wantI, wantErr := strconv.ParseInt(string(buf), 10, 64)
		gotI, gotErr := models.ParseIntBytes(buf, 10, 64)

		pred := wantI == gotI

		// error objects are heap allocated so naive equality checking
		// won't work here. naive pointer dereferencing will panic
		// in the case of a nil error.
		if wantErr != nil && gotErr == nil {
			pred = false
		} else if wantErr == nil && gotErr != nil {
			pred = false
		} else if wantErr != nil && gotErr != nil {
			if wantErr.Error() != gotErr.Error() {
				pred = false
			}
		}

		return pred
	}
	cfg := &quick.Config{
		MaxCount: 10000,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestParseFloatBytesEquivalenceFuzz(t *testing.T) {
	f := func(b []byte, bitSize int) bool {
		wantI, wantErr := strconv.ParseFloat(string(b), bitSize)
		gotI, gotErr := models.ParseFloatBytes(b, bitSize)

		pred := wantI == gotI

		// error objects are heap allocated so naive equality checking
		// won't work here. naive pointer dereferencing will panic
		// in the case of a nil error.
		if wantErr != nil && gotErr == nil {
			pred = false
		} else if wantErr == nil && gotErr != nil {
			pred = false
		} else if wantErr != nil && gotErr != nil {
			if wantErr.Error() != gotErr.Error() {
				pred = false
			}
		}

		return pred
	}
	cfg := &quick.Config{
		MaxCount: 10000,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestParseFloatBytesValid64bitEquivalenceFuzz(t *testing.T) {
	buf := []byte{}
	f := func(n float64) bool {
		buf = strconv.AppendFloat(buf[:0], n, 'f', -1, 64)

		wantI, wantErr := strconv.ParseFloat(string(buf), 64)
		gotI, gotErr := models.ParseFloatBytes(buf, 64)

		pred := wantI == gotI

		// error objects are heap allocated so naive equality checking
		// won't work here. naive pointer dereferencing will panic
		// in the case of a nil error.
		if wantErr != nil && gotErr == nil {
			pred = false
		} else if wantErr == nil && gotErr != nil {
			pred = false
		} else if wantErr != nil && gotErr != nil {
			if wantErr.Error() != gotErr.Error() {
				pred = false
			}
		}

		return pred
	}
	cfg := &quick.Config{
		MaxCount: 10000,
	}
	if err := quick.Check(f, cfg); err != nil {
		t.Fatal(err)
	}
}
