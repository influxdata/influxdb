package http

import (
	"fmt"
	"testing"
	"time"
)

// Ensure a time duration can be parsed.
func TestParseDuration(t *testing.T) {
	var tests = []struct {
		s       string
		want    time.Duration
		wantErr bool
	}{
		{s: `10n`, want: 10 * time.Nanosecond},
		{s: `10ns`, want: 10 * time.Nanosecond},
		{s: `10u`, want: 10 * time.Microsecond},
		{s: `10µ`, want: 10 * time.Microsecond},
		{s: `10us`, want: 10 * time.Microsecond},
		{s: `10µs`, want: 10 * time.Microsecond},
		{s: `15ms`, want: 15 * time.Millisecond},
		{s: `100s`, want: 100 * time.Second},
		{s: `2m`, want: 2 * time.Minute},
		{s: `2mo`, want: 2 * 30 * 24 * time.Hour},
		{s: `2h`, want: 2 * time.Hour},
		{s: `2d`, want: 2 * 24 * time.Hour},
		{s: `2w`, want: 2 * 7 * 24 * time.Hour},
		{s: `2y`, want: 2 * 365 * 24 * time.Hour},
		{s: `1h30m`, want: time.Hour + 30*time.Minute},
		{s: `30ms3000u`, want: 30*time.Millisecond + 3000*time.Microsecond},
		{s: `-5s`, want: -5 * time.Second},
		{s: `-5m30s`, want: -5*time.Minute - 30*time.Second},
		{s: ``, wantErr: true},
		{s: `3`, wantErr: true},
		{s: `3mm`, wantErr: true},
		{s: `3nm`, wantErr: true},
		{s: `1000`, wantErr: true},
		{s: `w`, wantErr: true},
		{s: `ms`, wantErr: true},
		{s: `1.2w`, wantErr: true},
		{s: `10x`, wantErr: true},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			got, err := ParseDuration(tt.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDuration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got != tt.want {
				t.Errorf("ParseDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Ensure a time duration can be formatted.
func TestFormatDuration(t *testing.T) {
	var tests = []struct {
		d    time.Duration
		want string
	}{
		{d: 3 * time.Nanosecond, want: `3ns`},
		{d: 3 * time.Microsecond, want: `3us`},
		{d: 1001 * time.Microsecond, want: `1001us`},
		{d: 15 * time.Millisecond, want: `15ms`},
		{d: 100 * time.Second, want: `100s`},
		{d: 2 * time.Minute, want: `2m`},
		{d: 2 * time.Hour, want: `2h`},
		{d: 2 * 24 * time.Hour, want: `2d`},
		{d: 2 * 7 * 24 * time.Hour, want: `2w`},
		{d: 2 * 30 * 24 * time.Hour, want: `2mo`},
		{d: 2 * 365 * 24 * time.Hour, want: `2y`},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			got := FormatDuration(tt.d)
			if got != tt.want {
				t.Errorf("FormatDuration() = %s, want %s", got, tt.want)
			}
		})
	}
}
