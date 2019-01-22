package internal_test

import (
	"errors"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx/internal"
)

func TestErrorFmt(t *testing.T) {
	tests := []struct {
		name   string
		err    string
		fmterr string
	}{
		{
			name:   "error already formatted",
			err:    "Invalid ID.",
			fmterr: "Invalid ID.",
		},
		{
			name:   "error missing period",
			err:    "Invalid ID",
			fmterr: "Invalid ID.",
		},
		{
			name:   "error does not start with a capital letter",
			err:    "invalid ID.",
			fmterr: "Invalid ID.",
		},
		{
			name:   "error does not start with a capital letter or end with period",
			err:    "invalid ID",
			fmterr: "Invalid ID.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmterr := internal.ErrorFmt(errors.New(tt.err))

			if got, want := fmterr.Error(), tt.fmterr; got != want {
				t.Errorf("error strings do not match. got/want\n%s\n%s\n", got, want)
			}
		})
	}
}
