package meta_test

import (
	"testing"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/assert"
)

func TestValidName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		want   string
		wantOK bool
	}{
		{name: "simple", input: "foo", want: "foo", wantOK: true},
		{name: "trims surrounding spaces", input: "  foo  ", want: "foo", wantOK: true},
		{name: "trims tabs and newlines", input: "\t foo \n", want: "foo", wantOK: true},
		{name: "inner space preserved", input: " foo bar ", want: "foo bar", wantOK: true},
		{name: "unicode is valid", input: "  méäsûré  ", want: "méäsûré", wantOK: true},
		{name: "empty is invalid", input: "", want: "", wantOK: false},
		{name: "all whitespace trims to empty and is invalid", input: "   ", want: "", wantOK: false},
		{name: "dot is invalid", input: ".", want: ".", wantOK: false},
		{name: "dot dot is invalid", input: "..", want: "..", wantOK: false},
		{name: "trims to dot and is invalid", input: "  .  ", want: ".", wantOK: false},
		{name: "forward slash is invalid", input: "foo/bar", want: "foo/bar", wantOK: false},
		{name: "back slash is invalid", input: `foo\bar`, want: `foo\bar`, wantOK: false},
		{name: "non-printable is invalid", input: "foo\x00bar", want: "foo\x00bar", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := meta.ValidName(tt.input)
			assert.Equal(t, tt.wantOK, ok, "validity mismatch for %q", tt.input)
			assert.Equal(t, tt.want, got, "trimmed name mismatch for %q", tt.input)
		})
	}
}
