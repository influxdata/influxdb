package influxql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodingFormatFromMimeType(t *testing.T) {
	tests := []struct {
		s   string
		exp EncodingFormat
	}{
		{s: "application/csv", exp: EncodingFormatAppCSV},
		{s: "text/csv", exp: EncodingFormatTextCSV},
		{s: "application/x-msgpack", exp: EncodingFormatMessagePack},
		{s: "application/json", exp: EncodingFormatJSON},
		{s: "*/*", exp: EncodingFormatJSON},
		{s: "", exp: EncodingFormatJSON},
		{s: "application/other", exp: EncodingFormatJSON},
	}
	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			got := EncodingFormatFromMimeType(tt.s)
			assert.Equal(t, tt.exp, got)
		})
	}
}
