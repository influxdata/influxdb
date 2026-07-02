package httpd_test

import (
	"net/http"
	"testing"

	"github.com/influxdata/influxdb/services/httpd"
	"github.com/stretchr/testify/require"
)

// TestPrintableRemoteAddr verifies the host string derived from a request's
// RemoteAddr and any X-Forwarded-For headers. This is the logic that was
// extracted out of buildLogLine so it can be reused by the query Host field.
func TestPrintableRemoteAddr(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		forwarded  []string // one entry per X-Forwarded-For header line
		want       string
	}{
		{
			name:       "host and port strips the port",
			remoteAddr: "10.1.2.3:4567",
			want:       "10.1.2.3",
		},
		{
			name:       "ipv6 host and port strips the port",
			remoteAddr: "[::1]:8086",
			want:       "::1",
		},
		{
			name:       "unparseable remote addr is returned verbatim",
			remoteAddr: "not-an-addr",
			want:       "not-an-addr",
		},
		{
			name:       "single forwarded-for is prepended",
			remoteAddr: "10.1.2.3:4567",
			forwarded:  []string{"192.168.0.1"},
			want:       "192.168.0.1,10.1.2.3",
		},
		{
			name:       "multiple forwarded-for values keep order then remote host",
			remoteAddr: "10.1.2.3:4567",
			forwarded:  []string{"192.168.0.1", "203.0.113.7"},
			want:       "192.168.0.1,203.0.113.7,10.1.2.3",
		},
		{
			name:       "forwarded-for with unparseable remote addr",
			remoteAddr: "unixsocket",
			forwarded:  []string{"192.168.0.1"},
			want:       "192.168.0.1,unixsocket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := http.NewRequest("GET", "/query", nil)
			require.NoError(t, err)
			r.RemoteAddr = tt.remoteAddr
			for _, f := range tt.forwarded {
				r.Header.Add("X-Forwarded-For", f)
			}
			require.Equal(t, tt.want, httpd.PrintableRemoteAddr(r))
		})
	}
}
