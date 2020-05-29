package http

import (
	nethttp "net/http"
	"testing"
)

func Test_userAgent(t *testing.T) {
	tests := []struct {
		name string
		ua   string
		want string
	}{
		{
			name: "linux chrome",
			ua:   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36",
			want: "Chrome",
		},
		{
			name: "telegraf",
			ua:   "Telegraf/1.12.6",
			want: "Telegraf",
		},
		{
			name: "curl",
			ua:   "curl/7.67.0",
			want: "curl",
		},
		{
			name: "go",
			ua:   "Go-http-client/1.1",
			want: "Go-http-client",
		},
		{
			name: "influx client",
			ua:   "InfluxDBClient/0.0.1  (golang; windows; amd64)",
			want: "InfluxDBClient",
		},
		{
			name: "iphone",
			ua:   "Mozilla/5.0 (iPhone; CPU iPhone OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Mobile/15E148 Safari/604.1",
			want: "Safari",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &nethttp.Request{
				Header: nethttp.Header{
					"User-Agent": []string{tt.ua},
				},
			}

			got := userAgent(r)
			if got != tt.want {
				t.Fatalf("userAgent %v want %v", got, tt.want)
			}
		})
	}
}
