package http

import (
	"net/http"
	"path"
	"testing"

	"github.com/influxdata/influxdb/kit/platform"
	"github.com/influxdata/influxdb/pkg/testttp"
	"github.com/stretchr/testify/assert"
)

func Test_normalizePath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "1",
			path:     path.Join("/api/v2/organizations", platform.ID(2).String()),
			expected: "/api/v2/organizations/:id",
		},
		{
			name:     "2",
			path:     "/api/v2/organizations",
			expected: "/api/v2/organizations",
		},
		{
			name:     "3",
			path:     "/",
			expected: "/",
		},
		{
			name:     "4",
			path:     path.Join("/api/v2/organizations", platform.ID(2).String(), "users", platform.ID(3).String()),
			expected: "/api/v2/organizations/:id/users/:id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := normalizePath(tt.path)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestCors(t *testing.T) {
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("nextHandler"))
	})

	tests := []struct {
		name            string
		method          string
		headers         []string
		expectedStatus  int
		expectedHeaders map[string]string
	}{
		{
			name:           "OPTIONS without Origin",
			method:         "OPTIONS",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "OPTIONS with Origin",
			method:         "OPTIONS",
			headers:        []string{"Origin", "http://myapp.com"},
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "GET with Origin",
			method:         "GET",
			headers:        []string{"Origin", "http://anotherapp.com"},
			expectedStatus: http.StatusOK,
			expectedHeaders: map[string]string{
				"Access-Control-Allow-Origin": "http://anotherapp.com",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := SkipOptions(SetCORS(nextHandler))

			testttp.
				HTTP(t, tt.method, "/", nil).
				Headers("", "", tt.headers...).
				Do(svr).
				ExpectStatus(tt.expectedStatus).
				ExpectHeaders(tt.expectedHeaders)
		})
	}
}
