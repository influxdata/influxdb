package http

import (
	"path"
	"testing"

	"github.com/influxdata/influxdb"
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
			path:     path.Join("/api/v2/organizations", influxdb.ID(2).String()),
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
			path:     path.Join("/api/v2/organizations", influxdb.ID(2).String(), "users", influxdb.ID(3).String()),
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
