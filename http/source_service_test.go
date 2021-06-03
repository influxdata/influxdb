package http

import (
	"reflect"
	"testing"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	platform "github.com/influxdata/influxdb/v2"
)

func Test_newSourceResponse(t *testing.T) {
	tests := []struct {
		name string
		s    *platform.Source
		want *sourceResponse
	}{
		{
			name: "self source returns links to this instance",
			s: &platform.Source{
				ID:             platform2.ID(1),
				OrganizationID: platform2.ID(1),
				Name:           "Hi",
				Type:           platform.SelfSourceType,
				URL:            "/",
			},
			want: &sourceResponse{
				Source: &platform.Source{
					ID:             platform2.ID(1),
					OrganizationID: platform2.ID(1),
					Name:           "Hi",
					Type:           platform.SelfSourceType,
					URL:            "/",
				},
				Links: map[string]interface{}{
					"self":    "/api/v2/sources/0000000000000001",
					"query":   "/api/v2/sources/0000000000000001/query",
					"buckets": "/api/v2/sources/0000000000000001/buckets",
					"health":  "/api/v2/sources/0000000000000001/health",
				},
			},
		},
		{
			name: "v1 sources have proxied links",
			s: &platform.Source{
				ID:             platform2.ID(1),
				OrganizationID: platform2.ID(1),
				Name:           "Hi",
				Type:           platform.V1SourceType,
				URL:            "/",
			},
			want: &sourceResponse{
				Source: &platform.Source{
					ID:             platform2.ID(1),
					OrganizationID: platform2.ID(1),
					Name:           "Hi",
					Type:           platform.V1SourceType,
					URL:            "/",
				},
				Links: map[string]interface{}{
					"self":    "/api/v2/sources/0000000000000001",
					"query":   "/api/v2/sources/0000000000000001/query",
					"buckets": "/api/v2/sources/0000000000000001/buckets",
					"health":  "/api/v2/sources/0000000000000001/health",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSourceResponse(tt.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newSourceResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}
