package server

import (
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
)

func Test_newSourceResponse(t *testing.T) {
	tests := []struct {
		name string
		src  chronograf.Source
		want sourceResponse
	}{
		{
			name: "Test empty telegraf",
			src: chronograf.Source{
				ID:       1,
				Telegraf: "",
			},
			want: sourceResponse{
				Source: chronograf.Source{
					ID:       1,
					Telegraf: "telegraf",
				},
				Links: sourceLinks{
					Self:        "/chronograf/v1/sources/1",
					Proxy:       "/chronograf/v1/sources/1/proxy",
					Kapacitors:  "/chronograf/v1/sources/1/kapacitors",
					Users:       "/chronograf/v1/sources/1/users",
					Permissions: "/chronograf/v1/sources/1/permissions",
					Databases:   "/chronograf/v1/sources/1/dbs",
				},
			},
		},
		{
			name: "Test non-default telegraf",
			src: chronograf.Source{
				ID:       1,
				Telegraf: "howdy",
			},
			want: sourceResponse{
				Source: chronograf.Source{
					ID:       1,
					Telegraf: "howdy",
				},
				Links: sourceLinks{
					Self:        "/chronograf/v1/sources/1",
					Proxy:       "/chronograf/v1/sources/1/proxy",
					Kapacitors:  "/chronograf/v1/sources/1/kapacitors",
					Users:       "/chronograf/v1/sources/1/users",
					Permissions: "/chronograf/v1/sources/1/permissions",
					Databases:   "/chronograf/v1/sources/1/dbs",
				},
			},
		},
	}
	for _, tt := range tests {
		if got := newSourceResponse(tt.src); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. newSourceResponse() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
