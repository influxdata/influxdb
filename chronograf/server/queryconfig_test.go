package server

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestValidateQueryConfig(t *testing.T) {
	tests := []struct {
		name    string
		q       *chronograf.QueryConfig
		wantErr bool
	}{
		{
			name: "invalid field type",
			q: &chronograf.QueryConfig{
				Fields: []chronograf.Field{
					{
						Type: "invalid",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid field args",
			q: &chronograf.QueryConfig{
				Fields: []chronograf.Field{
					{
						Type: "func",
						Args: []chronograf.Field{
							{
								Type: "invalid",
							},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateQueryConfig(tt.q); (err != nil) != tt.wantErr {
				t.Errorf("ValidateQueryConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
