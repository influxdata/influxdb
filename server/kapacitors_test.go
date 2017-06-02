package server

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestValidRuleRequest(t *testing.T) {
	tests := []struct {
		name    string
		rule    chronograf.AlertRule
		wantErr bool
	}{
		{
			name: "No every with functions",
			rule: chronograf.AlertRule{
				Query: &chronograf.QueryConfig{
					Fields: []chronograf.Field{
						{
							Field: "oldmanpeabody",
							Funcs: []string{"max"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "With every",
			rule: chronograf.AlertRule{
				Every: "10s",
				Query: &chronograf.QueryConfig{
					Fields: []chronograf.Field{
						{
							Field: "oldmanpeabody",
							Funcs: []string{"max"},
						},
					},
				},
			},
		},
		{
			name:    "No query config",
			rule:    chronograf.AlertRule{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidRuleRequest(tt.rule); (err != nil) != tt.wantErr {
				t.Errorf("ValidRuleRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
