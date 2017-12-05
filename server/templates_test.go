package server

import (
	"testing"

	"github.com/influxdata/chronograf"
)

func TestValidTemplateRequest(t *testing.T) {
	tests := []struct {
		name     string
		template *chronograf.Template
		wantErr  bool
	}{
		{
			name: "Valid Template",
			template: &chronograf.Template{
				Type: "fieldKeys",
				TemplateVar: chronograf.TemplateVar{
					Values: []chronograf.TemplateValue{
						{
							Type: "fieldKey",
						},
					},
				},
			},
		},
		{
			name:    "Invalid Template Type",
			wantErr: true,
			template: &chronograf.Template{
				Type: "Unknown Type",
				TemplateVar: chronograf.TemplateVar{
					Values: []chronograf.TemplateValue{
						{
							Type: "fieldKey",
						},
					},
				},
			},
		},
		{
			name:    "Invalid Template Variable Type",
			wantErr: true,
			template: &chronograf.Template{
				Type: "csv",
				TemplateVar: chronograf.TemplateVar{
					Values: []chronograf.TemplateValue{
						{
							Type: "unknown value",
						},
					},
				},
			},
		},
		{
			name:    "No query set",
			wantErr: true,
			template: &chronograf.Template{
				Type: "query",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidTemplateRequest(tt.template); (err != nil) != tt.wantErr {
				t.Errorf("ValidTemplateRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
