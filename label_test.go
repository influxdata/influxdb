package influxdb_test

import (
	"testing"

	platform "github.com/influxdata/influxdb"
)

func TestLabelValidate(t *testing.T) {
	type fields struct {
		ResourceID platform.ID
		Name       string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid label",
			fields: fields{
				Name: "iot",
			},
		},
		{
			name:    "label requires a name",
			fields:  fields{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.Label{
				Name: tt.fields.Name,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Label.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
