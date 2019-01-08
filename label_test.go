package influxdb_test

import (
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
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
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				Name:       "iot",
			},
		},
		{
			name: "label requires a resourceid",
			fields: fields{
				Name: "iot",
			},
			wantErr: true,
		},
		{
			name: "label requires a name",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.Label{
				ResourceID: tt.fields.ResourceID,
				Name:       tt.fields.Name,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Label.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
