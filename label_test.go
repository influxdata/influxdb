package influxdb_test

import (
	"github.com/influxdata/influxdb/v2/kit/platform"
	"testing"

	"github.com/influxdata/influxdb/v2"
	influxtest "github.com/influxdata/influxdb/v2/testing"
)

const (
	orgOneID = "020f755c3c083000"
)

func TestLabelValidate(t *testing.T) {
	type fields struct {
		Name  string
		OrgID platform.ID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid label",
			fields: fields{
				Name:  "iot",
				OrgID: influxtest.MustIDBase16(orgOneID),
			},
		},
		{
			name: "label requires a name",
			fields: fields{
				OrgID: influxtest.MustIDBase16(orgOneID),
			},
			wantErr: true,
		},
		{
			name: "label requires an organization ID",
			fields: fields{
				Name: "iot",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := influxdb.Label{
				Name:  tt.fields.Name,
				OrgID: tt.fields.OrgID,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Label.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
