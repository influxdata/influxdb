package platform_test

import (
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID platform.ID
		Owner      platform.Owner
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "mapping requires a resourceid",
			fields: fields{
				Owner: platform.Owner{
					ID: platformtesting.MustIDFromString("debac1e0deadbeef"),
				},
			},
			wantErr: true,
		},
		{
			name: "mapping requires an Owner",
			fields: fields{
				ResourceID: platformtesting.MustIDFromString("debac1e0deadbeef"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.OwnerMapping{
				ResourceID: tt.fields.ResourceID,
				Owner:      tt.fields.Owner,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
