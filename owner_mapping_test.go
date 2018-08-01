package platform

import (
  "testing"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID ID
		Owner      Owner
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
    {
  		name: "mapping requires a resourceid",
  		fields: fields{
  			Owner: Owner{
  				ID: []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
  			},
      },
      wantErr: true,
		},
    {
      name: "mapping requires an Owner",
      fields: fields{
        ResourceID: []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
      },
      wantErr: true,
    },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := OwnerMapping{
				ResourceID: tt.fields.ResourceID,
				Owner:      tt.fields.Owner,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
