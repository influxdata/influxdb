package platform

import (
	"testing"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID   ID
		ResourceType ResourceType
		UserID       ID
		UserType     UserType
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "mapping requires a resourceid",
			fields: fields{
				UserID:       []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserType:     Owner,
				ResourceType: DashboardResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a userid",
			fields: fields{
				ResourceID:   []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserType:     Owner,
				ResourceType: DashboardResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a usertype",
			fields: fields{
				ResourceID:   []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserID:       []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				ResourceType: DashboardResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a resourcetype",
			fields: fields{
				ResourceID: []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserID:     []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserType:   Owner,
			},
			wantErr: true,
		},
		{
			name: "the usertype provided must be valid",
			fields: fields{
				ResourceID:   []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserID:       []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserType:     "foo",
				ResourceType: DashboardResourceType,
			},
			wantErr: true,
		},
		{
			name: "the resourcetype provided must be valid",
			fields: fields{
				ResourceID:   []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserID:       []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				UserType:     Owner,
				ResourceType: "foo",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := UserResourceMapping{
				ResourceID: tt.fields.ResourceID,
				UserID:     tt.fields.UserID,
				UserType:   tt.fields.UserType,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
