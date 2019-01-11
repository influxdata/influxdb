package influxdb_test

import (
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID platform.ID
		Resource   platform.Resource
		UserID     platform.ID
		UserType   platform.UserType
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid mapping",
			fields: fields{
				UserID:     platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:   platform.Owner,
				Resource:   platform.DashboardsResource,
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
			},
		},
		{
			name: "mapping requires a resourceid",
			fields: fields{
				UserID:   platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType: platform.Owner,
				Resource: platform.DashboardsResource,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a userid",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				UserType:   platform.Owner,
				Resource:   platform.DashboardsResource,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a usertype",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:     platformtesting.MustIDBase16("debac1e0deadbeef"),
				Resource:   platform.DashboardsResource,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a resourcetype",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:     platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:   platform.Owner,
			},
			wantErr: true,
		},
		{
			name: "the usertype provided must be valid",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:     platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:   "foo",
				Resource:   platform.DashboardsResource,
			},
			wantErr: true,
		},
		{
			name: "the resourcetype provided must be valid",
			fields: fields{
				ResourceID: platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:     platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:   platform.Owner,
				Resource:   "foo",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.UserResourceMapping{
				ResourceID: tt.fields.ResourceID,
				Resource:   tt.fields.Resource,
				UserID:     tt.fields.UserID,
				UserType:   tt.fields.UserType,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
