package influxdb_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/stretchr/testify/require"
)

func TestOwnerMappingValidate(t *testing.T) {
	type fields struct {
		ResourceID   platform.ID
		ResourceType platform.ResourceType
		UserID       platform.ID
		UserType     platform.UserType
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid mapping",
			fields: fields{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Owner,
				ResourceType: platform.DashboardsResourceType,
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
			},
		},
		{
			name: "mapping requires a resourceid",
			fields: fields{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Owner,
				ResourceType: platform.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a userid",
			fields: fields{
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
				UserType:     platform.Owner,
				ResourceType: platform.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "mapping requires a usertype",
			fields: fields{
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				ResourceType: platform.DashboardsResourceType,
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
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     "foo",
				ResourceType: platform.DashboardsResourceType,
			},
			wantErr: true,
		},
		{
			name: "the resourcetype provided must be valid",
			fields: fields{
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Owner,
				ResourceType: "foo",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.UserResourceMapping{
				ResourceID:   tt.fields.ResourceID,
				ResourceType: tt.fields.ResourceType,
				UserID:       tt.fields.UserID,
				UserType:     tt.fields.UserType,
			}
			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("OwnerMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOwnerMappingToPermissions(t *testing.T) {
	type wants struct {
		perms platform.Permission
		err   bool
	}

	ResourceID, _ := platform.IDFromString("020f755c3c082000")

	tests := []struct {
		name  string
		urm   platform.UserResourceMapping
		wants wants
	}{
		{
			name: "Org Member Has Permission To Read Org",
			urm: platform.UserResourceMapping{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Member,
				ResourceType: platform.OrgsResourceType,
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Org Owner Has Permission To Write Org",
			urm: platform.UserResourceMapping{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Owner,
				ResourceType: platform.OrgsResourceType,
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "write", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Org Owner Has Permission To Read Org",
			urm: platform.UserResourceMapping{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Owner,
				ResourceType: platform.OrgsResourceType,
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "orgs", ID: ResourceID}}},
		},
		{
			name: "Bucket Member User Has Permission To Read Bucket",
			urm: platform.UserResourceMapping{
				UserID:       platformtesting.MustIDBase16("debac1e0deadbeef"),
				UserType:     platform.Member,
				ResourceType: platform.BucketsResourceType,
				ResourceID:   platformtesting.MustIDBase16("020f755c3c082000"),
			},
			wants: wants{
				err:   false,
				perms: influxdb.Permission{Action: "read", Resource: influxdb.Resource{Type: "buckets", ID: ResourceID}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perms, err := tt.urm.ToPermissions()

			require.Contains(t, perms, tt.wants.perms)
			require.Equal(t, tt.wants.err, err != nil)
		})
	}
}
