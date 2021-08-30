package influxdb_test

import (
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func TestDBRPMapping_Validate(t *testing.T) {
	type fields struct {
		Database        string
		RetentionPolicy string
		Default         bool
		OrganizationID  platform2.ID
		BucketID        platform2.ID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "mapping requires a database",
			fields: fields{
				Database: "",
			},
			wantErr: true,
		},
		{
			name: "mapping requires an rp",
			fields: fields{
				Database:        "telegraf",
				RetentionPolicy: "",
			},
			wantErr: true,
		},
		{
			name: "mapping requires an orgid",
			fields: fields{
				Database:        "telegraf",
				RetentionPolicy: "autogen",
			},
			wantErr: true,
		},
		{
			name: "mapping requires a bucket id",
			fields: fields{
				Database:        "telegraf",
				RetentionPolicy: "autogen",
				OrganizationID:  platformtesting.MustIDBase16("debac1e0deadbeef"),
			},
			wantErr: true,
		},
		{
			name: "db cannot have non-letters/numbers/_/./-",
			fields: fields{
				Database: string([]byte{0x0D}),
			},
			wantErr: true,
		},
		{
			name: "rp cannot have non-printable characters",
			fields: fields{
				Database:        "telegraf",
				RetentionPolicy: string([]byte{0x0D}),
			},
			wantErr: true,
		},
		{
			name: "dash accepted as valid database",
			fields: fields{
				Database:        "howdy-doody",
				RetentionPolicy: "autogen",
				OrganizationID:  platformtesting.MustIDBase16("debac1e0deadbeef"),
				BucketID:        platformtesting.MustIDBase16("5ca1ab1edeadbea7"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := platform.DBRPMapping{
				Database:        tt.fields.Database,
				RetentionPolicy: tt.fields.RetentionPolicy,
				Default:         tt.fields.Default,
				OrganizationID:  tt.fields.OrganizationID,
				BucketID:        tt.fields.BucketID,
			}

			if err := m.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("DBRPMapping.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
