package platform

import (
	"testing"
)

func TestDBRPMapping_Validate(t *testing.T) {
	type fields struct {
		Cluster         string
		Database        string
		RetentionPolicy string
		Default         bool
		OrganizationID  ID
		BucketID        ID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "mapping requires a cluster",
			fields: fields{
				Cluster: "",
			},
			wantErr: true,
		},
		{
			name: "mapping requires a database",
			fields: fields{
				Cluster:  "abc",
				Database: "",
			},
			wantErr: true,
		},
		{
			name: "mapping requires an rp",
			fields: fields{
				Cluster:         "abc",
				Database:        "telegraf",
				RetentionPolicy: "",
			},
			wantErr: true,
		},
		{
			name: "mapping requires an orgid",
			fields: fields{
				Cluster:         "abc",
				Database:        "telegraf",
				RetentionPolicy: "autogen",
			},
			wantErr: true,
		},
		{
			name: "mapping requires a bucket id",
			fields: fields{
				Cluster:         "abc",
				Database:        "telegraf",
				RetentionPolicy: "autogen",
				OrganizationID:  []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
			},
			wantErr: true,
		},
		{
			name: "cluster name cannot have non-printable characters.",
			fields: fields{
				Cluster: string([]byte{0x0D}),
			},
			wantErr: true,
		},
		{
			name: "db cannot have non-letters/numbers/_/./-",
			fields: fields{
				Cluster:  "12345_.",
				Database: string([]byte{0x0D}),
			},
			wantErr: true,
		},
		{
			name: "rp cannot have non-printable characters",
			fields: fields{
				Cluster:         "12345",
				Database:        "telegraf",
				RetentionPolicy: string([]byte{0x0D}),
			},
			wantErr: true,
		},

		{
			name: "dash accepted as valid database",
			fields: fields{
				Cluster:         "12345_.",
				Database:        "howdy-doody",
				RetentionPolicy: "autogen",
				OrganizationID:  []byte{0xde, 0xba, 0xc1, 0xe0, 0xde, 0xad, 0xbe, 0xef},
				BucketID:        []byte{0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xa7},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := DBRPMapping{
				Cluster:         tt.fields.Cluster,
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

func Test_validName(t *testing.T) {
	tests := []struct {
		arg  string
		name string
		want bool
	}{
		{
			name: "names cannot have unprintable characters",
			arg:  string([]byte{0x0D}),
			want: false,
		},
		{
			name: "names cannot have .",
			arg:  ".",
			want: false,
		},
		{
			name: "names cannot have ..",
			arg:  "..",
			want: false,
		},
		{
			name: "names cannot have /",
			arg:  "/",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validName(tt.arg); got != tt.want {
				t.Errorf("validName() = %v, want %v", got, tt.want)
			}
		})
	}
}
