package platform_test

import (
	"testing"

	"github.com/influxdata/platform"
)

func TestAuthorizer_PermissionAllowed(t *testing.T) {
	tests := []struct {
		name        string
		permission  platform.Permission
		permissions []platform.Permission
		allowed     bool
	}{
		{
			name: "bad resource id in permission",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(0),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.BucketsResource,
					ID:       IDPtr(1),
				},
			},
			allowed: false,
		},
		{
			name: "bad resource id in permissions",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(1),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.BucketsResource,
					ID:       IDPtr(0),
				},
			},
			allowed: false,
		},
		{
			name: "matching action resource and ID",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(1),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.BucketsResource,
					ID:       IDPtr(1),
				},
			},
			allowed: true,
		},
		{
			name: "matching action resource no ID",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.BucketsResource,
				},
			},
			allowed: true,
		},
		{
			name: "matching action resource differing ID",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(1),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.BucketsResource,
					ID:       IDPtr(2),
				},
			},
			allowed: false,
		},
		{
			name: "differing action same resource",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(1),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.ReadAction,
					Resource: platform.BucketsResource,
					ID:       IDPtr(1),
				},
			},
			allowed: false,
		},
		{
			name: "same action differing resource",
			permission: platform.Permission{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       IDPtr(1),
			},
			permissions: []platform.Permission{
				{
					Action:   platform.WriteAction,
					Resource: platform.TasksResource,
					ID:       IDPtr(1),
				},
			},
			allowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed := platform.PermissionAllowed(tt.permission, tt.permissions)
			if allowed != tt.allowed {
				t.Errorf("got allowed = %v, expected allowed = %v", allowed, tt.allowed)
			}
		})
	}
}

func TestPermission_Valid(t *testing.T) {
	type fields struct {
		Action   platform.Action
		Resource platform.Resource
		ID       *platform.ID
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid bucket permission with ID",
			fields: fields{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       validID(),
			},
		},
		{
			name: "valid bucket permission with nil ID",
			fields: fields{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       nil,
			},
		},
		{
			name: "invalid bucket permission with an invalid ID",
			fields: fields{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       func() *platform.ID { id := platform.InvalidID(); return &id }(),
			},
			wantErr: true,
		},
		{
			name: "invalid permission without an action",
			fields: fields{
				Resource: platform.BucketsResource,
			},
			wantErr: true,
		},
		{
			name: "invalid permission without a resource",
			fields: fields{
				Action: platform.WriteAction,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &platform.Permission{
				Action:   tt.fields.Action,
				Resource: tt.fields.Resource,
				ID:       tt.fields.ID,
			}
			if err := p.Valid(); (err != nil) != tt.wantErr {
				t.Errorf("Permission.Valid() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPermissionAllResources_Valid(t *testing.T) {
	var resources = []platform.Resource{
		platform.UsersResource,
		platform.OrgsResource,
		platform.TasksResource,
		platform.BucketsResource,
		platform.DashboardsResource,
		platform.SourcesResource,
	}

	for _, r := range resources {
		p := &platform.Permission{
			Action:   platform.WriteAction,
			Resource: r,
		}

		if err := p.Valid(); err != nil {
			t.Errorf("PermissionAllResources.Valid() error = %v", err)
		}
	}
}

func TestPermissionAllActions(t *testing.T) {
	var actions = []platform.Action{
		platform.ReadAction,
		platform.WriteAction,
	}

	for _, a := range actions {
		p := &platform.Permission{
			Action:   a,
			Resource: platform.TasksResource,
		}

		if err := p.Valid(); err != nil {
			t.Errorf("PermissionAllActions.Valid() error = %v", err)
		}
	}
}

func TestPermission_String(t *testing.T) {
	type fields struct {
		Action   platform.Action
		Resource platform.Resource
		ID       *platform.ID
		Name     *string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "valid permission with no id",
			fields: fields{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
			},
			want: `write:buckets`,
		},
		{
			name: "valid permission with an id",
			fields: fields{
				Action:   platform.WriteAction,
				Resource: platform.BucketsResource,
				ID:       validID(),
			},
			want: `write:buckets:0000000000000064`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := platform.Permission{
				Action:   tt.fields.Action,
				Resource: tt.fields.Resource,
				ID:       tt.fields.ID,
			}
			if got := p.String(); got != tt.want {
				t.Errorf("Permission.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func validID() *platform.ID {
	id := platform.ID(100)
	return &id
}

func IDPtr(id platform.ID) *platform.ID {
	return &id
}
