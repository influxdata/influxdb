package bolt_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/bolt"
	"github.com/influxdata/influxdb/chronograf/roles"
)

var orgCmpOptions = cmp.Options{
	cmpopts.IgnoreFields(chronograf.Organization{}, "ID"),
	cmpopts.EquateEmpty(),
}

func TestOrganizationsStore_GetWithName(t *testing.T) {
	type args struct {
		ctx context.Context
		org *chronograf.Organization
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.Organization
		wantErr  bool
		addFirst bool
	}{
		{
			name: "Organization not found",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{},
			},
			wantErr: true,
		},
		{
			name: "Get Organization",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					Name: "EE - Evil Empire",
				},
			},
			want: &chronograf.Organization{
				Name: "EE - Evil Empire",
			},
			addFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTestClient()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			s := client.OrganizationsStore
			if tt.addFirst {
				tt.args.org, err = s.Add(tt.args.ctx, tt.args.org)
				if err != nil {
					t.Fatal(err)
				}
			}

			got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{Name: &tt.args.org.Name})
			if (err != nil) != tt.wantErr {
				t.Errorf("%q. OrganizationsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
			}

		})
	}
}

func TestOrganizationsStore_GetWithID(t *testing.T) {
	type args struct {
		ctx context.Context
		org *chronograf.Organization
	}
	tests := []struct {
		name     string
		args     args
		want     *chronograf.Organization
		wantErr  bool
		addFirst bool
	}{
		{
			name: "Organization not found",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					ID: "1234",
				},
			},
			wantErr: true,
		},
		{
			name: "Get Organization",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					Name: "EE - Evil Empire",
				},
			},
			want: &chronograf.Organization{
				Name: "EE - Evil Empire",
			},
			addFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTestClient()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			s := client.OrganizationsStore
			if tt.addFirst {
				tt.args.org, err = s.Add(tt.args.ctx, tt.args.org)
				if err != nil {
					t.Fatal(err)
				}
			}

			got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{ID: &tt.args.org.ID})
			if (err != nil) != tt.wantErr {
				t.Errorf("%q. OrganizationsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
			}

		})
	}
}

func TestOrganizationsStore_All(t *testing.T) {
	type args struct {
		ctx  context.Context
		orgs []chronograf.Organization
	}
	tests := []struct {
		name     string
		args     args
		want     []chronograf.Organization
		addFirst bool
	}{
		{
			name: "Get Organizations",
			args: args{
				ctx: context.Background(),
				orgs: []chronograf.Organization{
					{
						Name:        "EE - Evil Empire",
						DefaultRole: roles.MemberRoleName,
					},
					{
						Name:        "The Good Place",
						DefaultRole: roles.EditorRoleName,
					},
				},
			},
			want: []chronograf.Organization{
				{
					Name:        "EE - Evil Empire",
					DefaultRole: roles.MemberRoleName,
				},
				{
					Name:        "The Good Place",
					DefaultRole: roles.EditorRoleName,
				},
				{
					Name:        bolt.DefaultOrganizationName,
					DefaultRole: bolt.DefaultOrganizationRole,
				},
			},
			addFirst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTestClient()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			s := client.OrganizationsStore
			if tt.addFirst {
				for _, org := range tt.args.orgs {
					_, err = s.Add(tt.args.ctx, &org)
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			got, err := s.All(tt.args.ctx)
			if err != nil {
				t.Fatal(err)
				return
			}
			if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}

func TestOrganizationsStore_Update(t *testing.T) {
	type fields struct {
		orgs []chronograf.Organization
	}
	type args struct {
		ctx     context.Context
		initial *chronograf.Organization
		updates *chronograf.Organization
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		addFirst bool
		want     *chronograf.Organization
		wantErr  bool
	}{
		{
			name:   "No such organization",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					ID:   "1234",
					Name: "The Okay Place",
				},
				updates: &chronograf.Organization{},
			},
			wantErr: true,
		},
		{
			name:   "Update organization name",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name: "The Good Place",
				},
				updates: &chronograf.Organization{
					Name: "The Bad Place",
				},
			},
			want: &chronograf.Organization{
				Name: "The Bad Place",
			},
			addFirst: true,
		},
		{
			name:   "Update organization default role",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name: "The Good Place",
				},
				updates: &chronograf.Organization{
					DefaultRole: roles.ViewerRoleName,
				},
			},
			want: &chronograf.Organization{
				Name:        "The Good Place",
				DefaultRole: roles.ViewerRoleName,
			},
			addFirst: true,
		},
		{
			name:   "Update organization name and default role",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name:        "The Good Place",
					DefaultRole: roles.AdminRoleName,
				},
				updates: &chronograf.Organization{
					Name:        "The Bad Place",
					DefaultRole: roles.ViewerRoleName,
				},
			},
			want: &chronograf.Organization{
				Name:        "The Bad Place",
				DefaultRole: roles.ViewerRoleName,
			},
			addFirst: true,
		},
		{
			name:   "Update organization name, role",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name:        "The Good Place",
					DefaultRole: roles.ViewerRoleName,
				},
				updates: &chronograf.Organization{
					Name:        "The Bad Place",
					DefaultRole: roles.AdminRoleName,
				},
			},
			want: &chronograf.Organization{
				Name:        "The Bad Place",
				DefaultRole: roles.AdminRoleName,
			},
			addFirst: true,
		},
		{
			name:   "Update organization name",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name:        "The Good Place",
					DefaultRole: roles.EditorRoleName,
				},
				updates: &chronograf.Organization{
					Name: "The Bad Place",
				},
			},
			want: &chronograf.Organization{
				Name:        "The Bad Place",
				DefaultRole: roles.EditorRoleName,
			},
			addFirst: true,
		},
		{
			name:   "Update organization name",
			fields: fields{},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name: "The Good Place",
				},
				updates: &chronograf.Organization{
					Name: "The Bad Place",
				},
			},
			want: &chronograf.Organization{
				Name: "The Bad Place",
			},
			addFirst: true,
		},
		{
			name: "Update organization name - name already taken",
			fields: fields{
				orgs: []chronograf.Organization{
					{
						Name: "The Bad Place",
					},
				},
			},
			args: args{
				ctx: context.Background(),
				initial: &chronograf.Organization{
					Name: "The Good Place",
				},
				updates: &chronograf.Organization{
					Name: "The Bad Place",
				},
			},
			wantErr:  true,
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationsStore

		for _, org := range tt.fields.orgs {
			_, err = s.Add(tt.args.ctx, &org)
			if err != nil {
				t.Fatal(err)
			}
		}

		if tt.addFirst {
			tt.args.initial, err = s.Add(tt.args.ctx, tt.args.initial)
			if err != nil {
				t.Fatal(err)
			}
		}

		if tt.args.updates.Name != "" {
			tt.args.initial.Name = tt.args.updates.Name
		}
		if tt.args.updates.DefaultRole != "" {
			tt.args.initial.DefaultRole = tt.args.updates.DefaultRole
		}

		if err := s.Update(tt.args.ctx, tt.args.initial); (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		// for the empty test
		if tt.want == nil {
			continue
		}

		got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{Name: &tt.args.initial.Name})
		if err != nil {
			t.Fatalf("failed to get organization: %v", err)
		}
		if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationStore_Delete(t *testing.T) {
	type args struct {
		ctx context.Context
		org *chronograf.Organization
	}
	tests := []struct {
		name     string
		args     args
		addFirst bool
		wantErr  bool
	}{
		{
			name: "No such organization",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					ID: "10",
				},
			},
			wantErr: true,
		},
		{
			name: "Delete new organization",
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					Name: "The Deleted Place",
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationsStore

		if tt.addFirst {
			tt.args.org, _ = s.Add(tt.args.ctx, tt.args.org)
		}
		if err := s.Delete(tt.args.ctx, tt.args.org); (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestOrganizationStore_DeleteDefaultOrg(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Delete the default organization",
			args: args{
				ctx: context.Background(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationsStore

		defaultOrg, err := s.DefaultOrganization(tt.args.ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(tt.args.ctx, defaultOrg); (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Delete() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestOrganizationsStore_Add(t *testing.T) {
	type fields struct {
		orgs []chronograf.Organization
	}
	type args struct {
		ctx context.Context
		org *chronograf.Organization
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.Organization
		wantErr bool
	}{
		{
			name: "Add organization - organization already exists",
			fields: fields{
				orgs: []chronograf.Organization{
					{
						Name: "The Good Place",
					},
				},
			},
			args: args{
				ctx: context.Background(),
				org: &chronograf.Organization{
					Name: "The Good Place",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		s := client.OrganizationsStore

		for _, org := range tt.fields.orgs {
			_, err = s.Add(tt.args.ctx, &org)
			if err != nil {
				t.Fatal(err)
			}
		}

		_, err = s.Add(tt.args.ctx, tt.args.org)

		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		// for the empty test
		if tt.want == nil {
			continue
		}

		got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{Name: &tt.args.org.Name})
		if err != nil {
			t.Fatalf("failed to get organization: %v", err)
		}
		if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizationsStore_DefaultOrganization(t *testing.T) {
	type fields struct {
		orgs []chronograf.Organization
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *chronograf.Organization
		wantErr bool
	}{
		{
			name: "Get Default Organization",
			fields: fields{
				orgs: []chronograf.Organization{
					{
						Name: "The Good Place",
					},
				},
			},
			args: args{
				ctx: context.Background(),
			},
			want: &chronograf.Organization{
				ID:          string(bolt.DefaultOrganizationID),
				Name:        bolt.DefaultOrganizationName,
				DefaultRole: bolt.DefaultOrganizationRole,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		client, err := NewTestClient()
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		s := client.OrganizationsStore

		for _, org := range tt.fields.orgs {
			_, err = s.Add(tt.args.ctx, &org)
			if err != nil {
				t.Fatal(err)
			}
		}

		got, err := s.DefaultOrganization(tt.args.ctx)

		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}

		if tt.want == nil {
			continue
		}

		if diff := cmp.Diff(got, tt.want, orgCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
