package organizations_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/organizations"
)

// IgnoreFields is used because ID cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var organizationCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Organization{}, "ID"),
}

func TestOrganizations_All(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    []chronograf.Organization
		wantRaw []chronograf.Organization
		wantErr bool
	}{
		{
			name: "No Organizations",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					AllF: func(ctx context.Context) ([]chronograf.Organization, error) {
						return nil, fmt.Errorf("No Organizations")
					},
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   0,
							Name: "Default",
						}, nil
					},
				},
			},
			wantErr: true,
		},
		{
			name: "All Organizations",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					DefaultOrganizationF: func(ctx context.Context) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   0,
							Name: "Default",
						}, nil
					},
					AllF: func(ctx context.Context) ([]chronograf.Organization, error) {
						return []chronograf.Organization{
							{
								Name: "howdy",
								ID:   1337,
							},
							{
								Name: "doody",
								ID:   1447,
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
			},
			want: []chronograf.Organization{
				{
					Name: "howdy",
					ID:   1337,
				},
				{
					Name: "Default",
					ID:   0,
				},
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewOrganizationsStore(tt.fields.OrganizationsStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], organizationCmpOptions...); diff != "" {
				t.Errorf("%q. OrganizationsStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestOrganizations_Add(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		organizationID string
		ctx            context.Context
		organization   *chronograf.Organization
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    *chronograf.Organization
		wantErr bool
	}{
		{
			name: "Add Organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					AddF: func(ctx context.Context, s *chronograf.Organization) (*chronograf.Organization, error) {
						return s, nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   1229,
							Name: "howdy",
						}, nil
					},
				},
			},
			args: args{
				organizationID: "1229",
				ctx:            context.Background(),
				organization: &chronograf.Organization{
					Name: "howdy",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s := organizations.NewOrganizationsStore(tt.fields.OrganizationsStore, tt.args.organizationID)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organizationID)
		d, err := s.Add(tt.args.ctx, tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if tt.wantErr {
			continue
		}
		got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{ID: &d.ID})
		if diff := cmp.Diff(got, tt.want, organizationCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizations_Delete(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		organizationID string
		ctx            context.Context
		organization   *chronograf.Organization
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     []chronograf.Organization
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					DeleteF: func(ctx context.Context, s *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   1229,
							Name: "howdy",
						}, nil
					},
				},
			},
			args: args{
				organizationID: "1229",
				ctx:            context.Background(),
				organization: &chronograf.Organization{
					ID:   1229,
					Name: "howdy",
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		s := organizations.NewOrganizationsStore(tt.fields.OrganizationsStore, tt.args.organizationID)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organizationID)
		err := s.Delete(tt.args.ctx, tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestOrganizations_Get(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		organizationID string
		ctx            context.Context
		organization   *chronograf.Organization
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     *chronograf.Organization
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   1337,
							Name: "howdy",
						}, nil
					},
				},
			},
			args: args{
				organizationID: "1337",
				ctx:            context.Background(),
				organization: &chronograf.Organization{
					ID:   1337,
					Name: "howdy",
				},
			},
			want: &chronograf.Organization{
				ID:   1337,
				Name: "howdy",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewOrganizationsStore(tt.fields.OrganizationsStore, tt.args.organizationID)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organizationID)
		got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{ID: &tt.args.organization.ID})
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, organizationCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestOrganizations_Update(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		organizationID string
		ctx            context.Context
		organization   *chronograf.Organization
		name           string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     *chronograf.Organization
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Organization Name",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					UpdateF: func(ctx context.Context, s *chronograf.Organization) error {
						return nil
					},
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:   1229,
							Name: "doody",
						}, nil
					},
				},
			},
			args: args{
				organizationID: "1229",
				ctx:            context.Background(),
				organization: &chronograf.Organization{
					ID:   1229,
					Name: "howdy",
				},
				name: "doody",
			},
			want: &chronograf.Organization{
				Name: "doody",
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		if tt.args.name != "" {
			tt.args.organization.Name = tt.args.name
		}
		s := organizations.NewOrganizationsStore(tt.fields.OrganizationsStore, tt.args.organizationID)
		tt.args.ctx = context.WithValue(tt.args.ctx, organizations.ContextKey, tt.args.organizationID)
		err := s.Update(tt.args.ctx, tt.args.organization)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. OrganizationsStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, chronograf.OrganizationQuery{ID: &tt.args.organization.ID})
		if diff := cmp.Diff(got, tt.want, organizationCmpOptions...); diff != "" {
			t.Errorf("%q. OrganizationsStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
