package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/organizations"
)

func TestStore_SourcesGet(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		id           int
	}
	type wants struct {
		source chronograf.Source
		err    bool
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Get source",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
				},
			},
		},
		{
			name: "Get source - no organization specified on context",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				err: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				SourcesStore: tt.fields.SourcesStore,
			}

			ctx := context.Background()

			if tt.args.organization != "" {
				ctx = context.WithValue(ctx, organizations.ContextKey, tt.args.organization)
			}

			source, err := store.Sources(ctx).Get(ctx, tt.args.id)
			if (err != nil) != tt.wants.err {
				t.Errorf("%q. Store.Sources().Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
				return
			}
			if diff := cmp.Diff(source, tt.wants.source); diff != "" {
				t.Errorf("%q. Store.Sources().Get():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}

func TestStore_SourcesAll(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
	}
	type wants struct {
		sources []chronograf.Source
		err     bool
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Get sources",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
					},
				},
			},
		},
		{
			name: "Get sources - multiple orgs",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
							},
							{
								ID:           2,
								Name:         "A bad source",
								Organization: "0",
							},
							{
								ID:           3,
								Name:         "A good source",
								Organization: "0",
							},
							{
								ID:           4,
								Name:         "a source I can has",
								Organization: "0",
							},
							{
								ID:           5,
								Name:         "i'm in the wrong org",
								Organization: "1",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
					},
					{
						ID:           2,
						Name:         "A bad source",
						Organization: "0",
					},
					{
						ID:           3,
						Name:         "A good source",
						Organization: "0",
					},
					{
						ID:           4,
						Name:         "a source I can has",
						Organization: "0",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				SourcesStore: tt.fields.SourcesStore,
			}

			ctx := context.Background()

			if tt.args.organization != "" {
				ctx = context.WithValue(ctx, organizations.ContextKey, tt.args.organization)
			}

			sources, err := store.Sources(ctx).All(ctx)
			if (err != nil) != tt.wants.err {
				t.Errorf("%q. Store.Sources().Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
				return
			}
			if diff := cmp.Diff(sources, tt.wants.sources); diff != "" {
				t.Errorf("%q. Store.Sources().Get():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}

func TestStore_OrganizationsAdd(t *testing.T) {
	type fields struct {
		OrganizationsStore chronograf.OrganizationsStore
	}
	type args struct {
		orgID         string
		serverContext bool
		organization  string
		user          *chronograf.User
	}
	type wants struct {
		organization *chronograf.Organization
		err          bool
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "Get organization with server context",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "21",
							Name:        "my sweet name",
							DefaultRole: "viewer",
						}, nil
					},
				},
			},
			args: args{
				serverContext: true,
				orgID:         "21",
			},
			wants: wants{
				organization: &chronograf.Organization{
					ID:          "21",
					Name:        "my sweet name",
					DefaultRole: "viewer",
				},
			},
		},
		{
			name: "Get organization with super admin",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "21",
							Name:        "my sweet name",
							DefaultRole: "viewer",
						}, nil
					},
				},
			},
			args: args{
				user: &chronograf.User{
					ID:         1337,
					Name:       "bobbetta",
					Provider:   "github",
					Scheme:     "oauth2",
					SuperAdmin: true,
				},
				orgID: "21",
			},
			wants: wants{
				organization: &chronograf.Organization{
					ID:          "21",
					Name:        "my sweet name",
					DefaultRole: "viewer",
				},
			},
		},
		{
			name: "Get organization not as super admin no organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "21",
							Name:        "my sweet name",
							DefaultRole: "viewer",
						}, nil
					},
				},
			},
			args: args{
				user: &chronograf.User{
					ID:       1337,
					Name:     "bobbetta",
					Provider: "github",
					Scheme:   "oauth2",
				},
				orgID: "21",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get organization not as super admin with organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						fmt.Println(*q.ID)
						return &chronograf.Organization{
							ID:          "22",
							Name:        "my sweet name",
							DefaultRole: "viewer",
						}, nil
					},
				},
			},
			args: args{
				user: &chronograf.User{
					ID:       1337,
					Name:     "bobbetta",
					Provider: "github",
					Scheme:   "oauth2",
				},
				organization: "22",
				orgID:        "22",
			},
			wants: wants{
				organization: &chronograf.Organization{
					ID:          "22",
					Name:        "my sweet name",
					DefaultRole: "viewer",
				},
			},
		},
		{
			name: "Get different organization not as super admin with organization",
			fields: fields{
				OrganizationsStore: &mocks.OrganizationsStore{
					GetF: func(ctx context.Context, q chronograf.OrganizationQuery) (*chronograf.Organization, error) {
						return &chronograf.Organization{
							ID:          "22",
							Name:        "my sweet name",
							DefaultRole: "viewer",
						}, nil
					},
				},
			},
			args: args{
				user: &chronograf.User{
					ID:       1337,
					Name:     "bobbetta",
					Provider: "github",
					Scheme:   "oauth2",
				},
				organization: "21",
				orgID:        "21",
			},
			wants: wants{
				err: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{
				OrganizationsStore: tt.fields.OrganizationsStore,
			}

			ctx := context.Background()

			if tt.args.serverContext {
				ctx = serverContext(ctx)
			}

			if tt.args.organization != "" {
				ctx = context.WithValue(ctx, organizations.ContextKey, tt.args.organization)
			}

			if tt.args.user != nil {
				ctx = context.WithValue(ctx, UserContextKey, tt.args.user)
			}

			organization, err := store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &tt.args.orgID})
			if (err != nil) != tt.wants.err {
				t.Errorf("%q. Store.Organizations().Get() error = %v, wantErr %v", tt.name, err, tt.wants.err)
				return
			}
			if diff := cmp.Diff(organization, tt.wants.organization); diff != "" {
				t.Errorf("%q. Store.Organizations().Get():\n-got/+want\ndiff %s", tt.name, diff)
			}
		})
	}
}
