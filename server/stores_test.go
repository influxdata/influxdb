package server

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/mocks"
	"github.com/influxdata/chronograf/organizations"
	"github.com/influxdata/chronograf/roles"
)

func TestStore_SourcesGet(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		role         string
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
			name: "Get viewer source as viewer",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "viewer",
				},
			},
		},
		{
			name: "Get viewer source as editor",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "editor",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "viewer",
				},
			},
		},
		{
			name: "Get viewer source as admin",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "admin",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "viewer",
				},
			},
		},
		{
			name: "Get admin source as viewer",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "admin",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get editor source as viewer",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "editor",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get editor source as editor",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "editor",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "editor",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "editor",
				},
			},
		},
		{
			name: "Get editor source as admin",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "editor",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "admin",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "editor",
				},
			},
		},
		{
			name: "Get editor source as viewer",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "editor",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get admin source as admin",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "admin",
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "admin",
			},
			wants: wants{
				source: chronograf.Source{
					ID:           1,
					Name:         "my sweet name",
					Organization: "0",
					Role:         "admin",
				},
			},
		},
		{
			name: "No organization or role set on context",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get source as viewer - no organization specified on context",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				role: "viewer",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get source as editor - no organization specified on context",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				role: "editor",
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get source as admin - no organization specified on context",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1,
							Name:         "my sweet name",
							Organization: "0",
							Role:         "viewer",
						}, nil
					},
				},
			},
			args: args{
				role: "admin",
			},
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

			if tt.args.role != "" {
				ctx = context.WithValue(ctx, roles.ContextKey, tt.args.role)
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
		role         string
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
			name: "Get viewer sources as viewer",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "viewer",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
						Role:         "viewer",
					},
				},
			},
		},
		{
			name: "Get viewer sources as viewer - multiple orgs and multiple roles",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           2,
								Name:         "A bad source",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "A good source",
								Organization: "0",
								Role:         "admin",
							},
							{
								ID:           4,
								Name:         "a source I can has",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           5,
								Name:         "i'm in the wrong org",
								Organization: "1",
								Role:         "viewer",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "viewer",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
						Role:         "viewer",
					},
					{
						ID:           4,
						Name:         "a source I can has",
						Organization: "0",
						Role:         "viewer",
					},
				},
			},
		},
		{
			name: "Get editor sources as editor - multiple orgs and multiple roles",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           2,
								Name:         "A bad source",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "A good source",
								Organization: "0",
								Role:         "admin",
							},
							{
								ID:           4,
								Name:         "a source I can has",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           5,
								Name:         "i'm in the wrong org",
								Organization: "1",
								Role:         "viewer",
							},
							{
								ID:           2,
								Name:         "i'm an editor, but wrong org",
								Organization: "3",
								Role:         "editor",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "editor",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
						Role:         "viewer",
					},
					{
						ID:           2,
						Name:         "A bad source",
						Organization: "0",
						Role:         "editor",
					},
					{
						ID:           4,
						Name:         "a source I can has",
						Organization: "0",
						Role:         "viewer",
					},
				},
			},
		},
		{
			name: "Get admin sources as admin - multiple orgs and multiple roles",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								ID:           1,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           2,
								Name:         "A bad source",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "A good source",
								Organization: "0",
								Role:         "admin",
							},
							{
								ID:           4,
								Name:         "a source I can has",
								Organization: "0",
								Role:         "viewer",
							},
							{
								ID:           5,
								Name:         "i'm in the wrong org",
								Organization: "1",
								Role:         "viewer",
							},
							{
								ID:           2,
								Name:         "i'm an editor, but wrong org",
								Organization: "3",
								Role:         "editor",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "0",
				role:         "admin",
			},
			wants: wants{
				sources: []chronograf.Source{
					{
						ID:           1,
						Name:         "my sweet name",
						Organization: "0",
						Role:         "viewer",
					},
					{
						ID:           2,
						Name:         "A bad source",
						Organization: "0",
						Role:         "editor",
					},
					{
						ID:           3,
						Name:         "A good source",
						Organization: "0",
						Role:         "admin",
					},
					{
						ID:           4,
						Name:         "a source I can has",
						Organization: "0",
						Role:         "viewer",
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

			if tt.args.role != "" {
				ctx = context.WithValue(ctx, roles.ContextKey, tt.args.role)
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
