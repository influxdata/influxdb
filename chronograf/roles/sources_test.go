package roles

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/chronograf"
	"github.com/influxdata/influxdb/chronograf/mocks"
)

func TestSources_Get(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		role string
		id   int
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
				role: "viewer",
				id:   1,
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
				role: "editor",
				id:   1,
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
				role: "admin",
				id:   1,
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
				role: "editor",
				id:   1,
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
				role: "admin",
				id:   1,
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
			name: "Get editor source as viewer - want error",
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
				role: "viewer",
				id:   1,
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
				role: "admin",
				id:   1,
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
			name: "Get admin source as viewer - want error",
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
				role: "viewer",
				id:   1,
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get admin source as editor - want error",
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
				role: "editor",
				id:   1,
			},
			wants: wants{
				err: true,
			},
		},
		{
			name: "Get source bad context",
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
				role: "random role",
				id:   1,
			},
			wants: wants{
				err: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSourcesStore(tt.fields.SourcesStore, tt.args.role)

			ctx := context.Background()

			if tt.args.role != "" {
				ctx = context.WithValue(ctx, ContextKey, tt.args.role)
			}

			source, err := store.Get(ctx, tt.args.id)
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

func TestSources_All(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		role string
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
							{
								ID:           2,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "admin",
							},
						}, nil
					},
				},
			},
			args: args{
				role: "viewer",
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
			name: "Get editor sources as editor",
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
								Name:         "my sweet name",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "admin",
							},
						}, nil
					},
				},
			},
			args: args{
				role: "editor",
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
						Name:         "my sweet name",
						Organization: "0",
						Role:         "editor",
					},
				},
			},
		},
		{
			name: "Get admin sources as admin",
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
								Name:         "my sweet name",
								Organization: "0",
								Role:         "editor",
							},
							{
								ID:           3,
								Name:         "my sweet name",
								Organization: "0",
								Role:         "admin",
							},
						}, nil
					},
				},
			},
			args: args{
				role: "admin",
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
						Name:         "my sweet name",
						Organization: "0",
						Role:         "editor",
					},
					{
						ID:           3,
						Name:         "my sweet name",
						Organization: "0",
						Role:         "admin",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := NewSourcesStore(tt.fields.SourcesStore, tt.args.role)

			ctx := context.Background()

			if tt.args.role != "" {
				ctx = context.WithValue(ctx, ContextKey, tt.args.role)
			}

			sources, err := store.All(ctx)
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
