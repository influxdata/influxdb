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

// IgnoreFields is used because ID is created by BoltDB and cannot be predicted reliably
// EquateEmpty is used because we want nil slices, arrays, and maps to be equal to the empty map
var sourceCmpOptions = cmp.Options{
	cmpopts.EquateEmpty(),
	cmpopts.IgnoreFields(chronograf.Source{}, "ID"),
	cmpopts.IgnoreFields(chronograf.Source{}, "Default"),
}

func TestSources_All(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		ctx          context.Context
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    []chronograf.Source
		wantRaw []chronograf.Source
		wantErr bool
	}{
		{
			name: "No Sources",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return nil, fmt.Errorf("No Sources")
					},
				},
			},
			wantErr: true,
		},
		{
			name: "All Sources",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AllF: func(ctx context.Context) ([]chronograf.Source, error) {
						return []chronograf.Source{
							{
								Name:         "howdy",
								Organization: "1337",
							},
							{
								Name:         "doody",
								Organization: "1338",
							},
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
			},
			want: []chronograf.Source{
				{
					Name:         "howdy",
					Organization: "1337",
				},
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewSourcesStore(tt.fields.SourcesStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		gots, err := s.All(tt.args.ctx)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. SourcesStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		for i, got := range gots {
			if diff := cmp.Diff(got, tt.want[i], sourceCmpOptions...); diff != "" {
				t.Errorf("%q. SourcesStore.All():\n-got/+want\ndiff %s", tt.name, diff)
			}
		}
	}
}

func TestSources_Add(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name    string
		args    args
		fields  fields
		want    chronograf.Source
		wantErr bool
	}{
		{
			name: "Add Source",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					AddF: func(ctx context.Context, s chronograf.Source) (chronograf.Source, error) {
						return s, nil
					},
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					ID:   1229,
					Name: "howdy",
				},
			},
			want: chronograf.Source{
				Name:         "howdy",
				Organization: "1337",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewSourcesStore(tt.fields.SourcesStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		d, err := s.Add(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. SourcesStore.Add() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, d.ID)
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. SourcesStore.Add():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestSources_Delete(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     []chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Delete source",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					DeleteF: func(ctx context.Context, s chronograf.Source) error {
						return nil
					},
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					ID:           1229,
					Name:         "howdy",
					Organization: "1337",
				},
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		s := organizations.NewSourcesStore(tt.fields.SourcesStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err := s.Delete(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. SourcesStore.All() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
	}
}

func TestSources_Get(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Get Source",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1229,
							Name:         "howdy",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					ID:           1229,
					Name:         "howdy",
					Organization: "1337",
				},
			},
			want: chronograf.Source{
				ID:           1229,
				Name:         "howdy",
				Organization: "1337",
			},
		},
	}
	for _, tt := range tests {
		s := organizations.NewSourcesStore(tt.fields.SourcesStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		got, err := s.Get(tt.args.ctx, tt.args.source.ID)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. SourcesStore.Get() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. SourcesStore.Get():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}

func TestSources_Update(t *testing.T) {
	type fields struct {
		SourcesStore chronograf.SourcesStore
	}
	type args struct {
		organization string
		ctx          context.Context
		source       chronograf.Source
		name         string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     chronograf.Source
		addFirst bool
		wantErr  bool
	}{
		{
			name: "Update Source Name",
			fields: fields{
				SourcesStore: &mocks.SourcesStore{
					UpdateF: func(ctx context.Context, s chronograf.Source) error {
						return nil
					},
					GetF: func(ctx context.Context, id int) (chronograf.Source, error) {
						return chronograf.Source{
							ID:           1229,
							Name:         "doody",
							Organization: "1337",
						}, nil
					},
				},
			},
			args: args{
				organization: "1337",
				ctx:          context.Background(),
				source: chronograf.Source{
					ID:           1229,
					Name:         "howdy",
					Organization: "1337",
				},
				name: "doody",
			},
			want: chronograf.Source{
				Name:         "doody",
				Organization: "1337",
			},
			addFirst: true,
		},
	}
	for _, tt := range tests {
		if tt.args.name != "" {
			tt.args.source.Name = tt.args.name
		}
		s := organizations.NewSourcesStore(tt.fields.SourcesStore, tt.args.organization)
		tt.args.ctx = context.WithValue(tt.args.ctx, "organizationID", tt.args.organization)
		err := s.Update(tt.args.ctx, tt.args.source)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. SourcesStore.Update() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		got, err := s.Get(tt.args.ctx, tt.args.source.ID)
		if diff := cmp.Diff(got, tt.want, sourceCmpOptions...); diff != "" {
			t.Errorf("%q. SourcesStore.Update():\n-got/+want\ndiff %s", tt.name, diff)
		}
	}
}
