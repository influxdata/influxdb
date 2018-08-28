package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

const (
	defaultSourceID = "020f755c3c082000"
	sourceOneID     = "020f755c3c082001"
	sourceTwoID     = "020f755c3c082002"
	sourceThreeID   = "020f755c3c082003"
)

var sourceCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Source) []*platform.Source {
		out := append([]*platform.Source(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// SourceFields will include the IDGenerator, and sources
type SourceFields struct {
	IDGenerator platform.IDGenerator
	Sources     []*platform.Source
}

// CreateSource testing
func CreateSource(
	init func(SourceFields, *testing.T) (platform.SourceService, func()),
	t *testing.T,
) {
	type args struct {
		source *platform.Source
	}
	type wants struct {
		err     error
		sources []*platform.Source
	}

	tests := []struct {
		name   string
		fields SourceFields
		args   args
		wants  wants
	}{
		{
			name: "create sources with empty set",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources:     []*platform.Source{},
			},
			args: args{
				source: &platform.Source{
					Name: "name1",
				},
			},
			wants: wants{
				sources: []*platform.Source{
					{
						Name:    "autogen",
						Type:    "self",
						ID:      idFromString(t, defaultSourceID),
						Default: true,
					},
					{
						Name: "name1",
						ID:   idFromString(t, sourceOneID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.CreateSource(ctx, tt.args.source)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteSource(ctx, tt.args.source.ID)

			sources, _, err := s.FindSources(ctx, platform.FindOptions{})
			if err != nil {
				t.Fatalf("failed to retrieve sources: %v", err)
			}
			if diff := cmp.Diff(sources, tt.wants.sources, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindSourceByID testing
func FindSourceByID(
	init func(SourceFields, *testing.T) (platform.SourceService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err    error
		source *platform.Source
	}

	tests := []struct {
		name   string
		fields SourceFields
		args   args
		wants  wants
	}{
		{
			name: "find default source by ID",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources: []*platform.Source{
					{
						Name: "name1",
						ID:   idFromString(t, sourceOneID),
					},
				},
			},
			args: args{
				id: idFromString(t, sourceOneID),
			},
			wants: wants{
				source: &platform.Source{
					Name: "name1",
					ID:   idFromString(t, sourceOneID),
				},
			},
		},
		{
			name: "find source by ID",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources:     []*platform.Source{},
			},
			args: args{
				id: idFromString(t, defaultSourceID),
			},
			wants: wants{
				source: &platform.Source{
					Name:    "autogen",
					Type:    "self",
					ID:      idFromString(t, defaultSourceID),
					Default: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			source, err := s.FindSourceByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(source, tt.wants.source, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindSources testing
func FindSources(
	init func(SourceFields, *testing.T) (platform.SourceService, func()),
	t *testing.T,
) {
	type args struct {
		opts platform.FindOptions
	}
	type wants struct {
		err     error
		sources []*platform.Source
	}

	tests := []struct {
		name   string
		fields SourceFields
		args   args
		wants  wants
	}{
		{
			name: "find all sources",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources: []*platform.Source{
					{
						Name: "name1",
						ID:   idFromString(t, sourceOneID),
					},
					{
						Name: "name2",
						ID:   idFromString(t, sourceTwoID),
					},
				},
			},
			args: args{},
			wants: wants{
				sources: []*platform.Source{
					{
						Name:    "autogen",
						Type:    "self",
						ID:      idFromString(t, defaultSourceID),
						Default: true,
					},
					{
						Name: "name1",
						ID:   idFromString(t, sourceOneID),
					},
					{
						Name: "name2",
						ID:   idFromString(t, sourceTwoID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			sources, _, err := s.FindSources(ctx, tt.args.opts)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(sources, tt.wants.sources, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteSource testing
func DeleteSource(
	init func(SourceFields, *testing.T) (platform.SourceService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err     error
		sources []*platform.Source
	}

	tests := []struct {
		name   string
		fields SourceFields
		args   args
		wants  wants
	}{
		{
			name: "delete source by ID",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources: []*platform.Source{
					{
						Name: "name1",
						ID:   idFromString(t, sourceOneID),
					},
				},
			},
			args: args{
				id: idFromString(t, sourceOneID),
			},
			wants: wants{
				sources: []*platform.Source{
					{
						Name:    "autogen",
						Type:    "self",
						ID:      idFromString(t, defaultSourceID),
						Default: true,
					},
				},
			},
		},
		{
			name: "delete default source by ID",
			fields: SourceFields{
				IDGenerator: mock.NewIDGenerator(sourceOneID, t),
				Sources:     []*platform.Source{},
			},
			args: args{
				id: idFromString(t, defaultSourceID),
			},
			wants: wants{
				err: fmt.Errorf("cannot delete autogen source"),
				sources: []*platform.Source{
					{
						Name:    "autogen",
						Type:    "self",
						ID:      idFromString(t, defaultSourceID),
						Default: true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteSource(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			sources, _, err := s.FindSources(ctx, platform.FindOptions{})
			if err != nil {
				t.Fatalf("failed to retrieve sources: %v", err)
			}
			if diff := cmp.Diff(sources, tt.wants.sources, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
