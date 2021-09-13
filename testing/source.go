package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb/v2"
	platform2 "github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	defaultSourceID             = "020f755c3c082000"
	defaultSourceOrganizationID = "50616e67652c206c"
	sourceOneID                 = "020f755c3c082001"
	sourceTwoID                 = "020f755c3c082002"
	sourceOrgOneID              = "61726920617a696f"
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
	IDGenerator platform2.IDGenerator
	Sources     []*platform.Source
}

// CreateSource testing
func CreateSource(
	init func(SourceFields, *testing.T) (platform.SourceService, string, func()),
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
						Name:           "autogen",
						Type:           "self",
						ID:             MustIDBase16(defaultSourceID),
						OrganizationID: MustIDBase16(defaultSourceOrganizationID),
						Default:        true,
					},
					{
						Name:           "name1",
						ID:             MustIDBase16(sourceOneID),
						OrganizationID: MustIDBase16(sourceOneID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateSource(ctx, tt.args.source)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)
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
	init func(SourceFields, *testing.T) (platform.SourceService, string, func()),
	t *testing.T,
) {
	type args struct {
		id platform2.ID
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
						Name:           "name1",
						ID:             MustIDBase16(sourceOneID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(sourceOneID),
			},
			wants: wants{
				source: &platform.Source{
					Name:           "name1",
					ID:             MustIDBase16(sourceOneID),
					OrganizationID: MustIDBase16(sourceOrgOneID),
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
				id: MustIDBase16(defaultSourceID),
			},
			wants: wants{
				source: &platform.Source{
					Name:           "autogen",
					Type:           "self",
					ID:             MustIDBase16(defaultSourceID),
					OrganizationID: MustIDBase16(defaultSourceOrganizationID),
					Default:        true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			source, err := s.FindSourceByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(source, tt.wants.source, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindSources testing
func FindSources(
	init func(SourceFields, *testing.T) (platform.SourceService, string, func()),
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
						Name:           "name1",
						ID:             MustIDBase16(sourceOneID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
					{
						Name:           "name2",
						ID:             MustIDBase16(sourceTwoID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
				},
			},
			args: args{},
			wants: wants{
				sources: []*platform.Source{
					{
						Name:           "autogen",
						Type:           "self",
						ID:             MustIDBase16(defaultSourceID),
						OrganizationID: MustIDBase16(defaultSourceOrganizationID),
						Default:        true,
					},
					{
						Name:           "name1",
						ID:             MustIDBase16(sourceOneID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
					{
						Name:           "name2",
						ID:             MustIDBase16(sourceTwoID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			sources, _, err := s.FindSources(ctx, tt.args.opts)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(sources, tt.wants.sources, sourceCmpOptions...); diff != "" {
				t.Errorf("sources are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteSource testing
func DeleteSource(
	init func(SourceFields, *testing.T) (platform.SourceService, string, func()),
	t *testing.T,
) {
	type args struct {
		id platform2.ID
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
						Name:           "name1",
						ID:             MustIDBase16(sourceOneID),
						OrganizationID: MustIDBase16(sourceOrgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(sourceOneID),
			},
			wants: wants{
				sources: []*platform.Source{
					{
						Name:           "autogen",
						Type:           "self",
						ID:             MustIDBase16(defaultSourceID),
						OrganizationID: MustIDBase16(defaultSourceOrganizationID),
						Default:        true,
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
				id: MustIDBase16(defaultSourceID),
			},
			wants: wants{
				err: &errors.Error{
					Code: errors.EForbidden,
					Op:   platform.OpDeleteSource,
					Msg:  "cannot delete autogen source",
				},
				sources: []*platform.Source{
					{
						Name:           "autogen",
						Type:           "self",
						ID:             MustIDBase16(defaultSourceID),
						OrganizationID: MustIDBase16(defaultSourceOrganizationID),
						Default:        true,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteSource(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

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
