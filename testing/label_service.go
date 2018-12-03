package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
)

var labelCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Label) []*platform.Label {
		out := append([]*platform.Label(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ResourceID.String() > out[j].ResourceID.String()
		})
		return out
	}),
}

type LabelFields struct {
	Labels []*platform.Label
}

type labelServiceF func(
	init func(LabelFields, *testing.T) (platform.LabelService, func()),
	t *testing.T,
)

// LabelService tests all the service functions.
func LabelService(
	init func(LabelFields, *testing.T) (platform.LabelService, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   labelServiceF
	}{
		{
			name: "CreateLabel",
			fn:   CreateLabel,
		},
		{
			name: "FindLabels",
			fn:   FindLabels,
		},
		{
			name: "DeleteLabel",
			fn:   DeleteLabel,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

func CreateLabel(
	init func(LabelFields, *testing.T) (platform.LabelService, func()),
	t *testing.T,
) {
	type args struct {
		label *platform.Label
	}
	type wants struct {
		err    error
		labels []*platform.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic create label",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
				},
			},
			args: args{
				label: &platform.Label{
					ResourceID: MustIDBase16(bucketOneID),
					Name:       "Tag2",
				},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
				},
			},
		},
		{
			name: "duplicate labels fail",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
				},
			},
			args: args{
				label: &platform.Label{
					ResourceID: MustIDBase16(bucketOneID),
					Name:       "Tag1",
				},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
				},
				err: fmt.Errorf("label Tag1 already exists"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateLabel(ctx, tt.args.label)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteLabel(ctx, *tt.args.label)

			labels, err := s.FindLabels(ctx, platform.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, mappingCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindLabels(
	init func(LabelFields, *testing.T) (platform.LabelService, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.LabelFilter
	}
	type wants struct {
		err    error
		labels []*platform.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic find labels",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
				},
			},
			args: args{
				filter: platform.LabelFilter{},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
				},
			},
		},
		{
			name: "find labels filtering",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
					{
						ResourceID: MustIDBase16(bucketTwoID),
						Name:       "Tag1",
					},
				},
			},
			args: args{
				filter: platform.LabelFilter{
					ResourceID: MustIDBase16(bucketOneID),
					Name:       "Tag1",
				},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
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
			labels, err := s.FindLabels(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(labels, tt.wants.labels, mappingCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteLabel(
	init func(LabelFields, *testing.T) (platform.LabelService, func()),
	t *testing.T,
) {
	type args struct {
		label platform.Label
	}
	type wants struct {
		err    error
		labels []*platform.Label
	}

	tests := []struct {
		name   string
		fields LabelFields
		args   args
		wants  wants
	}{
		{
			name: "basic delete label",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
				},
			},
			args: args{
				label: platform.Label{
					ResourceID: MustIDBase16(bucketOneID),
					Name:       "Tag1",
				},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag2",
					},
				},
			},
		},
		{
			name: "deleting a non-existant label",
			fields: LabelFields{
				Labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
				},
			},
			args: args{
				label: platform.Label{
					ResourceID: MustIDBase16(bucketOneID),
					Name:       "Tag2",
				},
			},
			wants: wants{
				labels: []*platform.Label{
					{
						ResourceID: MustIDBase16(bucketOneID),
						Name:       "Tag1",
					},
				},
				err: fmt.Errorf("label not found"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteLabel(ctx, tt.args.label)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			labels, err := s.FindLabels(ctx, platform.LabelFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve labels: %v", err)
			}
			if diff := cmp.Diff(labels, tt.wants.labels, labelCmpOptions...); diff != "" {
				t.Errorf("labels are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
