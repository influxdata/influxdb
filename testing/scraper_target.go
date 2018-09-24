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
	"github.com/pkg/errors"
)

const (
	targetOneID   = "020f755c3c082000"
	targetTwoID   = "020f755c3c082001"
	targetThreeID = "020f755c3c082002"
)

// TargetFields will include the IDGenerator, and targets
type TargetFields struct {
	IDGenerator platform.IDGenerator
	Targets     []platform.ScraperTarget
}

var targetCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []platform.ScraperTarget) []platform.ScraperTarget {
		out := append([]platform.ScraperTarget(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// AddTarget testing.
func AddTarget(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, func()),
	t *testing.T,
) {
	type args struct {
		target *platform.ScraperTarget
	}
	type wants struct {
		err     error
		targets []platform.ScraperTarget
	}
	tests := []struct {
		name   string
		fields TargetFields
		args   args
		wants  wants
	}{
		{
			name: "create targets with empty set",
			fields: TargetFields{
				IDGenerator: mock.NewIDGenerator(targetOneID, t),
				Targets:     []platform.ScraperTarget{},
			},
			args: args{
				target: &platform.ScraperTarget{
					Name:       "name1",
					Type:       platform.PrometheusScraperType,
					OrgName:    "org1",
					BucketName: "bucket1",
					URL:        "url1",
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:       "name1",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org1",
						BucketName: "bucket1",
						URL:        "url1",
						ID:         MustIDFromString(targetOneID),
					},
				},
			},
		},
		{
			name: "basic create target",
			fields: TargetFields{
				IDGenerator: mock.NewIDGenerator(targetTwoID, t),
				Targets: []platform.ScraperTarget{
					{
						Name:       "name1",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org1",
						BucketName: "bucket1",
						URL:        "url1",
						ID:         MustIDFromString(targetOneID),
					},
				},
			},
			args: args{
				target: &platform.ScraperTarget{
					Name:       "name2",
					Type:       platform.PrometheusScraperType,
					OrgName:    "org2",
					BucketName: "bucket2",
					URL:        "url2",
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:       "name1",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org1",
						BucketName: "bucket1",
						URL:        "url1",
						ID:         MustIDFromString(targetOneID),
					},
					{
						Name:       "name2",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org2",
						BucketName: "bucket2",
						URL:        "url2",
						ID:         MustIDFromString(targetTwoID),
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
			err := s.AddTarget(ctx, tt.args.target)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.RemoveTarget(ctx, tt.args.target.ID)

			targets, err := s.ListTargets(ctx)
			if err != nil {
				t.Fatalf("failed to retrieve scraper targets: %v", err)
			}
			if diff := cmp.Diff(targets, tt.wants.targets, targetCmpOptions...); diff != "" {
				t.Errorf("scraper targets are different -got/+want\ndiff %s", diff)
			}
		})

	}
}

// ListTargets testing
func ListTargets(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, func()),
	t *testing.T,
) {

	type wants struct {
		targets []platform.ScraperTarget
		err     error
	}

	tests := []struct {
		name   string
		fields TargetFields
		wants  wants
	}{
		{
			name: "find all targets",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						Name:       "name1",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org1",
						BucketName: "bucket1",
						URL:        "url1",
						ID:         MustIDFromString(targetOneID),
					},
					{
						Name:       "name2",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org2",
						BucketName: "bucket2",
						URL:        "url2",
						ID:         MustIDFromString(targetTwoID),
					},
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:       "name1",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org1",
						BucketName: "bucket1",
						URL:        "url1",
						ID:         MustIDFromString(targetOneID),
					},
					{
						Name:       "name2",
						Type:       platform.PrometheusScraperType,
						OrgName:    "org2",
						BucketName: "bucket2",
						URL:        "url2",
						ID:         MustIDFromString(targetTwoID),
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
			targets, err := s.ListTargets(ctx)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(targets, tt.wants.targets, targetCmpOptions...); diff != "" {
				t.Errorf("targets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// GetTargetByID testing
func GetTargetByID(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err    error
		target *platform.ScraperTarget
	}

	tests := []struct {
		name   string
		fields TargetFields
		args   args
		wants  wants
	}{
		{
			name: "basic find target by id",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID:   MustIDFromString(targetOneID),
						Name: "target1",
					},
					{
						ID:   MustIDFromString(targetTwoID),
						Name: "target2",
					},
				},
			},
			args: args{
				id: MustIDFromString(targetTwoID),
			},
			wants: wants{
				target: &platform.ScraperTarget{
					ID:   MustIDFromString(targetTwoID),
					Name: "target2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			target, err := s.GetTargetByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(target, tt.wants.target, targetCmpOptions...); diff != "" {
				t.Errorf("target is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// RemoveTarget testing
func RemoveTarget(init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, func()),
	t *testing.T) {
	type args struct {
		ID platform.ID
	}
	type wants struct {
		err     error
		targets []platform.ScraperTarget
	}
	tests := []struct {
		name   string
		fields TargetFields
		args   args
		wants  wants
	}{
		{
			name: "delete targets using exist id",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID: MustIDFromString(targetOneID),
					},
					{
						ID: MustIDFromString(targetTwoID),
					},
				},
			},
			args: args{
				ID: MustIDFromString(targetOneID),
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						ID: MustIDFromString(targetTwoID),
					},
				},
			},
		},
		{
			name: "delete targets using id that does not exist",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID: MustIDFromString(targetOneID),
					},
					{
						ID: MustIDFromString(targetTwoID),
					},
				},
			},
			args: args{
				ID: MustIDFromString(targetThreeID),
			},
			wants: wants{
				err: fmt.Errorf("scraper target is not found"),
				targets: []platform.ScraperTarget{
					{
						ID: MustIDFromString(targetOneID),
					},
					{
						ID: MustIDFromString(targetTwoID),
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
			err := s.RemoveTarget(ctx, tt.args.ID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			targets, err := s.ListTargets(ctx)
			if err != nil {
				t.Fatalf("failed to retrieve targets: %v", err)
			}
			if diff := cmp.Diff(targets, tt.wants.targets, targetCmpOptions...); diff != "" {
				t.Errorf("targets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateTarget testing
func UpdateTarget(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, func()),
	t *testing.T,
) {
	type args struct {
		url string
		id  platform.ID
	}
	type wants struct {
		err    error
		target *platform.ScraperTarget
	}

	tests := []struct {
		name   string
		fields TargetFields
		args   args
		wants  wants
	}{
		{
			name: "update url with blank id",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID:  MustIDFromString(targetOneID),
						URL: "url1",
					},
					{
						ID:  MustIDFromString(targetTwoID),
						URL: "url2",
					},
				},
			},
			args: args{
				url: "changed",
			},
			wants: wants{
				err: errors.New("update scraper: id is empty"),
			},
		},
		{
			name: "update url with non exist id",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID:  MustIDFromString(targetOneID),
						URL: "url1",
					},
					{
						ID:  MustIDFromString(targetTwoID),
						URL: "url2",
					},
				},
			},
			args: args{
				id:  MustIDFromString(targetThreeID),
				url: "changed",
			},
			wants: wants{
				err: errors.New("scraper target is not found"),
			},
		},
		{
			name: "update url",
			fields: TargetFields{
				Targets: []platform.ScraperTarget{
					{
						ID:  MustIDFromString(targetOneID),
						URL: "url1",
					},
					{
						ID:  MustIDFromString(targetTwoID),
						URL: "url2",
					},
				},
			},
			args: args{
				id:  MustIDFromString(targetOneID),
				url: "changed",
			},
			wants: wants{
				target: &platform.ScraperTarget{
					ID:  MustIDFromString(targetOneID),
					URL: "changed",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			upd := &platform.ScraperTarget{
				ID:  tt.args.id,
				URL: tt.args.url,
			}

			target, err := s.UpdateTarget(ctx, upd)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(target, tt.wants.target, targetCmpOptions...); diff != "" {
				t.Errorf("scraper target is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
