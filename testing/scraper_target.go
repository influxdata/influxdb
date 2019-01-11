package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	targetOneID   = "020f755c3c082000"
	targetTwoID   = "020f755c3c082001"
	targetThreeID = "020f755c3c082002"
)

// TargetFields will include the IDGenerator, and targets
type TargetFields struct {
	IDGenerator platform.IDGenerator
	Targets     []*platform.ScraperTarget
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

// ScraperService tests all the service functions.
func ScraperService(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
			t *testing.T)
	}{
		{
			name: "AddTarget",
			fn:   AddTarget,
		},
		{
			name: "ListTargets",
			fn:   ListTargets,
		},
		{
			name: "GetTargetByID",
			fn:   GetTargetByID,
		},
		{
			name: "RemoveTarget",
			fn:   RemoveTarget,
		},
		{
			name: "UpdateTarget",
			fn:   UpdateTarget,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// AddTarget testing.
func AddTarget(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
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
				Targets:     []*platform.ScraperTarget{},
			},
			args: args{
				target: &platform.ScraperTarget{
					Name:     "name1",
					Type:     platform.PrometheusScraperType,
					OrgID:    MustIDBase16(orgOneID),
					BucketID: MustIDBase16(bucketOneID),
					URL:      "url1",
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
		},
		{
			name: "create target with invalid org id",
			fields: TargetFields{
				IDGenerator: mock.NewIDGenerator(targetTwoID, t),
				Targets: []*platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
			args: args{
				target: &platform.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "name2",
					Type:     platform.PrometheusScraperType,
					BucketID: MustIDBase16(bucketTwoID),
					URL:      "url2",
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInvalid,
					Msg:  "org id is invalid",
					Op:   platform.OpAddTarget,
				},
				targets: []platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
		},
		{
			name: "create target with invalid bucket id",
			fields: TargetFields{
				IDGenerator: mock.NewIDGenerator(targetTwoID, t),
				Targets: []*platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
			args: args{
				target: &platform.ScraperTarget{
					ID:    MustIDBase16(targetTwoID),
					Name:  "name2",
					Type:  platform.PrometheusScraperType,
					OrgID: MustIDBase16(orgTwoID),
					URL:   "url2",
				},
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInvalid,
					Msg:  "bucket id is invalid",
					Op:   platform.OpAddTarget,
				},
				targets: []platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
		},
		{
			name: "basic create target",
			fields: TargetFields{
				IDGenerator: mock.NewIDGenerator(targetTwoID, t),
				Targets: []*platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
			args: args{
				target: &platform.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "name2",
					Type:     platform.PrometheusScraperType,
					OrgID:    MustIDBase16(orgTwoID),
					BucketID: MustIDBase16(bucketTwoID),
					URL:      "url2",
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
					{
						Name:     "name2",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgTwoID),
						BucketID: MustIDBase16(bucketTwoID),
						URL:      "url2",
						ID:       MustIDBase16(targetTwoID),
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
			err := s.AddTarget(ctx, tt.args.target)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)
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
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
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
				Targets: []*platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
					{
						Name:     "name2",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgTwoID),
						BucketID: MustIDBase16(bucketTwoID),
						URL:      "url2",
						ID:       MustIDBase16(targetTwoID),
					},
				},
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						Name:     "name1",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
					{
						Name:     "name2",
						Type:     platform.PrometheusScraperType,
						OrgID:    MustIDBase16(orgTwoID),
						BucketID: MustIDBase16(bucketTwoID),
						URL:      "url2",
						ID:       MustIDBase16(targetTwoID),
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
			targets, err := s.ListTargets(ctx)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(targets, tt.wants.targets, targetCmpOptions...); diff != "" {
				t.Errorf("targets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// GetTargetByID testing
func GetTargetByID(
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
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
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						Name:     "target1",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						Name:     "target2",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(targetTwoID),
			},
			wants: wants{
				target: &platform.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "target2",
					OrgID:    MustIDBase16(orgOneID),
					BucketID: MustIDBase16(bucketOneID),
				},
			},
		},
		{
			name: "find target by id not find",
			fields: TargetFields{
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						Name:     "target1",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						Name:     "target2",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpGetTargetByID,
					Msg:  "scraper target is not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			target, err := s.GetTargetByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(target, tt.wants.target, targetCmpOptions...); diff != "" {
				t.Errorf("target is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// RemoveTarget testing
func RemoveTarget(init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
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
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(targetOneID),
			},
			wants: wants{
				targets: []platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
		},
		{
			name: "delete targets using id that does not exist",
			fields: TargetFields{
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				ID: MustIDBase16(targetThreeID),
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpRemoveTarget,
					Msg:  "scraper target is not found",
				},
				targets: []platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
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
			err := s.RemoveTarget(ctx, tt.args.ID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

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
	init func(TargetFields, *testing.T) (platform.ScraperTargetStoreService, string, func()),
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
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				url: "changed",
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.EInvalid,
					Op:   platform.OpUpdateTarget,
					Msg:  "id is invalid",
				},
			},
		},
		{
			name: "update url with non exist id",
			fields: TargetFields{
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				id:  MustIDBase16(targetThreeID),
				url: "changed",
			},
			wants: wants{
				err: &platform.Error{
					Code: platform.ENotFound,
					Op:   platform.OpUpdateTarget,
					Msg:  "scraper target is not found",
				},
			},
		},
		{
			name: "update url",
			fields: TargetFields{
				Targets: []*platform.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    MustIDBase16(orgOneID),
						BucketID: MustIDBase16(bucketOneID),
					},
				},
			},
			args: args{
				id:  MustIDBase16(targetOneID),
				url: "changed",
			},
			wants: wants{
				target: &platform.ScraperTarget{
					ID:       MustIDBase16(targetOneID),
					URL:      "changed",
					OrgID:    MustIDBase16(orgOneID),
					BucketID: MustIDBase16(bucketOneID),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := &platform.ScraperTarget{
				ID:  tt.args.id,
				URL: tt.args.url,
			}

			target, err := s.UpdateTarget(ctx, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(target, tt.wants.target, targetCmpOptions...); diff != "" {
				t.Errorf("scraper target is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
