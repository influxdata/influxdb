package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
)

const (
	targetOneID   = "020f755c3c082000"
	targetTwoID   = "020f755c3c082001"
	targetThreeID = "020f755c3c082002"
)

var (
	target1 = influxdb.ScraperTarget{
		Name:     "name1",
		Type:     influxdb.PrometheusScraperType,
		OrgID:    idOne,
		BucketID: idOne,
		URL:      "url1",
		ID:       MustIDBase16(targetOneID),
	}
	target2 = influxdb.ScraperTarget{
		Name:     "name2",
		Type:     influxdb.PrometheusScraperType,
		OrgID:    idTwo,
		BucketID: idTwo,
		URL:      "url2",
		ID:       MustIDBase16(targetTwoID),
	}
	target3 = influxdb.ScraperTarget{
		Name:     "name3",
		Type:     influxdb.PrometheusScraperType,
		OrgID:    idOne,
		BucketID: idThree,
		URL:      "url3",
		ID:       MustIDBase16(targetThreeID),
	}
	newOrg = func(id influxdb.ID) *influxdb.Organization {
		return &influxdb.Organization{
			ID:   id,
			Name: fmt.Sprintf("org%d", int(id)),
		}
	}
)

// TargetFields will include the IDGenerator, and targets
type TargetFields struct {
	IDGenerator   influxdb.IDGenerator
	Targets       []*influxdb.ScraperTarget
	Organizations []*influxdb.Organization
}

var targetCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []influxdb.ScraperTarget) []influxdb.ScraperTarget {
		out := append([]influxdb.ScraperTarget(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// ScraperService tests all the service functions.
func ScraperService(
	init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()), t *testing.T,
) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
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
			tt := tt
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// AddTarget testing.
func AddTarget(
	init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
	t *testing.T,
) {
	t.Helper()
	type args struct {
		userID influxdb.ID
		target *influxdb.ScraperTarget
	}
	type wants struct {
		err     error
		targets []influxdb.ScraperTarget
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
				IDGenerator:   mock.NewIDGenerator(targetOneID, t),
				Targets:       []*influxdb.ScraperTarget{},
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
			},
			args: args{
				userID: MustIDBase16(threeID),
				target: &influxdb.ScraperTarget{
					Name:     "name1",
					Type:     influxdb.PrometheusScraperType,
					OrgID:    idOne,
					BucketID: idOne,
					URL:      "url1",
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
		},
		{
			name: "create target with invalid org id",
			fields: TargetFields{
				IDGenerator:   mock.NewIDGenerator(targetTwoID, t),
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
			args: args{
				target: &influxdb.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "name2",
					Type:     influxdb.PrometheusScraperType,
					BucketID: idTwo,
					URL:      "url2",
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "provided organization ID has invalid format",
					Op:   influxdb.OpAddTarget,
				},
				targets: []influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
		},
		{
			name: "create target with invalid bucket id",
			fields: TargetFields{
				IDGenerator:   mock.NewIDGenerator(targetTwoID, t),
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
			},
			args: args{
				target: &influxdb.ScraperTarget{
					ID:    MustIDBase16(targetTwoID),
					Name:  "name2",
					Type:  influxdb.PrometheusScraperType,
					OrgID: idTwo,
					URL:   "url2",
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "provided bucket ID has invalid format",
					Op:   influxdb.OpAddTarget,
				},
				targets: []influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
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
				Targets: []*influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
				},
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1)), newOrg(influxdb.ID(2))},
			},
			args: args{
				userID: MustIDBase16(threeID),
				target: &influxdb.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "name2",
					Type:     influxdb.PrometheusScraperType,
					OrgID:    idTwo,
					BucketID: idTwo,
					URL:      "url2",
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					{
						Name:     "name1",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idOne,
						BucketID: idOne,
						URL:      "url1",
						ID:       MustIDBase16(targetOneID),
					},
					{
						Name:     "name2",
						Type:     influxdb.PrometheusScraperType,
						OrgID:    idTwo,
						BucketID: idTwo,
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
			err := s.AddTarget(ctx, tt.args.target, tt.args.userID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)
			defer s.RemoveTarget(ctx, tt.args.target.ID)

			targets, err := s.ListTargets(ctx, influxdb.ScraperTargetFilter{})
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
	init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.ScraperTargetFilter
	}
	type wants struct {
		targets []influxdb.ScraperTarget
		err     error
	}

	tests := []struct {
		name   string
		fields TargetFields
		args   args
		wants  wants
	}{
		{
			name: "get all targets",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
					newOrg(influxdb.ID(2)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					target1,
					target2,
					target3,
				},
			},
		},
		{
			name: "filter by name",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
					newOrg(influxdb.ID(2)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{
					Name: strPtr(target2.Name),
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					target2,
				},
			},
		},
		{
			name: "filter by id",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
					newOrg(influxdb.ID(2)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{
					IDs: map[influxdb.ID]bool{target2.ID: false},
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					target2,
				},
			},
		},
		{
			name: "filter targets by orgID",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
					newOrg(influxdb.ID(2)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{
					OrgID: idPtr(idOne),
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					target1,
					target3,
				},
			},
		},
		{
			name: "filter targets by org name",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
					newOrg(influxdb.ID(2)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{
					Org: strPtr("org1"),
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					target1,
					target3,
				},
			},
		},
		{
			name: "filter targets by org name not exist",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{
					newOrg(influxdb.ID(1)),
				},
				Targets: []*influxdb.ScraperTarget{
					&target1,
					&target2,
					&target3,
				},
			},
			args: args{
				filter: influxdb.ScraperTargetFilter{
					Org: strPtr("org2"),
				},
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  `organization name "org2" not found`,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			targets, err := s.ListTargets(ctx, tt.args.filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(targets, tt.wants.targets, targetCmpOptions...); diff != "" {
				t.Errorf("targets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// GetTargetByID testing
func GetTargetByID(
	init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
	t *testing.T,
) {
	t.Helper()
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err    error
		target *influxdb.ScraperTarget
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
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						Name:     "target1",
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						Name:     "target2",
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				id: MustIDBase16(targetTwoID),
			},
			wants: wants{
				target: &influxdb.ScraperTarget{
					ID:       MustIDBase16(targetTwoID),
					Name:     "target2",
					OrgID:    idOne,
					BucketID: idOne,
				},
			},
		},
		{
			name: "find target by id not find",
			fields: TargetFields{
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						Name:     "target1",
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						Name:     "target2",
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpGetTargetByID,
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
func RemoveTarget(init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
	t *testing.T) {
	type args struct {
		ID     influxdb.ID
		userID influxdb.ID
	}
	type wants struct {
		err     error
		targets []influxdb.ScraperTarget
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
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				ID:     MustIDBase16(targetOneID),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				targets: []influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
		},
		{
			name: "delete targets using id that does not exist",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				ID:     MustIDBase16(targetThreeID),
				userID: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpRemoveTarget,
					Msg:  "scraper target is not found",
				},
				targets: []influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						OrgID:    idOne,
						BucketID: idOne,
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

			targets, err := s.ListTargets(ctx, influxdb.ScraperTargetFilter{})
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
	init func(TargetFields, *testing.T) (influxdb.ScraperTargetStoreService, string, func()),
	t *testing.T,
) {
	type args struct {
		url    string
		userID influxdb.ID
		id     influxdb.ID
	}
	type wants struct {
		err    error
		target *influxdb.ScraperTarget
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
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				url: "changed",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Op:   influxdb.OpUpdateTarget,
					Msg:  "provided scraper target ID has invalid format",
				},
			},
		},
		{
			name: "update url with non exist id",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				id:  MustIDBase16(targetThreeID),
				url: "changed",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpUpdateTarget,
					Msg:  "scraper target is not found",
				},
			},
		},
		{
			name: "update url",
			fields: TargetFields{
				Organizations: []*influxdb.Organization{newOrg(influxdb.ID(1))},
				Targets: []*influxdb.ScraperTarget{
					{
						ID:       MustIDBase16(targetOneID),
						URL:      "url1",
						OrgID:    idOne,
						BucketID: idOne,
					},
					{
						ID:       MustIDBase16(targetTwoID),
						URL:      "url2",
						OrgID:    idOne,
						BucketID: idOne,
					},
				},
			},
			args: args{
				id:  MustIDBase16(targetOneID),
				url: "changed",
			},
			wants: wants{
				target: &influxdb.ScraperTarget{
					ID:       MustIDBase16(targetOneID),
					URL:      "changed",
					OrgID:    idOne,
					BucketID: idOne,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := &influxdb.ScraperTarget{
				ID:  tt.args.id,
				URL: tt.args.url,
			}

			target, err := s.UpdateTarget(ctx, upd, tt.args.userID)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(target, tt.wants.target, targetCmpOptions...); diff != "" {
				t.Errorf("scraper target is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
