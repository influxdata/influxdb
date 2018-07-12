package testing

import (
	"bytes"
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/pkg/errors"
)

var dbrpMappingCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.DBRPMapping) []*platform.DBRPMapping {
		out := make([]*platform.DBRPMapping, len(in))
		copy(out, in) // Copy input slice to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			if out[i].Cluster != out[j].Cluster {
				return out[i].Cluster < out[j].Cluster
			}
			if out[i].Database != out[j].Database {
				return out[i].Database < out[j].Database
			}
			return out[i].RetentionPolicy < out[j].RetentionPolicy
		})
		return out
	}),
}

// DBRPMappingFields will include the dbrpMappings
type DBRPMappingFields struct {
	DBRPMappings []*platform.DBRPMapping
}

// Populate creates all entities in DBRPMappingFields
func (f DBRPMappingFields) Populate(ctx context.Context, s platform.DBRPMappingService) error {
	for _, m := range f.DBRPMappings {
		if err := s.Create(ctx, m); err != nil {
			return errors.Wrap(err, "failed to populate dbrp mappings")
		}
	}
	return nil
}

// CleanupDBRPMappings finds and removes all dbrp mappings
func CleanupDBRPMappings(ctx context.Context, s platform.DBRPMappingService) error {
	mappings, _, err := s.FindMany(ctx, platform.DBRPMappingFilter{})
	if err != nil {
		return errors.Wrap(err, "failed to retrieve all dbrp mappings")
	}

	for _, m := range mappings {
		if err := s.Delete(ctx, m.Cluster, m.Database, m.RetentionPolicy); err != nil {
			return errors.Wrapf(err, "failed to remove dbrp mapping %s/%s/%s", m.Cluster, m.Database, m.RetentionPolicy)
		}
	}
	return nil
}

// CreateDBRPMapping testing
func CreateDBRPMapping(
	init func(DBRPMappingFields, *testing.T) (platform.DBRPMappingService, func()),
	t *testing.T,
) {
	type args struct {
		dbrpMapping *platform.DBRPMapping
	}
	type wants struct {
		err          error
		dbrpMappings []*platform.DBRPMapping
	}

	tests := []struct {
		name   string
		fields DBRPMappingFields
		args   args
		wants  wants
	}{
		{
			name: "create dbrpMappings with empty set",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{},
			},
			args: args{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				}},
			},
		},
		{
			name: "basic create dbrpMapping",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				}},
			},
			args: args{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster2",
					Database:        "database2",
					RetentionPolicy: "retention_policy2",
					Default:         true,
					OrganizationID:  platform.ID("org2"),
					BucketID:        platform.ID("bucket2"),
				},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
		},
		{
			name: "idempotent create dbrpMapping",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				}},
			},
			args: args{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
				},
			},
		},
		{
			name: "error on create existing dbrpMapping",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         false,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				}},
			},
			args: args{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster1",
					Database:        "database1",
					RetentionPolicy: "retention_policy1",
					Default:         true,
					OrganizationID:  platform.ID("org1"),
					BucketID:        platform.ID("bucket1"),
				},
			},
			wants: wants{
				err: errors.New("dbrp mapping already exists"),
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
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
			err := s.Create(ctx, tt.args.dbrpMapping)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			dbrpMappings, _, err := s.FindMany(ctx, platform.DBRPMappingFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve dbrpMappings: %v", err)
			}
			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptions...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDBRPMappings testing
func FindDBRPMappings(
	init func(DBRPMappingFields, *testing.T) (platform.DBRPMappingService, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.DBRPMappingFilter
	}

	type wants struct {
		dbrpMappings []*platform.DBRPMapping
		err          error
	}
	tests := []struct {
		name   string
		fields DBRPMappingFields
		args   args
		wants  wants
	}{
		{
			name: "find all dbrpMappings",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
			args: args{
				filter: platform.DBRPMappingFilter{},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
		},
		{
			name: "find dbrpMappings by cluster",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
			args: args{
				filter: platform.DBRPMappingFilter{
					Cluster: strPtr("cluster2"),
				},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
		},
		{
			name: "find default rp from dbrpMappings",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketA"),
					},
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketB"),
					},
				},
			},
			args: args{
				filter: platform.DBRPMappingFilter{
					Cluster:  strPtr("cluster"),
					Database: strPtr("database"),
					Default:  boolPtr(true),
				},
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketB"),
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

			dbrpMappings, _, err := s.FindMany(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptions...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDBRPMappingByKey testing
func FindDBRPMappingByKey(
	init func(DBRPMappingFields, *testing.T) (platform.DBRPMappingService, func()),
	t *testing.T,
) {
	type args struct {
		Cluster,
		Database,
		RetentionPolicy string
	}

	type wants struct {
		dbrpMapping *platform.DBRPMapping
		err         error
	}

	tests := []struct {
		name   string
		fields DBRPMappingFields
		args   args
		wants  wants
	}{
		{
			name: "find dbrpMappings by cluster db and rp",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketA"),
					},
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketB"),
					},
				},
			},
			args: args{
				Cluster:         "cluster",
				Database:        "database",
				RetentionPolicy: "retention_policyB",
			},
			wants: wants{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster",
					Database:        "database",
					RetentionPolicy: "retention_policyB",
					Default:         false,
					OrganizationID:  platform.ID("org"),
					BucketID:        platform.ID("bucketB"),
				},
			},
		},
		{
			name: "find non existing dbrpMapping",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketA"),
					},
				},
			},
			args: args{
				Cluster:         "clusterX",
				Database:        "database",
				RetentionPolicy: "retention_policyA",
			},
			wants: wants{
				err: errors.New("dbrp mapping not found"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			dbrpMapping, err := s.FindBy(ctx, tt.args.Cluster, tt.args.Database, tt.args.RetentionPolicy)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(dbrpMapping, tt.wants.dbrpMapping, dbrpMappingCmpOptions...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindDBRPMapping testing
func FindDBRPMapping(
	init func(DBRPMappingFields, *testing.T) (platform.DBRPMappingService, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.DBRPMappingFilter
	}

	type wants struct {
		dbrpMapping *platform.DBRPMapping
		err         error
	}

	tests := []struct {
		name   string
		fields DBRPMappingFields
		args   args
		wants  wants
	}{
		{
			name: "find dbrpMappings by cluster db and rp",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketA"),
					},
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketB"),
					},
				},
			},
			args: args{
				filter: platform.DBRPMappingFilter{
					Cluster:         strPtr("cluster"),
					Database:        strPtr("database"),
					RetentionPolicy: strPtr("retention_policyB"),
				},
			},
			wants: wants{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster",
					Database:        "database",
					RetentionPolicy: "retention_policyB",
					Default:         false,
					OrganizationID:  platform.ID("org"),
					BucketID:        platform.ID("bucketB"),
				},
			},
		},
		{
			name: "find default rp from dbrpMappings",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyA",
						Default:         false,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketA"),
					},
					{
						Cluster:         "cluster",
						Database:        "database",
						RetentionPolicy: "retention_policyB",
						Default:         true,
						OrganizationID:  platform.ID("org"),
						BucketID:        platform.ID("bucketB"),
					},
				},
			},
			args: args{
				filter: platform.DBRPMappingFilter{
					Cluster:  strPtr("cluster"),
					Database: strPtr("database"),
					Default:  boolPtr(true),
				},
			},
			wants: wants{
				dbrpMapping: &platform.DBRPMapping{
					Cluster:         "cluster",
					Database:        "database",
					RetentionPolicy: "retention_policyB",
					Default:         true,
					OrganizationID:  platform.ID("org"),
					BucketID:        platform.ID("bucketB"),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			dbrpMapping, err := s.Find(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(dbrpMapping, tt.wants.dbrpMapping, dbrpMappingCmpOptions...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteDBRPMapping testing
func DeleteDBRPMapping(
	init func(DBRPMappingFields, *testing.T) (platform.DBRPMappingService, func()),
	t *testing.T,
) {
	type args struct {
		Cluster, Database, RetentionPolicy string
	}
	type wants struct {
		err          error
		dbrpMappings []*platform.DBRPMapping
	}

	tests := []struct {
		name   string
		fields DBRPMappingFields
		args   args
		wants  wants
	}{
		{
			name: "delete existing dbrpMapping",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
			args: args{
				Cluster:         "cluster1",
				Database:        "database1",
				RetentionPolicy: "retention_policy1",
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{{
					Cluster:         "cluster2",
					Database:        "database2",
					RetentionPolicy: "retention_policy2",
					Default:         true,
					OrganizationID:  platform.ID("org2"),
					BucketID:        platform.ID("bucket2"),
				}},
			},
		},
		{
			name: "delete dbrpMappings using key that does not exist",
			fields: DBRPMappingFields{
				DBRPMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
					},
				},
			},
			args: args{
				Cluster:         "cluster3",
				Database:        "db",
				RetentionPolicy: "rp",
			},
			wants: wants{
				dbrpMappings: []*platform.DBRPMapping{
					{
						Cluster:         "cluster1",
						Database:        "database1",
						RetentionPolicy: "retention_policy1",
						Default:         false,
						OrganizationID:  platform.ID("org1"),
						BucketID:        platform.ID("bucket1"),
					},
					{
						Cluster:         "cluster2",
						Database:        "database2",
						RetentionPolicy: "retention_policy2",
						Default:         true,
						OrganizationID:  platform.ID("org2"),
						BucketID:        platform.ID("bucket2"),
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
			err := s.Delete(ctx, tt.args.Cluster, tt.args.Database, tt.args.RetentionPolicy)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.DBRPMappingFilter{}
			dbrpMappings, _, err := s.FindMany(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve dbrpMappings: %v", err)
			}
			if diff := cmp.Diff(dbrpMappings, tt.wants.dbrpMappings, dbrpMappingCmpOptions...); diff != "" {
				t.Errorf("dbrpMappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func strPtr(s string) *string {
	return &s
}
func boolPtr(b bool) *bool {
	return &b
}
