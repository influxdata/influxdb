package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

var bucketCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.Bucket) []*platform.Bucket {
		out := append([]*platform.Bucket(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// BucketFields will include the IDGenerator, and buckets
type BucketFields struct {
	IDGenerator platform.IDGenerator
	Buckets     []*platform.Bucket
}

// CreateBucket testing
func CreateBucket(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		bucket *platform.Bucket
	}
	type wants struct {
		err     error
		buckets []*platform.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "create buckets with empty set",
			fields: BucketFields{
				IDGenerator: mock.NewIDGenerator("id1"),
				Buckets:     []*platform.Bucket{},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "name1",
					OrganizationID: platform.ID("org1"),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						Name:           "name1",
						ID:             platform.ID("id1"),
						OrganizationID: platform.ID("org1"),
					},
				},
			},
		},
		{
			name: "basic create bucket",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket2",
					OrganizationID: platform.ID("org1"),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
					{
						ID:             platform.ID("2"),
						Name:           "bucket2",
						OrganizationID: platform.ID("org1"),
					},
				},
			},
		},
		{
			name: "names should be unique within an organization",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket1",
					OrganizationID: platform.ID("org1"),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
				},
				err: fmt.Errorf("bucket with name bucket1 already exists"),
			},
		},
		{
			name: "names should not be unique across organizations",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return platform.ID("2")
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket1",
					OrganizationID: platform.ID("org2"),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org1"),
					},
					{
						ID:             platform.ID("2"),
						Name:           "bucket1",
						OrganizationID: platform.ID("org2"),
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
			err := s.CreateBucket(ctx, tt.args.bucket)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteBucket(ctx, tt.args.bucket.ID)

			buckets, _, err := s.FindBuckets(ctx, platform.BucketFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve buckets: %v", err)
			}
			if diff := cmp.Diff(buckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBucketByID testing
func FindBucketByID(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		id platform.ID
	}
	type wants struct {
		err    error
		bucket *platform.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "basic find bucket by id",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id: platform.ID("2"),
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:             platform.ID("2"),
					OrganizationID: platform.ID("org1"),
					Name:           "bucket2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			bucket, err := s.FindBucketByID(ctx, tt.args.id)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBuckets testing
func FindBuckets(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		ID   string
		name string
	}

	type wants struct {
		buckets []*platform.Bucket
		err     error
	}
	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "find all buckets",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org2"),
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find bucket by id",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Name:           "xyz",
					},
				},
			},
			args: args{
				ID: "test2",
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find bucket by name",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test1"),
						OrganizationID: platform.ID("org1"),
						Name:           "abc",
					},
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Name:           "xyz",
					},
				},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             platform.ID("test2"),
						OrganizationID: platform.ID("org1"),
						Name:           "xyz",
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

			filter := platform.BucketFilter{}
			if tt.args.ID != "" {
				id := platform.ID(tt.args.ID)
				filter.ID = &id
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			buckets, _, err := s.FindBuckets(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected errors to be equal '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if diff := cmp.Diff(buckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteBucket testing
func DeleteBucket(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err     error
		buckets []*platform.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "delete buckets using exist id",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						Name: "orgA",
						ID:   platform.ID("abc"),
					},
					{
						Name: "orgB",
						ID:   platform.ID("xyz"),
					},
				},
			},
			args: args{
				ID: "abc",
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						Name: "orgB",
						ID:   platform.ID("xyz"),
					},
				},
			},
		},
		{
			name: "delete buckets using id that does not exist",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						Name: "orgA",
						ID:   platform.ID("abc"),
					},
					{
						Name: "orgB",
						ID:   platform.ID("xyz"),
					},
				},
			},
			args: args{
				ID: "123",
			},
			wants: wants{
				err: fmt.Errorf("bucket not found"),
				buckets: []*platform.Bucket{
					{
						Name: "orgA",
						ID:   platform.ID("abc"),
					},
					{
						Name: "orgB",
						ID:   platform.ID("xyz"),
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
			err := s.DeleteBucket(ctx, platform.ID(tt.args.ID))
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			filter := platform.BucketFilter{}
			buckets, _, err := s.FindBuckets(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve buckets: %v", err)
			}
			if diff := cmp.Diff(buckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBucket testing
func FindBucket(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		name string
	}

	type wants struct {
		bucket *platform.Bucket
		err    error
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "find bucket by name",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:   platform.ID("a"),
						Name: "abc",
					},
					{
						ID:   platform.ID("b"),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:   platform.ID("a"),
					Name: "abc",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			filter := platform.BucketFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			bucket, err := s.FindBucket(ctx, filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateBucket testing
func UpdateBucket(
	init func(BucketFields, *testing.T) (platform.BucketService, func()),
	t *testing.T,
) {
	type args struct {
		name      string
		id        platform.ID
		retention int
	}
	type wants struct {
		err    error
		bucket *platform.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:   platform.ID("1"),
				name: "changed",
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:             platform.ID("1"),
					OrganizationID: platform.ID("org1"),
					Name:           "changed",
				},
			},
		},
		{
			name: "update retention",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:        platform.ID("1"),
				retention: 100,
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:              platform.ID("1"),
					OrganizationID:  platform.ID("org1"),
					Name:            "bucket1",
					RetentionPeriod: 100 * time.Minute,
				},
			},
		},
		{
			name: "update retention and name",
			fields: BucketFields{
				Buckets: []*platform.Bucket{
					{
						ID:             platform.ID("1"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket1",
					},
					{
						ID:             platform.ID("2"),
						OrganizationID: platform.ID("org1"),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:        platform.ID("2"),
				retention: 101,
				name:      "changed",
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:              platform.ID("2"),
					OrganizationID:  platform.ID("org1"),
					Name:            "changed",
					RetentionPeriod: 101 * time.Minute,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()

			upd := platform.BucketUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.retention != 0 {
				d := time.Duration(tt.args.retention) * time.Minute
				upd.RetentionPeriod = &d
			}

			bucket, err := s.UpdateBucket(ctx, tt.args.id, upd)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
