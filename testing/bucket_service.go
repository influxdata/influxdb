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

const (
	bucketOneID   = "020f755c3c082000"
	bucketTwoID   = "020f755c3c082001"
	bucketThreeID = "020f755c3c082002"
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
	IDGenerator   platform.IDGenerator
	Buckets       []*platform.Bucket
	Organizations []*platform.Organization
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
				IDGenerator: mock.NewIDGenerator(bucketOneID, t),
				Buckets:     []*platform.Bucket{},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "name1",
					OrganizationID: idFromString(t, orgOneID),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						Name:           "name1",
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
					},
				},
			},
		},
		{
			name: "basic create bucket",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return idFromString(t, bucketTwoID)
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						OrganizationID: idFromString(t, orgOneID),
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket2",
					OrganizationID: idFromString(t, orgTwoID),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						Organization:   "theorg",
						OrganizationID: idFromString(t, orgOneID),
					},
					{
						ID:             idFromString(t, bucketTwoID),
						Name:           "bucket2",
						Organization:   "otherorg",
						OrganizationID: idFromString(t, orgTwoID),
					},
				},
			},
		},
		{
			name: "basic create bucket using org name",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return idFromString(t, bucketTwoID)
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						OrganizationID: idFromString(t, orgOneID),
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:         "bucket2",
					Organization: "otherorg",
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						Organization:   "theorg",
						OrganizationID: idFromString(t, orgOneID),
					},
					{
						ID:             idFromString(t, bucketTwoID),
						Name:           "bucket2",
						Organization:   "otherorg",
						OrganizationID: idFromString(t, orgTwoID),
					},
				},
			},
		},
		{
			name: "names should be unique within an organization",
			fields: BucketFields{
				IDGenerator: &mock.IDGenerator{
					IDFn: func() platform.ID {
						return idFromString(t, bucketTwoID)
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						OrganizationID: idFromString(t, orgOneID),
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket1",
					OrganizationID: idFromString(t, orgOneID),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						Organization:   "theorg",
						OrganizationID: idFromString(t, orgOneID),
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
						return idFromString(t, bucketTwoID)
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						OrganizationID: idFromString(t, orgOneID),
					},
				},
			},
			args: args{
				bucket: &platform.Bucket{
					Name:           "bucket1",
					OrganizationID: idFromString(t, orgTwoID),
				},
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						Name:           "bucket1",
						Organization:   "theorg",
						OrganizationID: idFromString(t, orgOneID),
					},
					{
						ID:             idFromString(t, bucketTwoID),
						Name:           "bucket1",
						Organization:   "otherorg",
						OrganizationID: idFromString(t, orgTwoID),
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
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket1",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket2",
					},
				},
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
			},
			args: args{
				id: idFromString(t, bucketTwoID),
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:             idFromString(t, bucketTwoID),
					OrganizationID: idFromString(t, orgOneID),
					Organization:   "theorg",
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
		ID             platform.ID
		name           string
		organization   string
		organizationID platform.ID
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgTwoID),
						Name:           "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgTwoID),
						Organization:   "otherorg",
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find buckets by organization name",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgTwoID),
						Name:           "xyz",
					},
					{
						ID:             idFromString(t, bucketThreeID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "123",
					},
				},
			},
			args: args{
				organization: "theorg",
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketThreeID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
		{
			name: "find buckets by organization id",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
					{
						Name: "otherorg",
						ID:   idFromString(t, orgTwoID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgTwoID),
						Name:           "xyz",
					},
					{
						ID:             idFromString(t, bucketThreeID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "123",
					},
				},
			},
			args: args{
				organizationID: idFromString(t, orgOneID),
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketThreeID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "123",
					},
				},
			},
		},
		{
			name: "find bucket by id",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "xyz",
					},
				},
			},
			args: args{
				ID: idFromString(t, bucketTwoID),
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
						Name:           "xyz",
					},
				},
			},
		},
		{
			name: "find bucket by name",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
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
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
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
			if tt.args.ID != nil {
				filter.ID = &tt.args.ID
			}
			if tt.args.organizationID != nil {
				filter.OrganizationID = &tt.args.organizationID
			}
			if tt.args.organization != "" {
				filter.Organization = &tt.args.organization
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: idFromString(t, orgOneID),
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: idFromString(t, orgOneID),
					},
				},
			},
			args: args{
				ID: "abc",
			},
			wants: wants{
				buckets: []*platform.Bucket{
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
					},
				},
			},
		},
		{
			name: "delete buckets using id that does not exist",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: idFromString(t, orgOneID),
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: idFromString(t, orgOneID),
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
						Name:           "A",
						ID:             platform.ID("abc"),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
					},
					{
						Name:           "B",
						ID:             platform.ID("xyz"),
						OrganizationID: idFromString(t, orgOneID),
						Organization:   "theorg",
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
		name           string
		organizationID platform.ID
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "abc",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "xyz",
					},
				},
			},
			args: args{
				name:           "abc",
				organizationID: idFromString(t, orgOneID),
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:             idFromString(t, bucketOneID),
					OrganizationID: idFromString(t, orgOneID),
					Organization:   "theorg",
					Name:           "abc",
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
			if tt.args.organizationID != nil {
				filter.OrganizationID = &tt.args.organizationID
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
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket1",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:   idFromString(t, bucketOneID),
				name: "changed",
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:             idFromString(t, bucketOneID),
					OrganizationID: idFromString(t, orgOneID),
					Organization:   "theorg",
					Name:           "changed",
				},
			},
		},
		{
			name: "update retention",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket1",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:        idFromString(t, bucketOneID),
				retention: 100,
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:              idFromString(t, bucketOneID),
					OrganizationID:  idFromString(t, orgOneID),
					Organization:    "theorg",
					Name:            "bucket1",
					RetentionPeriod: 100 * time.Minute,
				},
			},
		},
		{
			name: "update retention and name",
			fields: BucketFields{
				Organizations: []*platform.Organization{
					{
						Name: "theorg",
						ID:   idFromString(t, orgOneID),
					},
				},
				Buckets: []*platform.Bucket{
					{
						ID:             idFromString(t, bucketOneID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket1",
					},
					{
						ID:             idFromString(t, bucketTwoID),
						OrganizationID: idFromString(t, orgOneID),
						Name:           "bucket2",
					},
				},
			},
			args: args{
				id:        idFromString(t, bucketTwoID),
				retention: 101,
				name:      "changed",
			},
			wants: wants{
				bucket: &platform.Bucket{
					ID:              idFromString(t, bucketTwoID),
					OrganizationID:  idFromString(t, orgOneID),
					Organization:    "theorg",
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
