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

var mappingCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*platform.UserResourceMapping) []*platform.UserResourceMapping {
		out := append([]*platform.UserResourceMapping(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ResourceID.String() > out[j].ResourceID.String()
		})
		return out
	}),
}

// UserResourceFields includes prepopulated data for mapping tests
type UserResourceFields struct {
	UserResourceMappings []*platform.UserResourceMapping
}

type userResourceMappingServiceF func(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
)

// UserResourceMappingService tests all the service functions.
func UserResourceMappingService(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   userResourceMappingServiceF
	}{
		{
			name: "CreateUserResourceMapping",
			fn:   CreateUserResourceMapping,
		},
		{
			name: "FindUserResourceMappings",
			fn:   FindUserResourceMappings,
		},
		{
			name: "DeleteUserResourceMapping",
			fn:   DeleteUserResourceMapping,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

func CreateUserResourceMapping(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		mapping *platform.UserResourceMapping
	}
	type wants struct {
		err      error
		mappings []*platform.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic create user resource mapping",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				mapping: &platform.UserResourceMapping{
					ResourceID:   MustIDBase16(bucketTwoID),
					UserID:       MustIDBase16(userTwoID),
					UserType:     platform.Member,
					ResourceType: platform.BucketResourceType,
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
		},
		{
			name: "duplicate mappings are not allowed",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID: MustIDBase16(bucketOneID),
						UserID:     MustIDBase16(userOneID),
						UserType:   platform.Member,
					},
				},
			},
			args: args{
				mapping: &platform.UserResourceMapping{
					ResourceID: MustIDBase16(bucketOneID),
					UserID:     MustIDBase16(userOneID),
					UserType:   platform.Member,
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID: MustIDBase16(bucketOneID),
						UserID:     MustIDBase16(userOneID),
						UserType:   platform.Member,
					},
				},
				err: fmt.Errorf("mapping for user %s already exists", userOneID),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.CreateUserResourceMapping(ctx, tt.args.mapping)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}
			defer s.DeleteUserResourceMapping(ctx, tt.args.mapping.ResourceID, tt.args.mapping.UserID)

			mappings, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve mappings: %v", err)
			}
			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func DeleteUserResourceMapping(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		resourceID platform.ID
		userID     platform.ID
	}
	type wants struct {
		err      error
		mappings []*platform.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic delete user resource mapping",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID: MustIDBase16(bucketOneID),
						UserID:     MustIDBase16(userOneID),
						UserType:   platform.Member,
					},
				},
			},
			args: args{
				resourceID: MustIDBase16(bucketOneID),
				userID:     MustIDBase16(userOneID),
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{},
			},
		},
		{
			name: "deleting a non-existant user",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{},
			},
			args: args{
				resourceID: MustIDBase16(bucketOneID),
				userID:     MustIDBase16(userOneID),
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{},
				err:      fmt.Errorf("userResource mapping not found"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.TODO()
			err := s.DeleteUserResourceMapping(ctx, tt.args.resourceID, tt.args.userID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			mappings, _, err := s.FindUserResourceMappings(ctx, platform.UserResourceMappingFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve mappings: %v", err)
			}
			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

func FindUserResourceMappings(
	init func(UserResourceFields, *testing.T) (platform.UserResourceMappingService, func()),
	t *testing.T,
) {
	type args struct {
		filter platform.UserResourceMappingFilter
	}
	type wants struct {
		err      error
		mappings []*platform.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields UserResourceFields
		args   args
		wants  wants
	}{
		{
			name: "basic find mappings",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by user",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserID: MustIDBase16(userOneID),
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by resource",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					ResourceID: MustIDBase16(bucketOneID),
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by user type",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Owner,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					UserType: platform.Owner,
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Owner,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
		},
		{
			name: "find mappings filtered by resource type",
			fields: UserResourceFields{
				UserResourceMappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketOneID),
						UserID:       MustIDBase16(userOneID),
						UserType:     platform.Member,
						ResourceType: platform.DashboardResourceType,
					},
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
					},
				},
			},
			args: args{
				filter: platform.UserResourceMappingFilter{
					ResourceType: platform.BucketResourceType,
				},
			},
			wants: wants{
				mappings: []*platform.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(bucketTwoID),
						UserID:       MustIDBase16(userTwoID),
						UserType:     platform.Member,
						ResourceType: platform.BucketResourceType,
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
			mappings, _, err := s.FindUserResourceMappings(ctx, tt.args.filter)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}

			if err != nil && tt.wants.err != nil {
				if err.Error() != tt.wants.err.Error() {
					t.Fatalf("expected error messages to match '%v' got '%v'", tt.wants.err, err.Error())
				}
			}

			if diff := cmp.Diff(mappings, tt.wants.mappings, mappingCmpOptions...); diff != "" {
				t.Errorf("mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
