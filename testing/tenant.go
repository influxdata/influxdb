package testing

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
)

type TenantFields struct {
	OrgIDGenerator       platform.IDGenerator
	BucketIDGenerator    platform.IDGenerator
	Users                []*influxdb.User
	Passwords            []string // passwords are indexed against the Users field
	UserResourceMappings []*influxdb.UserResourceMapping
	Organizations        []*influxdb.Organization
	Buckets              []*influxdb.Bucket
}

// TenantService tests the tenant service functions.
// These tests stress the relation between the services embedded by the TenantService.
// The individual functionality of services is tested elsewhere.
func TenantService(t *testing.T, init func(*testing.T, TenantFields) (influxdb.TenantService, func())) {
	tests := []struct {
		name string
		fn   func(t *testing.T, init func(*testing.T, TenantFields) (influxdb.TenantService, func()))
	}{
		{
			name: "Create",
			fn:   Create,
		},
		{
			name: "Delete",
			fn:   Delete,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, init)
		})
	}
}

type bucketsByName []*influxdb.Bucket

func (b bucketsByName) Len() int {
	return len(b)
}

func (b bucketsByName) Less(i, j int) bool {
	return strings.Compare(b[i].Name, b[j].Name) < 0
}

func (b bucketsByName) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type urmByResourceID []*influxdb.UserResourceMapping

func (u urmByResourceID) Len() int {
	return len(u)
}

func (u urmByResourceID) Less(i, j int) bool {
	return u[i].ResourceID < u[j].ResourceID
}

func (u urmByResourceID) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type urmByUserID []*influxdb.UserResourceMapping

func (u urmByUserID) Len() int {
	return len(u)
}

func (u urmByUserID) Less(i, j int) bool {
	return u[i].UserID < u[j].UserID
}

func (u urmByUserID) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

// Create tests various cases of creation for the services in the TenantService.
// For example, when you create a user, do you create system buckets? How are URMs organized?
func Create(t *testing.T, init func(*testing.T, TenantFields) (influxdb.TenantService, func())) {
	t.Helper()

	// Blank fields, we are testing creation.
	fields := func() TenantFields {
		return TenantFields{
			OrgIDGenerator:    mock.NewIncrementingIDGenerator(1),
			BucketIDGenerator: mock.NewIncrementingIDGenerator(1),
		}
	}

	// NOTE(affo)(*kv.Service): tests that contain s.CreateOrganization() generate error in logs:
	//   Failed to make user owner of organization: {"error": "could not find authorizer on context when adding user to resource type orgs"}.
	// This happens because kv requires an authorization to be in context.
	// This is a bad dependency pattern (store -> auth) and should not be there.
	// Anyways this does not prevent the org to be created. If you add the urm manually you'll obtain the same result.

	// NOTE(affo)(*kv.Service): it also creates urms for the non existing user found in context.
	t.Run("creating an org creates system buckets", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		o := &influxdb.Organization{
			// ID(1)
			Name: "org1",
		}
		if err := s.CreateOrganization(ctx, o); err != nil {
			t.Fatal(err)
		}

		// Check existence
		orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if norgs != 1 {
			t.Errorf("expected 1 org, got: %v", orgs)
		}
		usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nusrs > 0 {
			t.Errorf("expected no user, got: %v", usrs)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected no urm, got: %+v", urms)
		}
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 2 {
			t.Errorf("expected 2 buckets, got: %v", bs)
		}
		sort.Sort(bucketsByName(bs))
		if name := bs[0].Name; name != "_monitoring" {
			t.Errorf("unexpected nam for bucket: %s", name)
		}
		if name := bs[1].Name; name != "_tasks" {
			t.Errorf("unexpected nam for bucket: %s", name)
		}
	})

	// NOTE(affo)(*kv.Service): nope, it does create system buckets with invalid OrgIDs.
	t.Run("creating user creates only the user", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// Number of buckets prior to user creation.
		// This is because, for now, system buckets always get returned for compatibility with the old system.
		_, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}

		u := &influxdb.User{
			ID:   1,
			Name: "user1",
		}
		if err := s.CreateUser(ctx, u); err != nil {
			t.Fatal(err)
		}

		// Check existence
		orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if norgs != 0 {
			t.Errorf("expected no org, got: %v", orgs)
		}
		usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nusrs != 1 {
			t.Errorf("expected 1 user, got: %v", usrs)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected no urm, got: %v", urms)
		}
		bs, nnbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		// Compare new number of buckets with the one prior to user creation.
		if nnbs != nbs {
			t.Errorf("expected no bucket created, got: %+v", bs)
		}
	})

	// NOTE(affo)(*kv.Service): nope, it does create a useless URM, no existence check.
	//  Apparently, system buckets are created too :thinking.
	t.Run("creating urm pointing to non existing user fails", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// First create an org and a user.
		u := &influxdb.User{
			ID:   1,
			Name: "user1",
		}
		if err := s.CreateUser(ctx, u); err != nil {
			t.Fatal(err)
		}
		o := &influxdb.Organization{
			// ID(1)
			Name: "org1",
		}
		if err := s.CreateOrganization(ctx, o); err != nil {
			t.Fatal(err)
		}

		checkInvariance := func(nurms int) {
			orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if norgs != 1 {
				t.Errorf("expected 1 org, got: %v", orgs)
			}
			usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if nusrs != 1 {
				t.Errorf("expected 1 user, got: %v", usrs)
			}
			urms, nnurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if nnurms != nurms {
				t.Errorf("expected %d urms got %d: %+v", nurms, nnurms, urms)
			}
			bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if nbs != 2 {
				t.Errorf("expected 2 buckets, got: %v", bs)
			}
		}

		checkInvariance(0)

		// Wrong userID.
		urm := &influxdb.UserResourceMapping{
			UserID:       2,
			UserType:     influxdb.Owner,
			MappingType:  influxdb.UserMappingType,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   1,
		}
		if err := s.CreateUserResourceMapping(ctx, urm); err == nil {
			t.Errorf("expected error got none")
		}

		checkInvariance(0)

		// Wrong orgID. The URM gets created successfully.
		urm = &influxdb.UserResourceMapping{
			UserID:       1,
			UserType:     influxdb.Owner,
			MappingType:  influxdb.UserMappingType,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   2,
		}
		if err := s.CreateUserResourceMapping(ctx, urm); err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		checkInvariance(1)
	})

	// NOTE(affo)(*kv.Service): errors on bucket creation.
	//  But, apparently, system buckets are created too :thinking.
	t.Run("should not be possible to create bucket without org", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// Number of buckets prior to bucket creation.
		// This is because, for now, system buckets always get returned for compatibility with the old system.
		_, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}

		b := &influxdb.Bucket{
			// ID(1)
			OrgID: 1,
			Name:  "bucket1",
		}
		if err := s.CreateBucket(ctx, b); err == nil {
			t.Errorf("expected error got none")
		}

		// Check existence
		orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if norgs != 0 {
			t.Errorf("expected no org, got: %v", orgs)
		}
		usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nusrs != 0 {
			t.Errorf("expected no user, got: %v", usrs)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected no urm, got: %v", urms)
		}
		bs, nnbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		// Compare new number of buckets with the one prior to bucket creation.
		if nnbs != nbs {
			t.Errorf("expected bucket created, got: %+v", bs)
		}
	})

	t.Run("making user part of org creates mapping to org only", func(t *testing.T) {
		for _, userType := range []influxdb.UserType{influxdb.Owner, influxdb.Member} {
			t.Run(string(userType), func(t *testing.T) {
				data := fields()
				s, done := init(t, data)
				defer done()
				ctx := context.Background()

				u := &influxdb.User{
					ID:   1,
					Name: "user1",
				}
				if err := s.CreateUser(ctx, u); err != nil {
					t.Fatal(err)
				}
				o := &influxdb.Organization{
					// ID(1)
					Name: "org1",
				}
				if err := s.CreateOrganization(ctx, o); err != nil {
					t.Fatal(err)
				}
				urm := &influxdb.UserResourceMapping{
					UserID:       u.ID,
					UserType:     userType,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   o.ID,
				}
				if err := s.CreateUserResourceMapping(ctx, urm); err != nil {
					t.Fatal(err)
				}

				// Check existence
				orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
				if err != nil {
					t.Fatal(err)
				}
				if norgs != 1 {
					t.Errorf("expected 1 org, got: %v", orgs)
				}
				usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
				if err != nil {
					t.Fatal(err)
				}
				if nusrs != 1 {
					t.Errorf("expected 1 user, got: %v", usrs)
				}

				bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
				if err != nil {
					t.Fatal(err)
				}
				if nbs != 2 {
					t.Errorf("expected 2 buckets, got: %v", bs)
				}
				sort.Sort(bucketsByName(bs))
				if name := bs[0].Name; name != "_monitoring" {
					t.Errorf("unexpected name for bucket: %s", name)
				}
				if name := bs[1].Name; name != "_tasks" {
					t.Errorf("unexpected name for bucket: %v", name)
				}

				urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
				if err != nil {
					t.Fatal(err)
				}
				want := []*influxdb.UserResourceMapping{
					{
						UserID:       u.ID,
						UserType:     userType,
						MappingType:  influxdb.UserMappingType,
						ResourceType: influxdb.OrgsResourceType,
						ResourceID:   o.ID,
					},
				}
				sort.Sort(urmByResourceID(want))
				sort.Sort(urmByResourceID(urms))
				if diff := cmp.Diff(want, urms); diff != "" {
					t.Errorf("unexpected urms -want/+got:\n\t%s", diff)
				}

				// Now add a new bucket and check the URMs.
				b := &influxdb.Bucket{
					// ID(1)
					OrgID: o.ID,
					Name:  "bucket1",
				}
				if err := s.CreateBucket(ctx, b); err != nil {
					t.Fatal(err)
				}
				urms, _, err = s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
				if err != nil {
					t.Fatal(err)
				}

				sort.Sort(urmByResourceID(urms))
				if diff := cmp.Diff(want, urms); diff != "" {
					t.Errorf("unexpected urms -want/+got:\n\t%s", diff)
				}
			})
		}
	})
}

// Delete tests various cases of deletion for the services in the TenantService.
// An example: if you delete a bucket the corresponding user resource mapping is not present.
func Delete(t *testing.T, init func(*testing.T, TenantFields) (influxdb.TenantService, func())) {
	t.Helper()

	fields := func() TenantFields {
		return TenantFields{
			OrgIDGenerator: mock.NewIncrementingIDGenerator(1),
			// URM are userID + resourceID (they do not include resource type)
			// so same IDs across different resources leads to collisions
			// therefore, we need to start bucket IDs at higher offset for
			// test.
			BucketIDGenerator: mock.NewIncrementingIDGenerator(10),
			Users: []*influxdb.User{
				{
					ID:   1,
					Name: "user1",
				},
				{
					ID:   2,
					Name: "user2",
				},
			},
			Passwords: []string{"password1", "password2"},
			Organizations: []*influxdb.Organization{
				{
					// ID(1)
					Name: "org1",
				},
				{
					// ID(2)
					Name: "org2",
				},
			},
			// 2 organizations create 2 system buckets each
			// so start at 14
			Buckets: []*influxdb.Bucket{
				{
					// ID(14)
					OrgID: 1,
					Name:  "bucket1",
				},
				{
					// ID(15)
					OrgID: 2,
					Name:  "bucket2",
				},
			},
			UserResourceMappings: []*influxdb.UserResourceMapping{
				// NOTE(affo): bucket URMs should not be here, create them only for deletion purposes.
				// user 1 owns org1 (and so bucket1)
				{
					UserID:       1,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   1,
				},
				{
					UserID:       1,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.BucketsResourceType,
					ResourceID:   14,
				},
				// user 1 is member of org2 (and so bucket2)
				{
					UserID:       1,
					UserType:     influxdb.Member,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   2,
				},
				{
					UserID:       1,
					UserType:     influxdb.Member,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.BucketsResourceType,
					ResourceID:   15,
				},
				// user 2 owns org2 (and so bucket2)
				{
					UserID:       2,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   2,
				},
				{
					UserID:       2,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.BucketsResourceType,
					ResourceID:   15,
				},
			},
		}
	}

	t.Run("deleting bucket deletes urm", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		f := influxdb.UserResourceMappingFilter{
			ResourceID:   data.Buckets[0].ID,
			ResourceType: influxdb.BucketsResourceType,
		}
		urms, n, err := s.FindUserResourceMappings(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected 1 urm, got: %v", urms)
		}
		if err := s.DeleteBucket(ctx, data.Buckets[0].ID); err != nil {
			t.Fatal(err)
		}
		f = influxdb.UserResourceMappingFilter{
			ResourceID:   data.Buckets[0].ID,
			ResourceType: influxdb.BucketsResourceType,
		}
		urms, n, err = s.FindUserResourceMappings(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			t.Fatalf("expected no urm, got: %v", urms)
		}
	})

	// NOTE(affo): those resources could not be dangling (URM could be inferred from an user being in the owner org).
	// We do not want to automatically propagate this kind of delete because an resource will always have an owner org.
	t.Run("deleting bucket urm does create dangling bucket", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// Pre-check the current situation.
		// bucket1 is owned by user1.
		// Check it.
		urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[0].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []*influxdb.UserResourceMapping{
			{
				UserID:       data.Users[0].ID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[0].ID,
			},
		}
		sort.Sort(urmByUserID(want))
		sort.Sort(urmByUserID(urms))
		if diff := cmp.Diff(want, urms); diff != "" {
			t.Fatalf("unexpected urms -want/+got:\n\t%s", diff)
		}
		// bucket2 is owned by user2.
		// bucket2 is readable by user2.
		// Check it.
		urms, _, err = s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[1].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		want = []*influxdb.UserResourceMapping{
			{
				UserID:       data.Users[1].ID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[1].ID,
			},
			{
				UserID:       data.Users[0].ID,
				UserType:     influxdb.Member,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[1].ID,
			},
		}
		sort.Sort(urmByUserID(want))
		sort.Sort(urmByUserID(urms))
		if diff := cmp.Diff(want, urms); diff != "" {
			t.Fatalf("unexpected urms -want/+got:\n\t%s", diff)
		}

		// Now delete user2 -> bucket2.
		// Still expect bucket2 to exist (user1 still points to it).
		if err := s.DeleteUserResourceMapping(ctx, data.Buckets[1].ID, data.Users[1].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[1].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 1 {
			t.Errorf("expected 1 buckets, got: %v", bs)
		}
		// Now delete user1 -> bucket2.
		// Still expect bucket2 to exist (nobody points to it).
		if err := s.DeleteUserResourceMapping(ctx, data.Buckets[1].ID, data.Users[0].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[1].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 1 {
			t.Errorf("expected 1 buckets, got: %v", bs)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[1].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if nurms != 0 {
			t.Errorf("expected bucket2, to be dangling, got: %+v", urms)
		}
		// Now delete user1 -> bucket1.
		// Still expect bucket1 to exist (nobody points to it).
		if err := s.DeleteUserResourceMapping(ctx, data.Buckets[0].ID, data.Users[0].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 1 {
			t.Errorf("expected 1 buckets, got: %v", bs)
		}
		urms, nurms, err = s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[0].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if nurms != 0 {
			t.Errorf("expected bucket1, to be dangling, got: %+v", urms)
		}
	})

	t.Run("deleting a user deletes every related urm and nothing else", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// bucket1 is owned by user1.
		// Check it.
		urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[0].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := []*influxdb.UserResourceMapping{
			{
				UserID:       data.Users[0].ID,
				UserType:     influxdb.Owner,
				MappingType:  influxdb.UserMappingType,
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[0].ID,
			},
		}
		sort.Sort(urmByUserID(want))
		sort.Sort(urmByUserID(urms))
		if diff := cmp.Diff(want, urms); diff != "" {
			t.Fatalf("unexpected urms -want/+got:\n\t%s", diff)
		}

		// Delete user1.
		// We expect his urms deleted but not bucket1.
		if err := s.DeleteUser(ctx, data.Users[0].ID); err != nil {
			t.Fatal(err)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			UserID: data.Users[0].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected that user deletion would remove dangling urms, got: %+v", urms)
		}
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 1 {
			t.Errorf("expected 1 buckets, got: %v", bs)
		}
	})

	t.Run("deleting a bucket deletes every related urm", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// Delete bucket2.
		// We expect its urms deleted.
		if err := s.DeleteBucket(ctx, data.Buckets[1].ID); err != nil {
			t.Fatal(err)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			ResourceType: influxdb.BucketsResourceType,
			ResourceID:   data.Buckets[1].ID,
		})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected that bucket deletion would remove dangling urms, got: %+v", urms)
		}
	})

	// NOTE(affo)(*kv.Service): buckets, users, and urms survive.
	t.Run("deleting an organization should delete everything that depends on it", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		// Delete org1.
		// We expect its buckets to be deleted.
		// We expect urms to those buckets to be deleted too.
		// No user should be deleted.
		preDeletionBuckets, _, err := s.FindBuckets(ctx, influxdb.BucketFilter{OrganizationID: &data.Organizations[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteOrganization(ctx, data.Organizations[0].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{OrganizationID: &data.Organizations[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected org buckets to be deleted, got: %+v", bs)
		}

		urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
			UserID:       data.Users[0].ID,
			ResourceType: influxdb.BucketsResourceType,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, urm := range urms {
			for _, b := range preDeletionBuckets {
				if urm.ResourceID == b.ID {
					t.Errorf("expected this urm to be deleted, got %+v instead", urm)
				}
			}
		}
		if _, err := s.FindUser(ctx, influxdb.UserFilter{ID: &data.Users[0].ID}); err != nil {
			t.Fatal(err)
		}

		// Delete org2.
		// Everything should disappear.
		if err := s.DeleteOrganization(ctx, data.Organizations[1].ID); err != nil {
			t.Fatal(err)
		}
		orgs, norgs, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if norgs != 0 {
			t.Errorf("expected no org, got: %v", orgs)
		}
		usrs, nusrs, err := s.FindUsers(ctx, influxdb.UserFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nusrs != 2 {
			t.Errorf("expected 2 users, got: %v", usrs)
		}
		urms, nurms, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nurms > 0 {
			t.Errorf("expected no urm, got: %v", urms)
		}
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected buckets to be deleted, got: %+v", bs)
		}
	})
}
