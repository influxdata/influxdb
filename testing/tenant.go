package testing

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
)

type TenantFields struct {
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
	// Blank fields, we are testing creation.
	fields := func() TenantFields {
		return TenantFields{}
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
			ID:   1,
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
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected no buckets, got: %+v", bs)
		}
	})

	// NOTE(affo)(*kv.Service): nope, it does create a useless URM, no existence check.
	//  Apparently, system buckets are created too :thinking.
	t.Run("creating urm pointing to non existing user/org fails", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		urm := &influxdb.UserResourceMapping{
			UserID:       1,
			UserType:     influxdb.Owner,
			MappingType:  influxdb.UserMappingType,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   1,
		}
		if err := s.CreateUserResourceMapping(ctx, urm); err == nil {
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
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected no buckets, got: %v", bs)
		}
	})

	// NOTE(affo)(*kv.Service): errors on bucket creation.
	//  But, apparently, system buckets are created too :thinking.
	t.Run("should not be possible to create bucket without org", func(t *testing.T) {
		data := fields()
		s, done := init(t, data)
		defer done()
		ctx := context.Background()

		b := &influxdb.Bucket{
			ID:    1,
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
		bs, nbs, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected no buckets, got: %v", bs)
		}
	})

	t.Run("making user part of org creates mappings to buckets", func(t *testing.T) {
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
					ID:   1,
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
					{
						UserID:       u.ID,
						UserType:     userType,
						MappingType:  influxdb.UserMappingType,
						ResourceType: influxdb.BucketsResourceType,
						ResourceID:   bs[0].ID,
					},
					{
						UserID:       u.ID,
						UserType:     userType,
						MappingType:  influxdb.UserMappingType,
						ResourceType: influxdb.BucketsResourceType,
						ResourceID:   bs[1].ID,
					},
				}
				sort.Sort(urmByResourceID(want))
				sort.Sort(urmByResourceID(urms))
				if diff := cmp.Diff(want, urms); diff != "" {
					t.Errorf("unexpected urms -want/+got:\n\t%s", diff)
				}

				// Now add a new bucket and check the URMs.
				b := &influxdb.Bucket{
					ID:    1,
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
				want = append(want, &influxdb.UserResourceMapping{
					UserID:       u.ID,
					UserType:     userType,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.BucketsResourceType,
					ResourceID:   b.ID,
				})
				sort.Sort(urmByResourceID(want))
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
	fields := func() TenantFields {
		return TenantFields{
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
					ID:   10,
					Name: "org1",
				},
				{
					ID:   20,
					Name: "org2",
				},
			},
			Buckets: []*influxdb.Bucket{
				{
					ID:    100,
					OrgID: 10,
					Name:  "bucket1",
				},
				{
					ID:    200,
					OrgID: 20,
					Name:  "bucket2",
				},
			},
			UserResourceMappings: []*influxdb.UserResourceMapping{
				// user 1 owns org1 (and so bucket1)
				{
					UserID:       1,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   10,
				},
				// user 1 is member of org2 (and so bucket2)
				{
					UserID:       1,
					UserType:     influxdb.Member,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   20,
				},
				// user 2 owns org2 (and so bucket2)
				{
					UserID:       2,
					UserType:     influxdb.Owner,
					MappingType:  influxdb.UserMappingType,
					ResourceType: influxdb.OrgsResourceType,
					ResourceID:   20,
				},
				// NOTE(affo): URMs to buckets are automatically created as for the tests for creation.
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

	// NOTE(affo)(*kv.Service): it does create dangling resources.
	t.Run("deleting bucket urm does not create dangling resources", func(t *testing.T) {
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
		// bucket2 should be deleted (nobody points to it).
		if err := s.DeleteUserResourceMapping(ctx, data.Buckets[1].ID, data.Users[0].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[1].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs > 0 {
			t.Errorf("expected no buckets, got: %v", bs)
			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[1].ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bucket2 should be dangling at this point. These are its urms: %+v", urms)
		}
		// Now delete user1 -> bucket1.
		// bucket1 should be deleted (nobody points to it).
		if err := s.DeleteUserResourceMapping(ctx, data.Buckets[0].ID, data.Users[0].ID); err != nil {
			t.Fatal(err)
		}
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{ID: &data.Buckets[0].ID})
		if err != nil {
			t.Fatal(err)
		}
		if nbs > 0 {
			t.Errorf("expected no buckets, got: %v", bs)
			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[1].ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bucket1 should be dangling at this point. These are its urms: %+v", urms)
		}
	})

	// NOTE(affo)(*kv.Service): it deletes urms, but not dangling resources.
	t.Run("deleting a user deletes every urm and so for dangling resources", func(t *testing.T) {
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
		// We expect his urms deleted and bucket1 deleted (he was the only one pointing at it).
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
		if nbs > 0 {
			t.Errorf("expected no buckets, got: %v", bs)
			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				ResourceType: influxdb.BucketsResourceType,
				ResourceID:   data.Buckets[1].ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("bucket1 should be dangling at this point. These are its urms: %+v", urms)
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
		// We expect urms to those buckets to be deleted too
		// user1 should not be deleted because he is member of org2.
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
		if nbs > 0 {
			t.Errorf("expected no buckets, got: %v", bs)
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
		bs, nbs, err = s.FindBuckets(ctx, influxdb.BucketFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if nbs != 0 {
			t.Errorf("expected no buckets, got: %v", bs)
		}
	})
}
