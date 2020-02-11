package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/snowflake"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltAuthorizationService(t *testing.T) {
	influxdbtesting.AuthorizationService(initBoltAuthorizationService, t)
}

func initBoltAuthorizationService(f influxdbtesting.AuthorizationFields, t *testing.T) (influxdb.AuthorizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initAuthorizationService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initAuthorizationService(s kv.Store, f influxdbtesting.AuthorizationFields, t testable) (influxdb.AuthorizationService, string, func()) {
	var (
		ctx = context.Background()
		svc = kv.NewService(zaptest.NewLogger(t), s, kv.ServiceConfigForTest())
	)

	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	svc.TimeGenerator = f.TimeGenerator

	if !f.AuthsPopulateIndexOnPut {
		ctx = kv.AuthSkipIndexOnPut(ctx)
	}

	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing authorization service: %v", err)
	}

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, o := range f.Orgs {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate orgs")
		}
	}

	for _, a := range f.Authorizations {
		if err := svc.PutAuthorization(ctx, a); err != nil {
			t.Fatalf("failed to populate authorizations %s", err)
		}
	}

	return svc, kv.OpPrefix, func() {
		// cleanup assets
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("failed to remove user: %v", err)
			}
		}

		for _, o := range f.Orgs {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove org: %v", err)
			}
		}

		for _, a := range f.Authorizations {
			if err := svc.DeleteAuthorization(ctx, a.ID); err != nil {
				t.Logf("failed to remove authorizations: %v", err)
			}
		}
	}
}

func Test_AuthorizationService_FindAuthorizations_ByUserIndex(t *testing.T) {
	var (
		idgen           = snowflake.NewDefaultIDGenerator()
		userOneID, _    = influxdb.IDFromString("05392292e0f9f000")
		userTwoID       = idgen.ID()
		orgOneID        = idgen.ID()
		authOneID, _    = influxdb.IDFromString("05392292e0f9f001")
		authTwoID       = idgen.ID()
		authThreeID, _  = influxdb.IDFromString("05392292e0f9f003")
		encodedOne, _   = authOneID.Encode()
		encodedThree, _ = authThreeID.Encode()
		fields          = influxdbtesting.AuthorizationFields{
			Users: []*platform.User{
				{
					Name: "cooluser",
					ID:   *userOneID,
				},
				{
					Name: "regularuser",
					ID:   userTwoID,
				},
			},
			Orgs: []*platform.Organization{
				{
					Name: "o1",
					ID:   orgOneID,
				},
			},
			Authorizations: []*platform.Authorization{
				{
					ID:     *authOneID,
					UserID: *userOneID,
					OrgID:  orgOneID,
					Token:  "rand1",
					Status: platform.Active,
				},
				{
					ID:     authTwoID,
					UserID: userTwoID,
					OrgID:  orgOneID,
					Token:  "rand2",
				},
				{
					ID:     *authThreeID,
					UserID: *userOneID,
					OrgID:  orgOneID,
					Token:  "rand3",
				},
			},
			// given the index is not initially populated
			AuthsPopulateIndexOnPut: false,
		}
		st = inmem.NewKVStore()
	)

	initAuthorizationService(st, fields, t)

	svc := kv.NewService(zaptest.NewLogger(t), st, kv.ServiceConfigForTest())
	svc.FindAuthorizations(context.Background(), influxdb.AuthorizationFilter{
		UserID: userOneID,
	})

	// expect indexer to have been called with following args
	kv.AssertIndexesWereCreated(t,
		svc,
		kv.AddToIndexCall{
			Bucket: []byte("authorizationbyuserindexv1"),
			Keys: map[string][]byte{
				"05392292e0f9f000/05392292e0f9f001": encodedOne,
			},
		}, kv.AddToIndexCall{
			Bucket: []byte("authorizationbyuserindexv1"),
			Keys: map[string][]byte{
				"05392292e0f9f000/05392292e0f9f003": encodedThree,
			},
		})
}
