package kv_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb"
	icontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/kv"
	itesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltTenantService(t *testing.T) {
	t.Skip("some tests fail using `*kv.Service` as `influxdb.TenantService`, see the NOTEs in `testing/tenant.go`.")
	itesting.TenantService(t, initBoltTenantService)
}

func initBoltTenantService(t *testing.T, f itesting.TenantFields) (influxdb.TenantService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc := kv.NewService(zaptest.NewLogger(t), s)

	// Create a mapping from user-specified IDs to kv generated ones.
	// This way we can map the user intent to the actual IDs.
	uIDs := make(map[influxdb.ID]influxdb.ID, len(f.Users))
	oIDs := make(map[influxdb.ID]influxdb.ID, len(f.Organizations))
	bIDs := make(map[influxdb.ID]influxdb.ID, len(f.Buckets))

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing authorization service: %v", err)
	}
	for _, u := range f.Users {
		id := u.ID
		if err := svc.CreateUser(ctx, u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
		uIDs[id] = u.ID
	}
	for i := range f.Passwords {
		if err := svc.SetPassword(ctx, f.Users[i].ID, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	// Preserve only the urms that do not express ownership of an org.
	// Those will be expressed on org creation through the authorizer.
	urms := f.UserResourceMappings[:0]
	orgOwners := make(map[influxdb.ID]influxdb.ID)
	for _, urm := range f.UserResourceMappings {
		if urm.ResourceType == influxdb.OrgsResourceType && urm.UserType == influxdb.Owner {
			orgOwners[urm.ResourceID] = urm.UserID
		} else {
			urms = append(urms, urm)
		}
	}
	for _, o := range f.Organizations {
		id := o.ID
		ctx := context.Background()
		ctx = icontext.SetAuthorizer(ctx, &influxdb.Authorization{
			UserID: uIDs[orgOwners[id]],
		})
		if err := svc.CreateOrganization(ctx, o); err != nil {
			t.Fatalf("error populating orgs: %v", err)
		}
		oIDs[id] = o.ID
	}
	for _, b := range f.Buckets {
		id := b.ID
		b.OrgID = oIDs[b.OrgID]
		if err := svc.CreateBucket(ctx, b); err != nil {
			t.Fatalf("error populating buckets: %v", err)
		}
		bIDs[id] = b.ID
	}
	for _, urm := range urms {
		urm.UserID = uIDs[urm.UserID]
		switch urm.ResourceType {
		case influxdb.BucketsResourceType:
			urm.ResourceID = bIDs[urm.ResourceID]
		case influxdb.OrgsResourceType:
			urm.ResourceID = oIDs[urm.ResourceID]
		default:
			panic(fmt.Errorf("invalid resource type: %v", urm.ResourceType))
		}
		if err := svc.CreateUserResourceMapping(ctx, urm); err != nil {
			t.Fatalf("error populating urms: %v", err)
		}
	}

	return svc, func() {
		closeStore()
	}
}
