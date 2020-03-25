package kv_test

import (
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tenant"
	influxdbtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

func initBoltDuplicateReadBucketService(f influxdbtesting.BucketFields, t *testing.T) (influxdb.BucketService, string, func()) {
	s, closeInMem, err := NewTestInmemStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc, op, closeSvc := initBucketService(s, f, t)
	ro, err := tenant.NewReadOnlyStore(s)
	if err != nil {
		t.Fatal(err)
	}
	newSvc := tenant.NewService(ro)
	svc = tenant.NewDuplicateReadBucketService(zaptest.NewLogger(t), svc, newSvc)
	return svc, op, func() {
		closeSvc()
		closeInMem()
	}
}

func TestBoltDuplicateReadBucketService(t *testing.T) {
	influxdbtesting.BucketService(initBoltDuplicateReadBucketService, t)
}

func initBoltDuplicateReadOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc, op, closeSvc := initOrganizationService(s, f, t)
	ro, err := tenant.NewReadOnlyStore(s)
	if err != nil {
		t.Fatal(err)
	}
	newSvc := tenant.NewService(ro)
	svc = tenant.NewDuplicateReadOrganizationService(zaptest.NewLogger(t), svc, newSvc)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func TestBoltDuplicateReadOrganizationService(t *testing.T) {
	influxdbtesting.OrganizationService(initBoltDuplicateReadOrganizationService, t)
}

func initBoltDuplicateReadUserResourceMappingService(f influxdbtesting.UserResourceFields, t *testing.T) (influxdb.UserResourceMappingService, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc, closeSvc := initUserResourceMappingService(s, f, t)
	ro, err := tenant.NewReadOnlyStore(s)
	if err != nil {
		t.Fatal(err)
	}
	newSvc := tenant.NewService(ro)
	svc = tenant.NewDuplicateReadUserResourceMappingService(zaptest.NewLogger(t), svc, newSvc)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func TestBoltDuplicateReadUserResourceMappingService(t *testing.T) {
	influxdbtesting.UserResourceMappingService(initBoltDuplicateReadUserResourceMappingService, t)
}

func initBoltDuplicateReadUserService(f influxdbtesting.UserFields, t *testing.T) (influxdb.UserService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc, op, closeSvc := initUserService(s, f, t)
	ro, err := tenant.NewReadOnlyStore(s)
	if err != nil {
		t.Fatal(err)
	}
	newSvc := tenant.NewService(ro)
	svc = tenant.NewDuplicateReadUserService(zaptest.NewLogger(t), svc, newSvc)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func TestBoltDuplicateReadUserService(t *testing.T) {
	influxdbtesting.UserService(initBoltDuplicateReadUserService, t)
}

func initBoltDuplicateReadPasswordsService(f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}
	svc, closeSvc := initPasswordsService(s, f, t)
	ro, err := tenant.NewReadOnlyStore(s)
	if err != nil {
		t.Fatal(err)
	}
	newSvc := tenant.NewService(ro)
	svc = tenant.NewDuplicateReadPasswordsService(zaptest.NewLogger(t), svc, newSvc)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func TestBoltDuplicateReadPasswordService(t *testing.T) {
	influxdbtesting.PasswordsService(initBoltDuplicateReadPasswordsService, t)
}
