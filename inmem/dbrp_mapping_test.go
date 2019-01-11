package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initDBRPMappingService(f platformtesting.DBRPMappingFields, t *testing.T) (platform.DBRPMappingService, func()) {
	s := NewService()
	ctx := context.TODO()
	if err := f.Populate(ctx, s); err != nil {
		t.Fatal(err)
	}
	return s, func() {}
}

func TestDBRPMappingService_CreateDBRPMapping(t *testing.T) {
	t.Parallel()
	platformtesting.CreateDBRPMapping(initDBRPMappingService, t)
}

func TestDBRPMappingService_FindDBRPMappingByKey(t *testing.T) {
	t.Parallel()
	platformtesting.FindDBRPMappingByKey(initDBRPMappingService, t)
}

func TestDBRPMappingService_FindDBRPMappings(t *testing.T) {
	t.Parallel()
	platformtesting.FindDBRPMappings(initDBRPMappingService, t)
}

func TestDBRPMappingService_DeleteDBRPMapping(t *testing.T) {
	t.Parallel()
	platformtesting.DeleteDBRPMapping(initDBRPMappingService, t)
}

func TestDBRPMappingService_FindDBRPMapping(t *testing.T) {
	t.Parallel()
	platformtesting.FindDBRPMapping(initDBRPMappingService, t)
}
