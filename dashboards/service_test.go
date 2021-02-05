package dashboards

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	dashboardtesting "github.com/influxdata/influxdb/v2/dashboards/testing"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	"go.uber.org/zap/zaptest"
)

func TestBoltDashboardService(t *testing.T) {
	dashboardtesting.DashboardService(initBoltDashboardService, t)
}

func initBoltDashboardService(f dashboardtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	s, closeBolt, err := newTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initDashboardService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initDashboardService(s kv.SchemaStore, f dashboardtesting.DashboardFields, t *testing.T) (influxdb.DashboardService, string, func()) {
	if f.TimeGenerator == nil {
		f.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	ctx := context.Background()
	kvSvc := kv.NewService(zaptest.NewLogger(t), s, &mock.OrganizationService{})
	kvSvc.IDGenerator = f.IDGenerator
	kvSvc.TimeGenerator = f.TimeGenerator

	svc := NewService(s, kvSvc)
	svc.IDGenerator = f.IDGenerator
	svc.TimeGenerator = f.TimeGenerator

	for _, b := range f.Dashboards {
		if err := svc.PutDashboard(ctx, b); err != nil {
			t.Fatalf("failed to populate dashboards")
		}
	}
	return svc, kv.OpPrefix, func() {
		for _, b := range f.Dashboards {
			if err := svc.DeleteDashboard(ctx, b.ID); err != nil {
				t.Logf("failed to remove dashboard: %v", err)
			}
		}
	}
}

func newTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	path := f.Name()

	// skip fsync to improve test performance
	s := bolt.NewKVStore(logger, path, bolt.WithNoSync)
	if err := s.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	if err := all.Up(ctx, logger, s); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}
