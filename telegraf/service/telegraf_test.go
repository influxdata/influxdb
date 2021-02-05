package service_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	influxdb "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	telegraftesting "github.com/influxdata/influxdb/v2/telegraf/service/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltTelegrafService(t *testing.T) {
	telegraftesting.TelegrafConfigStore(initBoltTelegrafService, t)
}

func NewTestBoltStore(t *testing.T) (kv.SchemaStore, func(), error) {
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

func initBoltTelegrafService(f telegraftesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initTelegrafService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initTelegrafService(s kv.SchemaStore, f telegraftesting.TelegrafConfigFields, t *testing.T) (influxdb.TelegrafConfigStore, func()) {
	ctx := context.Background()

	svc := telegrafservice.New(s)
	svc.IDGenerator = f.IDGenerator

	for _, tc := range f.TelegrafConfigs {
		if err := svc.PutTelegrafConfig(ctx, tc); err != nil {
			t.Fatalf("failed to populate telegraf config: %v", err)
		}
	}

	return svc, func() {
		for _, tc := range f.TelegrafConfigs {
			if err := svc.DeleteTelegrafConfig(ctx, tc.ID); err != nil {
				t.Logf("failed to remove telegraf config: %v", err)
			}
		}
	}
}
