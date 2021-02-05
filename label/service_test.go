package label_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/label"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltLabelService(t *testing.T) {
	influxdbtesting.LabelService(initBoltLabelService, t)
}

func NewTestBoltStore(t *testing.T) (kv.Store, func(), error) {
	t.Helper()

	f, err := ioutil.TempFile("", "influxdata-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	s := bolt.NewKVStore(logger, path, bolt.WithNoSync)
	if err := s.Open(ctx); err != nil {
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

func initBoltLabelService(f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	s, closeBolt, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, op, closeSvc := initLabelService(s, f, t)
	return svc, op, func() {
		closeSvc()
		closeBolt()
	}
}

func initLabelService(s kv.Store, f influxdbtesting.LabelFields, t *testing.T) (influxdb.LabelService, string, func()) {
	st, err := label.NewStore(s)
	if err != nil {
		t.Fatalf("failed to create label store: %v", err)
	}

	if f.IDGenerator != nil {
		st.IDGenerator = f.IDGenerator
	}

	svc := label.NewService(st)
	ctx := context.Background()

	for _, l := range f.Labels {
		mock.SetIDForFunc(&st.IDGenerator, l.ID, func() {
			if err := svc.CreateLabel(ctx, l); err != nil {
				t.Fatalf("failed to populate labels: %v", err)
			}
		})
	}

	for _, m := range f.Mappings {
		if err := svc.CreateLabelMapping(ctx, m); err != nil {
			t.Fatalf("failed to populate label mappings: %v", err)
		}
	}

	return svc, kv.OpPrefix, func() {
		for _, l := range f.Labels {
			if err := svc.DeleteLabel(ctx, l.ID); err != nil {
				t.Logf("failed to remove label: %v", err)
			}
		}
	}
}
