package udp_test

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/diagnostic"
	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/udp"
)

func TestService_OpenClose(t *testing.T) {
	service := NewTestService(nil)

	// Closing a closed service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	// Closing a closed service again is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Opening an already open service is fine.
	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Reopening a previously opened service is fine.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}

	if err := service.Service.Open(); err != nil {
		t.Fatal(err)
	}

	// Tidy up.
	if err := service.Service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestService_CreatesDatabase(t *testing.T) {
	t.Parallel()

	s := NewTestService(nil)
	s.WritePointsFn = func(string, string, models.ConsistencyLevel, []models.Point) error {
		return nil
	}

	called := make(chan struct{})
	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != s.Config.Database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", s.Config.Database, name)
		}
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, errors.New("an error")
	}

	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}

	points, err := models.ParsePointsString(`cpu value=1`)
	if err != nil {
		t.Fatal(err)
	}

	batcher := s.Service.Batcher()
	batcher.In() <- points[0] // Send a point.
	batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	ready := s.Service.Ready()

	if got, exp := ready, false; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	// This time MC won't cause an error.
	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		// Allow some time for the caller to return and the ready status to
		// be set.
		time.AfterFunc(10*time.Millisecond, func() { called <- struct{}{} })
		return nil, nil
	}

	batcher.In() <- points[0] // Send a point.
	batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should now be true.
	ready = s.Service.Ready()

	if got, exp := ready, true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	s.Service.Close()
}

type TestService struct {
	Service       *udp.Service
	Config        udp.Config
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
}

func NewTestService(c *udp.Config) *TestService {
	if c == nil {
		defaultC := udp.NewConfig()
		c = &defaultC
	}

	service := &TestService{
		Service:    udp.NewService(*c),
		Config:     *c,
		MetaClient: &internal.MetaClientMock{},
	}

	if testing.Verbose() {
		diag := diagnostic.New(os.Stderr)
		service.Service.With(diag.UDPContext())
	}

	service.Service.MetaClient = service.MetaClient
	service.Service.PointsWriter = service
	return service
}

func (s *TestService) WritePointsPrivileged(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(database, retentionPolicy, consistencyLevel, points)
}
