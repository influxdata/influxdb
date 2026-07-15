package opentsdb

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/influxdata/influxdb/pkg/testing/selfsigned"
	"github.com/influxdata/influxdb/pkg/tlsconfig"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func Test_Service_OpenClose(t *testing.T) {
	// Let the OS assign a random port since we are only opening and closing the service,
	// not actually connecting to it.
	service := NewTestService(t, "db0", "127.0.0.1:0")

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

// Ensure a point can be written via the telnet protocol.
func TestService_CreatesDatabase(t *testing.T) {
	t.Parallel()

	database := "db0"
	s := NewTestService(t, database, "127.0.0.1:0")
	s.WritePointsFn = func(tsdb.WriteContext, string, string, models.ConsistencyLevel, []models.Point) error {
		return nil
	}

	called := make(chan struct{})
	s.MetaClient.CreateDatabaseFn = func(name string) (*meta.DatabaseInfo, error) {
		if name != database {
			t.Errorf("\n\texp = %s\n\tgot = %s\n", database, name)
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

	s.Service.batcher.In() <- points[0] // Send a point.
	s.Service.batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	s.Service.mu.RLock()
	ready := s.Service.ready
	s.Service.mu.RUnlock()

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

	s.Service.batcher.In() <- points[0] // Send a point.
	s.Service.batcher.Flush()
	select {
	case <-called:
		// OK
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("Service should have attempted to create database")
	}

	// ready status should not have been switched due to meta client error.
	s.Service.mu.RLock()
	ready = s.Service.ready
	s.Service.mu.RUnlock()

	if got, exp := ready, true; got != exp {
		t.Fatalf("got %v, expected %v", got, exp)
	}

	s.Service.Close()
}

// Ensure a point can be written via the telnet protocol.
func TestService_Telnet(t *testing.T) {
	t.Parallel()

	s := NewTestService(t, "db0", "127.0.0.1:0")
	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Service.Close()

	// Mock points writer.
	var called int32
	s.WritePointsFn = func(_ tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		atomic.StoreInt32(&called, 1)

		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.user",
				models.NewTags(map[string]string{"host": "webserver01", "cpu": "0"}),
				map[string]interface{}{"value": 42.5},
				time.Unix(1356998400, 0),
			),
		}) {
			t.Fatalf("unexpected points: %#v", points)
		}
		return nil
	}

	// Open connection to the service.
	conn, err := net.Dial("tcp", s.Service.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Write telnet data and close.
	if _, err := conn.Write([]byte("put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0")); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	tick := time.Tick(10 * time.Millisecond)
	timeout := time.After(10 * time.Second)

	for {
		select {
		case <-tick:
			// Verify that the writer was called.
			if atomic.LoadInt32(&called) > 0 {
				return
			}
		case <-timeout:
			t.Fatal("points writer not called")
		}
	}
}

// Ensure a point can be written via the HTTP protocol.
func TestService_HTTP(t *testing.T) {
	t.Parallel()

	s := NewTestService(t, "db0", "127.0.0.1:0")
	if err := s.Service.Open(); err != nil {
		t.Fatal(err)
	}
	defer s.Service.Close()

	// Mock points writer.
	var called bool
	s.WritePointsFn = func(_ tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
		called = true
		if database != "db0" {
			t.Fatalf("unexpected database: %s", database)
		} else if retentionPolicy != "" {
			t.Fatalf("unexpected retention policy: %s", retentionPolicy)
		} else if !reflect.DeepEqual(points, []models.Point{
			models.MustNewPoint(
				"sys.cpu.nice",
				models.NewTags(map[string]string{"dc": "lga", "host": "web01"}),
				map[string]interface{}{"value": 18.0},
				time.Unix(1346846400, 0),
			),
		}) {
			spew.Dump(points)
			t.Fatalf("unexpected points: %#v", points)
		}
		return nil
	}

	// Write HTTP request to server.
	resp, err := http.Post("http://"+s.Service.Addr().String()+"/api/put", "application/json", strings.NewReader(`{"metric":"sys.cpu.nice", "timestamp":1346846400, "value":18, "tags":{"host":"web01", "dc":"lga"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Verify status and body.
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	// Verify that the writer was called.
	if !called {
		t.Fatal("points writer not called")
	}
}

type TestService struct {
	t             *testing.T
	Service       *Service
	MetaClient    *internal.MetaClientMock
	WritePointsFn func(ctx tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	certMonitor   *tlsconfig.TLSCertMonitor
}

// NewTestService returns a new instance of Service.
func NewTestService(t *testing.T, database string, bind string) *TestService {
	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	// The monitor has to outlive this helper, so close it when the test ends
	// rather than when the helper returns.
	t.Cleanup(th.CheckedClose(t, certMonitor))

	s, err := NewService(Config{
		BindAddress:      bind,
		Database:         database,
		ConsistencyLevel: "one",
	}, certMonitor)
	require.NoError(t, err)

	service := &TestService{
		Service:     s,
		MetaClient:  &internal.MetaClientMock{},
		certMonitor: certMonitor,
	}

	service.MetaClient.CreateDatabaseFn = func(db string) (*meta.DatabaseInfo, error) {
		if got, exp := db, database; got != exp {
			return nil, fmt.Errorf("got %v, expected %v", got, exp)
		}
		return nil, nil
	}

	if testing.Verbose() {
		service.Service.WithLogger(logger.New(os.Stderr))
	}

	service.Service.MetaClient = service.MetaClient
	service.Service.PointsWriter = service
	return service
}

func (s *TestService) Close() error {
	var allErrs []error

	if err := s.certMonitor.Close(); err != nil {
		allErrs = append(allErrs, fmt.Errorf("error closing cert monitor: %w", err))
	}
	if err := s.Service.Close(); err != nil {
		allErrs = append(allErrs, fmt.Errorf("error closing opentsdb service: %w", err))
	}

	return errors.Join(allErrs...)
}

func (s *TestService) WritePointsPrivileged(ctx tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return s.WritePointsFn(ctx, database, retentionPolicy, consistencyLevel, points)
}

// pointsWriterFunc adapts a function to the Service.PointsWriter interface.
type pointsWriterFunc func(ctx tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error

func (f pointsWriterFunc) WritePointsPrivileged(ctx tsdb.WriteContext, database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error {
	return f(ctx, database, retentionPolicy, consistencyLevel, points)
}

// TestService_ClientCertAuth verifies that, when configured with a client auth
// type and a client CA, the opentsdb service enforces mutual TLS: a client
// presenting a certificate signed by the configured CA is authenticated, while
// a client presenting no certificate or an untrusted one is rejected during the
// TLS handshake.
func TestService_ClientCertAuth(t *testing.T) {
	// serverSS is the server's TLS certificate; clientSS provides the client
	// certificate and the CA (clientSS.CACertPath) the server is told to trust
	// for client authentication.
	serverSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithDNSName("localhost"))
	clientSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("client", "Client CA"))

	certMonitor := tlsconfig.NewTLSCertMonitor()
	require.NoError(t, certMonitor.Open())
	defer th.CheckedClose(t, certMonitor)()

	authType := toml.TlsClientAuthType(tls.RequireAndVerifyClientCert)
	s, err := NewService(Config{
		BindAddress:      "127.0.0.1:0",
		Database:         "db0",
		ConsistencyLevel: "one",
		TLSEnabled:       true,
		Certificate:      serverSS.CertPath,
		PrivateKey:       serverSS.KeyPath,
		ClientAuthType:   &authType,
		ClientCA:         &tlsconfig.CAConfig{Paths: []string{clientSS.CACertPath}},
		TLS:              new(tls.Config),
	}, certMonitor)
	require.NoError(t, err)

	// Wire the minimal dependencies so /api/put succeeds once the handshake passes.
	s.MetaClient = &internal.MetaClientMock{
		CreateDatabaseFn: func(string) (*meta.DatabaseInfo, error) { return nil, nil },
	}
	s.PointsWriter = pointsWriterFunc(func(tsdb.WriteContext, string, string, models.ConsistencyLevel, []models.Point) error {
		return nil
	})

	require.NoError(t, s.Open())
	defer s.Close()

	putURL := "https://" + s.Addr().String() + "/api/put"
	body := `{"metric":"sys.cpu.nice","timestamp":1346846400,"value":18,"tags":{"host":"web01","dc":"lga"}}`

	post := func(clientTLS *tls.Config) (*http.Response, error) {
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS}}
		return client.Post(putURL, "application/json", strings.NewReader(body))
	}

	t.Run("client with trusted certificate is authenticated", func(t *testing.T) {
		// Present the client certificate; skip server-cert verification so the
		// test isolates client authentication.
		resp, err := post(clientSS.ClientTLSConfig(t, true, true))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.NotNil(t, resp.TLS)
		require.NotEmpty(t, resp.TLS.PeerCertificates, "handshake should have completed with the server cert")
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})

	t.Run("client without certificate is rejected", func(t *testing.T) {
		resp, err := post(&tls.Config{InsecureSkipVerify: true})
		if resp != nil {
			resp.Body.Close()
		}
		require.Error(t, err, "server should reject a client without a certificate")
		require.ErrorContains(t, err, "certificate required")
	})

	t.Run("client with untrusted certificate is rejected", func(t *testing.T) {
		otherSS := selfsigned.NewSelfSignedCert(t, selfsigned.WithCASubject("other", "Other CA"))
		resp, err := post(otherSS.ClientTLSConfig(t, true, true))
		if resp != nil {
			resp.Body.Close()
		}
		require.Error(t, err, "server should reject a client with an untrusted certificate")
	})
}

// TestService_TLSUsage covers the service naming itself to the certificate
// monitor. A server can run several OpenTSDB services at once, so the usage
// carries the bind address; otherwise the monitor, which groups its warnings by
// usage, could not tell two of them apart.
func TestService_TLSUsage(t *testing.T) {
	serverSS := selfsigned.NewSelfSignedCert(t)

	core, logs := observer.New(zapcore.InfoLevel)
	certMonitor := tlsconfig.NewTLSCertMonitor(tlsconfig.WithMonitorLogger(zap.New(core)))
	require.NoError(t, certMonitor.Open())
	t.Cleanup(th.CheckedClose(t, certMonitor))

	const bind = "127.0.0.1:0"
	s, err := NewService(Config{
		BindAddress:      bind,
		Database:         "db0",
		ConsistencyLevel: "one",
		TLSEnabled:       true,
		Certificate:      serverSS.CertPath,
		PrivateKey:       serverSS.KeyPath,
		TLS:              new(tls.Config),
	}, certMonitor)
	require.NoError(t, err)

	s.MetaClient = &internal.MetaClientMock{
		CreateDatabaseFn: func(string) (*meta.DatabaseInfo, error) { return nil, nil },
	}
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	entries := logs.FilterMessage("Registered certificate loader").TakeAll()
	require.Len(t, entries, 1)
	require.Equal(t, "opentsdb("+bind+").server", entries[0].ContextMap()["usage"])
}
