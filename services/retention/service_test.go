package retention_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/uber-go/zap"
)

func TestService_OpenDisabled(t *testing.T) {
	// Opening a disabled service should be a no-op.
	c := retention.NewConfig()
	c.Enabled = false
	s := NewService(c)

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() != "" {
		t.Fatalf("service logged %q, didn't expect any logging", s.LogBuf.String())
	}
}

func TestService_OpenClose(t *testing.T) {
	// Opening a disabled service should be a no-op.
	s := NewService(retention.NewConfig())

	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if s.LogBuf.String() == "" {
		t.Fatal("service didn't log anything on open")
	}

	// Reopening is a no-op
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Re-closing is a no-op
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

type Service struct {
	MetaClient *internal.MetaClientMock
	TSDBStore  *internal.TSDBStoreMock

	LogBuf bytes.Buffer
	*retention.Service
}

func NewService(c retention.Config) *Service {
	s := &Service{
		MetaClient: &internal.MetaClientMock{},
		TSDBStore:  &internal.TSDBStoreMock{},
		Service:    retention.NewService(c),
	}

	l := zap.New(
		zap.NewTextEncoder(),
		zap.Output(zap.AddSync(&s.LogBuf)),
	)
	s.WithLogger(l)

	s.Service.MetaClient = s.MetaClient
	s.Service.TSDBStore = s.TSDBStore
	return s
}
