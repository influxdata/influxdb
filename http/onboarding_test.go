package http

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/mock"
	"go.uber.org/zap"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/kv"
	platformtesting "github.com/influxdata/influxdb/testing"
)

// NewMockSetupBackend returns a SetupBackend with mock services.
func NewMockSetupBackend() *SetupBackend {
	return &SetupBackend{
		Logger:            zap.NewNop().With(zap.String("handler", "scraper")),
		OnboardingService: mock.NewOnboardingService(),
	}
}

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	t.Helper()
	ctx := context.Background()
	svc := kv.NewService(inmem.NewKVStore())
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing kv service: %v", err)
	}

	if err := svc.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}

	setupBackend := NewMockSetupBackend()
	setupBackend.OnboardingService = svc
	handler := NewSetupHandler(setupBackend)
	server := httptest.NewServer(handler)
	client := struct {
		*SetupService
		*Service
		platform.PasswordsService
	}{
		SetupService: &SetupService{
			Addr: server.URL,
		},
		Service: &Service{
			Addr: server.URL,
		},
		PasswordsService: svc,
	}

	done := server.Close

	return client, done
}
func TestOnboardingService(t *testing.T) {
	platformtesting.Generate(initOnboardingService, t)
}
