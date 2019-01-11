package http

import (
	"context"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator

	ctx := context.Background()
	if err := svc.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}

	handler := NewSetupHandler()
	handler.OnboardingService = svc
	server := httptest.NewServer(handler)
	client := struct {
		*SetupService
		*Service
		platform.BasicAuthService
	}{
		SetupService: &SetupService{
			Addr: server.URL,
		},
		Service: &Service{
			Addr: server.URL,
		},
		BasicAuthService: svc,
	}

	done := server.Close

	return client, done
}
func TestOnboardingService(t *testing.T) {
	platformtesting.Generate(initOnboardingService, t)
}
