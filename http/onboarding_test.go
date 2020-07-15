package http

import (
	"context"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

// NewMockSetupBackend returns a SetupBackend with mock services.
func NewMockSetupBackend(t *testing.T) *SetupBackend {
	return &SetupBackend{
		log:               zaptest.NewLogger(t),
		OnboardingService: mock.NewOnboardingService(),
	}
}

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	t.Helper()
	store := NewTestInmemStore(t)
	svc := kv.NewService(zaptest.NewLogger(t), store)
	svc.IDGenerator = f.IDGenerator
	svc.OrgBucketIDs = f.IDGenerator
	svc.TokenGenerator = f.TokenGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = platform.RealTimeGenerator{}
	}
	svc.TimeGenerator = f.TimeGenerator

	ctx := context.Background()
	if err := svc.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}

	setupBackend := NewMockSetupBackend(t)
	setupBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	setupBackend.OnboardingService = svc
	handler := NewSetupHandler(zaptest.NewLogger(t), setupBackend)
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
	platformtesting.OnboardInitialUser(initOnboardingService, t)
}
