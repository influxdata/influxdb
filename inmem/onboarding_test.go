package inmem

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	s := NewService()
	s.IDGenerator = f.IDGenerator
	s.TokenGenerator = f.TokenGenerator
	s.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		s.TimeGenerator = platform.RealTimeGenerator{}
	}
	ctx := context.TODO()
	if err := s.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}
	return s, func() {}
}

func TestGenerate(t *testing.T) {
	t.Skip("these are no longer being used, using kv inmem instead")
	platformtesting.Generate(initOnboardingService, t)
}
