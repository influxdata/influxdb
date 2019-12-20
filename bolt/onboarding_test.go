package bolt_test

import (
	"context"
	"testing"

	platform "github.com/influxdata/influxdb/v2"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
)

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	c, closeFn, err := NewTestClient(t)
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TokenGenerator = f.TokenGenerator
	c.TimeGenerator = f.TimeGenerator
	if c.TimeGenerator == nil {
		c.TimeGenerator = platform.RealTimeGenerator{}
	}
	ctx := context.TODO()
	if err = c.PutOnboardingStatus(ctx, !f.IsOnboarding); err != nil {
		t.Fatalf("failed to set new onboarding finished: %v", err)
	}

	return c, func() {
		defer closeFn()
		if err := c.PutOnboardingStatus(ctx, false); err != nil {
			t.Logf("failed to remove onboarding finished: %v", err)
		}
	}
}

func TestOnboardingService_Generate(t *testing.T) {
	t.Skip("This service is not used, we use the kv bolt implementation")
	platformtesting.Generate(initOnboardingService, t)
}
