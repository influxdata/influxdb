package bolt_test

import (
	"context"
	"testing"

	"github.com/influxdata/platform"
	platformtesting "github.com/influxdata/platform/testing"
)

func initOnboardingService(f platformtesting.OnboardingFields, t *testing.T) (platform.OnboardingService, func()) {
	c, closeFn, err := NewTestClient()
	if err != nil {
		t.Fatalf("failed to create new bolt client: %v", err)
	}
	c.IDGenerator = f.IDGenerator
	c.TokenGenerator = f.TokenGenerator
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

func TestGenerate(t *testing.T) {
	platformtesting.Generate(initOnboardingService, t)
}
