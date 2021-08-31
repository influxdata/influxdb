package transport

import (
	"context"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// FIXME: Ideally we wouldn't need to keep this hand-rolled HTTP client in the influxdb codebase now that we've
//  moved the CLI to its own repo, but existing tests rely upon it so we leave it for now.
type OnboardingClient struct {
	Client *httpc.Client
}

var _ platform.OnboardingService = (*OnboardingClient)(nil)

func (c *OnboardingClient) IsOnboarding(ctx context.Context) (bool, error) {
	var res isOnboardingResponse
	if err := c.Client.Get(prefixOnboard).DecodeJSON(&res).Do(ctx); err != nil {
		return false, err
	}
	return res.Allowed, nil
}

func (c *OnboardingClient) OnboardInitialUser(ctx context.Context, or *platform.OnboardingRequest) (*platform.OnboardingResults, error) {
	var res response
	if err := c.Client.PostJSON(or, prefixOnboard).DecodeJSON(&res).Do(ctx); err != nil {
		return nil, err
	}
	return &platform.OnboardingResults{
		Org:    &res.Organization.Organization,
		User:   &res.User.User,
		Auth:   res.Auth.ToPlatform(),
		Bucket: res.Bucket.ToInfluxDB(),
	}, nil
}
