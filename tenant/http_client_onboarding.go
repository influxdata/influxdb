package tenant

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// OnboardClientService connects to Influx via HTTP to perform onboarding operations
type OnboardClientService struct {
	Client *httpc.Client
}

// IsOnboarding determine if onboarding request is allowed.
func (s *OnboardClientService) IsOnboarding(ctx context.Context) (bool, error) {
	var resp isOnboardingResponse
	err := s.Client.
		Get(prefixOnboard).
		DecodeJSON(&resp).
		Do(ctx)

	if err != nil {
		return false, err
	}
	return resp.Allowed, nil
}

// OnboardInitialUser OnboardingResults.
func (s *OnboardClientService) OnboardInitialUser(ctx context.Context, or *influxdb.OnboardingRequest) (*influxdb.OnboardingResults, error) {
	res := &onboardingResponse{}

	err := s.Client.
		PostJSON(or, prefixOnboard).
		DecodeJSON(res).
		Do(ctx)

	if err != nil {
		return nil, err
	}

	return &influxdb.OnboardingResults{
		Org:    &res.Organization.Organization,
		User:   &res.User.User,
		Auth:   res.Auth.toPlatform(),
		Bucket: res.Bucket.toInfluxDB(),
	}, nil
}
