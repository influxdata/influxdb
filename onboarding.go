package influxdb

import (
	"context"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

// OnboardingService represents a service for the first run.
type OnboardingService interface {
	// IsOnboarding determine if onboarding request is allowed.
	IsOnboarding(ctx context.Context) (bool, error)

	// OnboardInitialUser creates the initial org/user/bucket in the DB.
	OnboardInitialUser(ctx context.Context, req *OnboardingRequest) (*OnboardingResults, error)
}

// OnboardingResults is a group of elements required for first run.
type OnboardingResults struct {
	User   *User          `json:"user"`
	Org    *Organization  `json:"org"`
	Bucket *Bucket        `json:"bucket"`
	Auth   *Authorization `json:"auth"`
}

// OnboardingRequest is the request
// to setup defaults.
type OnboardingRequest struct {
	User                      string        `json:"username"`
	Password                  string        `json:"password"`
	Org                       string        `json:"org"`
	Bucket                    string        `json:"bucket"`
	RetentionPeriodSeconds    int64         `json:"retentionPeriodSeconds,omitempty"`
	RetentionPeriodDeprecated time.Duration `json:"retentionPeriodHrs,omitempty"`
	Token                     string        `json:"token,omitempty"`
}

func (r *OnboardingRequest) Valid() error {
	if r.User == "" {
		return &errors.Error{
			Code: errors.EEmptyValue,
			Msg:  "username is empty",
		}
	}

	if r.Org == "" {
		return &errors.Error{
			Code: errors.EEmptyValue,
			Msg:  "org name is empty",
		}
	}

	if r.Bucket == "" {
		return &errors.Error{
			Code: errors.EEmptyValue,
			Msg:  "bucket name is empty",
		}
	}
	return nil
}

func (r *OnboardingRequest) RetentionPeriod() time.Duration {
	if r.RetentionPeriodSeconds > 0 {
		return time.Duration(r.RetentionPeriodSeconds) * time.Second
	}
	return r.RetentionPeriodDeprecated
}
