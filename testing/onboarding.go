package testing

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
)

// OnboardingFields will include the IDGenerator, TokenGenerator
// and IsOnboarding
type OnboardingFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	IsOnboarding   bool
}

// OnBoardingNBasicAuthService includes onboarding service
// and basic auth service.
type OnBoardingNBasicAuthService interface {
	platform.OnboardingService
	platform.BasicAuthService
}

// Generate testing
func Generate(
	init func(OnboardingFields, *testing.T) (OnBoardingNBasicAuthService, func()),
	t *testing.T,
) {
	type args struct {
		request *platform.OnboardingRequest
	}
	type wants struct {
		errCode  string
		results  *platform.OnboardingResults
		password string
	}
	tests := []struct {
		name   string
		fields OnboardingFields
		args   args
		wants  wants
	}{
		{
			name: "denied",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   false,
			},
			wants: wants{
				errCode: platform.EConflict,
			},
		},
		{
			name: "missing password",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:   "admin",
					Org:    "org1",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: platform.EEmptyValue,
			},
		},
		{
			name: "missing username",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					Org:    "org1",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: platform.EEmptyValue,
			},
		},
		{
			name: "missing org",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:   "admin",
					Bucket: "bucket1",
				},
			},
			wants: wants{
				errCode: platform.EEmptyValue,
			},
		},
		{
			name: "missing bucket",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User: "admin",
					Org:  "org1",
				},
			},
			wants: wants{
				errCode: platform.EEmptyValue,
			},
		},
		{
			name: "regular",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
					t: t,
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:     "admin",
					Org:      "org1",
					Bucket:   "bucket1",
					Password: "pass1",
				},
			},
			wants: wants{
				password: "pass1",
				results: &platform.OnboardingResults{
					User: &platform.User{
						ID:   idFromString(t, oneID),
						Name: "admin",
					},
					Org: &platform.Organization{
						ID:   idFromString(t, twoID),
						Name: "org1",
					},
					Bucket: &platform.Bucket{
						ID:             idFromString(t, threeID),
						Name:           "bucket1",
						Organization:   "org1",
						OrganizationID: idFromString(t, twoID),
					},
					Auth: &platform.Authorization{
						ID:     idFromString(t, fourID),
						Token:  oneToken,
						Status: platform.Active,
						User:   "admin",
						UserID: idFromString(t, oneID),
						Permissions: []platform.Permission{
							platform.CreateUserPermission,
							platform.DeleteUserPermission,
							platform.Permission{
								Resource: platform.OrganizationResource,
								Action:   platform.WriteAction,
							},
							platform.WriteBucketPermission(idFromString(t, threeID)),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			results, err := s.Generate(ctx, tt.args.request)
			if (err != nil) != (tt.wants.errCode != "") {
				t.Fatalf("expected error code '%s' got '%v'", tt.wants.errCode, err)
			}
			if err != nil && tt.wants.errCode != "" {
				if code := platform.ErrorCode(err); code != tt.wants.errCode {
					t.Fatalf("expected error code to match '%s' got '%v'", tt.wants.errCode, code)
				}
			}
			if diff := cmp.Diff(results, tt.wants.results); diff != "" {
				t.Errorf("onboarding results are different -got/+want\ndiff %s", diff)
			}
			if results != nil {
				if err = s.ComparePassword(ctx, results.User.Name, tt.wants.password); err != nil {
					t.Errorf("onboarding set password is wrong")
				}
			}
		})
	}

}

const (
	oneID    = "020f755c3c082000"
	twoID    = "020f755c3c082001"
	threeID  = "020f755c3c082002"
	fourID   = "020f755c3c082003"
	oneToken = "020f755c3c082008"
)

type loopIDGenerator struct {
	s []string
	p int
	t *testing.T
}

func (g *loopIDGenerator) ID() platform.ID {
	if g.p == len(g.s) {
		g.p = 0
	}
	id := idFromString(g.t, g.s[g.p])
	g.p++
	return id
}
