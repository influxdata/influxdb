package testing

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

// OnboardingFields will include the IDGenerator, TokenGenerator
// and IsOnboarding
type OnboardingFields struct {
	IDGenerator    platform.IDGenerator
	TokenGenerator platform.TokenGenerator
	IsOnboarding   bool
}

// Generate testing
func Generate(
	init func(OnboardingFields, *testing.T) (platform.OnboardingService, func()),
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
			name: "valid onboarding json should create a user, org, bucket, and authorization",
			fields: OnboardingFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				IsOnboarding:   true,
			},
			args: args{
				request: &platform.OnboardingRequest{
					User:            "admin",
					Org:             "org1",
					Bucket:          "bucket1",
					Password:        "pass1",
					RetentionPeriod: 24 * 7, // 1 week
				},
			},
			wants: wants{
				password: "pass1",
				results: &platform.OnboardingResults{
					User: &platform.User{
						ID:   MustIDBase16(oneID),
						Name: "admin",
					},
					Org: &platform.Organization{
						ID:   MustIDBase16(twoID),
						Name: "org1",
					},
					Bucket: &platform.Bucket{
						ID:              MustIDBase16(threeID),
						Name:            "bucket1",
						Organization:    "org1",
						OrganizationID:  MustIDBase16(twoID),
						RetentionPeriod: time.Hour * 24 * 7,
					},
					Auth: &platform.Authorization{
						ID:          MustIDBase16(fourID),
						Token:       oneToken,
						Status:      platform.Active,
						UserID:      MustIDBase16(oneID),
						Description: "admin's Token",
						OrgID:       MustIDBase16(twoID),
						Permissions: platform.OperPermissions(),
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
}

func (g *loopIDGenerator) ID() platform.ID {
	if g.p == len(g.s) {
		g.p = 0
	}
	id := MustIDBase16(g.s[g.p])
	g.p++
	return id
}
