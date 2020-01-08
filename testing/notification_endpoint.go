package testing

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/endpoints"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type (
	// NotificationEndpointFields includes prepopulated data for mapping tests.
	NotificationEndpointFields struct {
		IDGenerator           influxdb.IDGenerator
		TimeGenerator         influxdb.TimeGenerator
		NotificationEndpoints []influxdb.NotificationEndpoint
		Orgs                  []*influxdb.Organization
		UserResourceMappings  []*influxdb.UserResourceMapping
	}

	NotificationEndpointDeps struct {
		SecretSVC              influxdb.SecretService
		UserResourceMappingSVC influxdb.UserResourceMappingService
	}
)

var (
	timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
	timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
	time3    = time.Date(2006, time.July, 15, 5, 23, 53, 10, time.UTC)
)

var notificationEndpointCmpOptions = cmp.Options{
	cmp.Transformer("Sort", func(in []influxdb.NotificationEndpoint) []influxdb.NotificationEndpoint {
		out := append([]influxdb.NotificationEndpoint(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].Base().ID > out[j].Base().ID
		})
		return out
	}),
}

// NotificationEndpointService tests all the service functions.
func NotificationEndpointService(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
			t *testing.T)
	}{
		{
			name: "Create",
			fn:   CreateNotificationEndpoint,
		},
		{
			name: "FindNotificationEndpointByID",
			fn:   FindNotificationEndpointByID,
		},
		{
			name: "FindNotificationEndpoints",
			fn:   FindNotificationEndpoints,
		},
		{
			name: "Update",
			fn:   UpdateNotificationEndpoint,
		},
		{
			name: "PatchNotificationEndpoint",
			fn:   PatchNotificationEndpoint,
		},
		{
			name: "Delete",
			fn:   DeleteNotificationEndpoint,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateNotificationEndpoint testing.
func CreateNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {
	type args struct {
		notificationEndpoint influxdb.NotificationEndpoint
		userID               influxdb.ID
	}
	type wants struct {
		err                   error
		notificationEndpoints []influxdb.NotificationEndpoint
		userResourceMapping   []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "basic create notification endpoint",
			fields: NotificationEndpointFields{
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				TimeGenerator: fakeGenerator,
				Orgs: []*influxdb.Organization{
					{ID: MustIDBase16(fourID), Name: "org1"},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: influxdb.NotificationEndpointResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Active,
					},
					ClientURL: "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{
						Value: strPtr("pagerduty secret2"),
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: fakeDate,
								UpdatedAt: fakeDate,
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
				userResourceMapping: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: influxdb.NotificationEndpointResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationEndpointResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, deps, done := init(tt.fields, t)
			defer done()

			ctx := context.Background()
			err := s.Create(ctx, tt.args.userID, tt.args.notificationEndpoint)
			ErrorsEqual(t, err, tt.wants.err)

			urmFilter := influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationEndpointResourceType,
			}

			filter := influxdb.NotificationEndpointFilter{}
			edps, err := s.Find(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve notification endpoints: %v", err)
			}
			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := deps.UserResourceMappingSVC.FindUserResourceMappings(ctx, urmFilter)
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMapping, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}

			for _, edp := range tt.wants.notificationEndpoints {
				secrets, err := deps.SecretSVC.GetSecretKeys(ctx, edp.Base().OrgID)
				if err != nil {
					t.Errorf("failed to retrieve secrets for endpoint: %v", err)
				}
				for _, expected := range edp.SecretFields() {
					assert.Contains(t, secrets, expected.Key)
				}
			}
		})
	}
}

// FindNotificationEndpointByID testing.
func FindNotificationEndpointByID(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err                  *influxdb.Error
		notificationEndpoint influxdb.NotificationEndpoint
	}

	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: influxdb.ID(0),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "no key was provided for notification endpoint",
				},
			},
		},
		{
			name: "not found",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification endpoint not found",
				},
			},
		},
		{
			name: "basic find telegraf config by id",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Active,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: timeGen2.Now(),
						},
					},
					ClientURL:  "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edp, err := s.FindByID(ctx, tt.args.id)
			influxErrsEqual(t, tt.wants.err, err)
			if diff := cmp.Diff(edp, tt.wants.notificationEndpoint, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoint is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationEndpoints testing
func FindNotificationEndpoints(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.NotificationEndpointFilter
		opts   influxdb.FindOptions
	}

	type wants struct {
		notificationEndpoints []influxdb.NotificationEndpoint
		err                   error
	}
	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "find nothing (empty set)",
			fields: NotificationEndpointFields{
				NotificationEndpoints: []influxdb.NotificationEndpoint{},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{},
			},
		},
		{
			name: "find all notification endpoints",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "filter by organization id only",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty2.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						ResourceType: influxdb.NotificationEndpointResourceType,
						UserID:       MustIDBase16(sixID),
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty2.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "filter by organization name only",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					Org: strPtr("org4"),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
				},
			},
		},
		{
			name: "find options limit",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fiveID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp4",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					Org: strPtr("org4"),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				opts: influxdb.FindOptions{
					Limit: 2,
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
				},
			},
		},
		{
			name: "find options offset",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},

					{
						ResourceID:   MustIDBase16(fourID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					Org: strPtr("org4"),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				opts: influxdb.FindOptions{
					Offset: 1,
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "find options offset",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},

					{
						ResourceID:   MustIDBase16(fourID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					Org: strPtr("org4"),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				opts: influxdb.FindOptions{
					Limit:  1,
					Offset: 1,
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
				},
			},
		},
		{
			name: "look for organization not bound to any notification endpoint",
			fields: NotificationEndpointFields{
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(oneID),
						Name: "org1",
					},
					{
						ID:   MustIDBase16(fourID),
						Name: "org4",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(threeID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.HTTP{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-webhook.com",
						Method:     http.MethodGet,
						AuthMethod: "none",
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(threeID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						ClientURL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: threeID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edps, err := s.Find(ctx, tt.args.filter, tt.args.opts)
			ErrorsEqual(t, err, tt.wants.err)
			if len(edps) != len(tt.wants.notificationEndpoints) {
				t.Fatalf("notification endpoints length is different got %d, want %d", len(edps), len(tt.wants.notificationEndpoints))
			}

			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoints are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateNotificationEndpoint testing.
func UpdateNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {
	type args struct {
		userID               influxdb.ID
		orgID                influxdb.ID
		id                   influxdb.ID
		notificationEndpoint influxdb.NotificationEndpoint
	}

	type wants struct {
		notificationEndpoint influxdb.NotificationEndpoint
		err                  *influxdb.Error
	}
	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "can't find the id",
			fields: NotificationEndpointFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(fourID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					ClientURL:  "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: "pager-duty-routing-key-2"},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification endpoint not found",
				},
			},
		},
		{
			name: "regular update",
			fields: NotificationEndpointFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					ClientURL:  "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{Value: strPtr("secret value")},
				},
			},
			wants: wants{
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					ClientURL:  "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
		},
		{
			name: "update secret",
			fields: NotificationEndpointFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					ClientURL: "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{
						Value: strPtr("pager-duty-value2"),
					},
				},
			},
			wants: wants{
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					ClientURL: "example-pagerduty2.com",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, deps, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edp, err := s.Update(ctx, endpoints.UpdateEndpoint(tt.args.notificationEndpoint))
			if err != nil {
				iErr, ok := err.(*influxdb.Error)
				require.True(t, ok)
				assert.Equal(t, tt.wants.err.Code, iErr.Code)
				return
			}

			if tt.wants.notificationEndpoint != nil {
				secrets, err := deps.SecretSVC.GetSecretKeys(ctx, edp.Base().OrgID)
				if err != nil {
					t.Errorf("failed to retrieve secrets for endpoint: %v", err)
				}
				for _, actual := range edp.SecretFields() {
					assert.Contains(t, secrets, actual.Key)
				}

				actual, ok := edp.(*endpoint.PagerDuty)
				require.Truef(t, ok, "did not get a pager duty endpoint; got: %#v", edp)
				wanted := tt.wants.notificationEndpoint.(*endpoint.PagerDuty)

				wb, ab := wanted.Base(), actual.Base()
				require.NotZero(t, ab.CRUDLog)
				wb.CRUDLog, ab.CRUDLog = influxdb.CRUDLog{}, influxdb.CRUDLog{} // zero out times
				assert.Equal(t, wb, ab)
				assert.Equal(t, wanted.ClientURL, actual.ClientURL)
				assert.NotEqual(t, wanted.RoutingKey, actual.RoutingKey)
			}
		})
	}
}

// PatchNotificationEndpoint testing.
func PatchNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {

	name3 := "name2"
	status3 := influxdb.Inactive

	type args struct {
		//userID           influxdb.ID
		id  influxdb.ID
		upd influxdb.NotificationEndpointUpdate
	}

	type wants struct {
		notificationEndpoint influxdb.NotificationEndpoint
		err                  *influxdb.Error
	}
	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "can't find the id",
			fields: NotificationEndpointFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: MustIDBase16(fourID),
				upd: influxdb.NotificationEndpointUpdate{
					Name:   &name3,
					Status: &status3,
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification endpoint not found",
				},
			},
		},
		{
			name: "regular update",
			fields: NotificationEndpointFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							Status: influxdb.Active,
							OrgID:  MustIDBase16(fourID),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							Status: influxdb.Active,
							OrgID:  MustIDBase16(fourID),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
				upd: influxdb.NotificationEndpointUpdate{
					Name:   &name3,
					Status: &status3,
				},
			},
			wants: wants{
				notificationEndpoint: &endpoint.PagerDuty{
					EndpointBase: influxdb.EndpointBase{
						ID:     MustIDBase16(twoID),
						Name:   name3,
						Status: status3,
						OrgID:  MustIDBase16(fourID),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					ClientURL:  "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edp, err := s.Update(ctx, endpoints.UpdateChangeSet(tt.args.id, tt.args.upd))
			if err != nil {
				if tt.wants.err == nil {
					require.NoError(t, err)
				}
				iErr, ok := err.(*influxdb.Error)
				require.True(t, ok, err)
				assert.Equal(t, tt.wants.err.Code, iErr.Code)
				return
			}
			if diff := cmp.Diff(edp, tt.wants.notificationEndpoint, notificationEndpointCmpOptions...); tt.wants.err == nil && diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteNotificationEndpoint testing.
func DeleteNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, NotificationEndpointDeps, func()),
	t *testing.T,
) {
	type args struct {
		id     influxdb.ID
		orgID  influxdb.ID
		userID influxdb.ID
	}

	type wants struct {
		notificationEndpoints []influxdb.NotificationEndpoint
		userResourceMappings  []*influxdb.UserResourceMapping
		orgID                 influxdb.ID
		err                   *influxdb.Error
	}
	tests := []struct {
		name   string
		fields NotificationEndpointFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id:     influxdb.ID(0),
				orgID:  MustIDBase16(fourID),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "no key was provided for notification endpoint",
				},
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "none existing endpoint",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id:     MustIDBase16(fourID),
				orgID:  MustIDBase16(fourID),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification endpoint not found",
				},
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "regular delete",
			fields: NotificationEndpointFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						ClientURL:  "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id:     MustIDBase16(twoID),
				orgID:  MustIDBase16(fourID),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				orgID: MustIDBase16(fourID),
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationEndpointResourceType,
					},
				},
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						EndpointBase: influxdb.EndpointBase{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, deps, done := init(tt.fields, t)
			defer done()

			ctx := context.Background()
			err := s.Delete(ctx, tt.args.id)
			influxErrsEqual(t, tt.wants.err, err)

			filter := influxdb.NotificationEndpointFilter{}
			edps, err := s.Find(ctx, filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if len(edps) != len(tt.wants.notificationEndpoints) {
				t.Fatalf("notification endpoints length is different got %d, want %d", len(edps), len(tt.wants.notificationEndpoints))
			}
			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoints are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := deps.UserResourceMappingSVC.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationEndpointResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			assert.Equal(t, tt.wants.userResourceMappings, urms)

			var deletedEndpoint influxdb.NotificationEndpoint
			for _, ne := range tt.fields.NotificationEndpoints {
				if ne.Base().ID == tt.args.id {
					deletedEndpoint = ne
					break
				}
			}
			if deletedEndpoint == nil {
				return
			}

			secrets, err := deps.SecretSVC.GetSecretKeys(ctx, deletedEndpoint.Base().OrgID)
			require.NoError(t, err)
			for _, deleted := range deletedEndpoint.SecretFields() {
				assert.NotContains(t, secrets, deleted.Key)
			}
		})
	}
}

func influxErrsEqual(t *testing.T, expected *influxdb.Error, actual error) {
	t.Helper()

	if expected != nil {
		require.Error(t, actual)
	}

	if actual == nil {
		return
	}

	if expected == nil {
		require.NoError(t, actual)
	}
	iErr, ok := actual.(*influxdb.Error)
	require.True(t, ok)
	assert.Equal(t, expected.Code, iErr.Code)
	assert.Truef(t, strings.HasPrefix(iErr.Error(), expected.Error()), "expected: %s got err: %s", expected.Error(), actual.Error())
}
