package testing

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/endpoint"
)

// NotificationEndpointFields includes prepopulated data for mapping tests.
type NotificationEndpointFields struct {
	IDGenerator           influxdb.IDGenerator
	TimeGenerator         influxdb.TimeGenerator
	NotificationEndpoints []influxdb.NotificationEndpoint
	Orgs                  []*influxdb.Organization
	UserResourceMappings  []*influxdb.UserResourceMapping
	Secrets               []Secret
}

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
var time3 = time.Date(2006, time.July, 15, 5, 23, 53, 10, time.UTC)

var notificationEndpointCmpOptions = cmp.Options{
	cmp.Transformer("Sort", func(in []influxdb.NotificationEndpoint) []influxdb.NotificationEndpoint {
		out := append([]influxdb.NotificationEndpoint(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].GetID() > out[j].GetID()
		})
		return out
	}),
}

// NotificationEndpointService tests all the service functions.
func NotificationEndpointService(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
			t *testing.T)
	}{
		{
			name: "CreateNotificationEndpoint",
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
			name: "UpdateNotificationEndpoint",
			fn:   UpdateNotificationEndpoint,
		},
		{
			name: "PatchNotificationEndpoint",
			fn:   PatchNotificationEndpoint,
		},
		{
			name: "DeleteNotificationEndpoint",
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
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token": "slack-secret-1",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							Name:   "name1",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL: "example-slack.com",
						Token: influxdb.SecretField{
							Key: oneID + "-token",
						},
					},
				},
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
					Base: endpoint.Base{
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Active,
					},
					URL: "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{
						Value: strPtr("pagerduty secret2"),
					},
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: fakeDate,
								UpdatedAt: fakeDate,
							},
						},
						URL:        "example-pagerduty.com",
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
		{
			name: "secret not found",
			fields: NotificationEndpointFields{
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				TimeGenerator: fakeGenerator,
				Orgs: []*influxdb.Organization{
					{ID: MustIDBase16(fourID), Name: "org1"},
				},
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token": "slack-secret-1",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
					Base: endpoint.Base{
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Active,
					},
					URL:        "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "Unable to locate secret key: " + twoID + "-routing-key",
				},
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
				userResourceMapping: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: influxdb.NotificationEndpointResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
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
			err := s.CreateNotificationEndpoint(ctx, tt.args.notificationEndpoint, tt.args.userID)
			ErrorsEqual(t, err, tt.wants.err)

			urmFilter := influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationEndpointResourceType,
			}

			filter := influxdb.NotificationEndpointFilter{}
			edps, _, err := s.FindNotificationEndpoints(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve notification endpoints: %v", err)
			}
			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := s.FindUserResourceMappings(ctx, urmFilter)
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMapping, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationEndpointByID testing.
func FindNotificationEndpointByID(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err                  error
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
					Msg:  "provided notification endpoint ID has invalid format",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:     MustIDBase16(twoID),
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Active,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: timeGen2.Now(),
						},
					},
					URL:        "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edp, err := s.FindNotificationEndpointByID(ctx, tt.args.id)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(edp, tt.wants.notificationEndpoint, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoint is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationEndpoints testing
func FindNotificationEndpoints(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.NotificationEndpointFilter
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "filter by organization id only",
			fields: NotificationEndpointFields{
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
					{
						OrganizationID: MustIDBase16(oneID),
						Env: map[string]string{
							fourID + "-routing-key": "pager-duty-secret-3",
						},
					},
				},
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
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.PagerDuty{
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp2",
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
					&endpoint.PagerDuty{
						Base: endpoint.Base{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL:        "example-pagerduty2.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.PagerDuty{
						Base: endpoint.Base{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL:        "example-pagerduty2.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "filter by organization name only",
			fields: NotificationEndpointFields{
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token": "slack-secret-1",
						},
					},
					{
						OrganizationID: MustIDBase16(oneID),
						Env: map[string]string{
							fourID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
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
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.WebHook{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					Org: strPtr("org4"),
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.WebHook{
						Base: endpoint.Base{
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
			name: "find by id",
			fields: NotificationEndpointFields{
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token": "slack-secret-1",
						},
					},
					{
						OrganizationID: MustIDBase16(oneID),
						Env: map[string]string{
							fourID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
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
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.WebHook{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					ID: idPtr(MustIDBase16(fourID)),
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.PagerDuty{
						Base: endpoint.Base{
							ID:     MustIDBase16(fourID),
							OrgID:  MustIDBase16(oneID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: fourID + "-routing-key"},
					},
				},
			},
		},
		{
			name: "look for organization not bound to any notification endpoint",
			fields: NotificationEndpointFields{
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":         "slack-secret-1",
							threeID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
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
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.WebHook{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(threeID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: threeID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				notificationEndpoints: []influxdb.NotificationEndpoint{},
			},
		},
		{
			name: "find nothing",
			fields: NotificationEndpointFields{
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":         "slack-secret-1",
							threeID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
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
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
							ID:     MustIDBase16(oneID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp1",
						},
						URL:   "example-slack.com",
						Token: influxdb.SecretField{Key: oneID + "-token"},
					},
					&endpoint.WebHook{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(threeID),
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							Name:   "edp3",
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: threeID + "-routing-key"},
					},
				},
			},
			args: args{
				filter: influxdb.NotificationEndpointFilter{
					ID: idPtr(MustIDBase16(fiveID)),
				},
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edps, n, err := s.FindNotificationEndpoints(ctx, tt.args.filter)
			ErrorsEqual(t, err, tt.wants.err)
			if n != len(tt.wants.notificationEndpoints) {
				t.Fatalf("notification endpoints length is different got %d, want %d", n, len(tt.wants.notificationEndpoints))
			}

			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoints are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateNotificationEndpoint testing.
func UpdateNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
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
		secret               *Secret
		err                  error
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(fourID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:     MustIDBase16(twoID),
						Name:   "name2",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					URL:        "example-pagerduty.com",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					URL:        "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
			wants: wants{
				secret: &Secret{
					OrganizationID: MustIDBase16(fourID),
					Env: map[string]string{
						twoID + "-routing-key": "pager-duty-secret-2",
					},
				},
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					URL:        "example-pagerduty2.com",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
						RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				orgID:  MustIDBase16(fourID),
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
					},
					URL: "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{
						Key:   twoID + "-routing-key",
						Value: strPtr("pager-duty-value2"),
					},
				},
			},
			wants: wants{
				secret: &Secret{
					OrganizationID: MustIDBase16(fourID),
					Env: map[string]string{
						twoID + "-routing-key": "pager-duty-value2",
					},
				},
				notificationEndpoint: &endpoint.PagerDuty{
					Base: endpoint.Base{
						ID:     MustIDBase16(twoID),
						Name:   "name3",
						OrgID:  MustIDBase16(fourID),
						Status: influxdb.Inactive,
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					URL: "example-pagerduty2.com",
					RoutingKey: influxdb.SecretField{
						Key:   twoID + "-routing-key",
						Value: strPtr("pager-duty-value2"),
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

			edp, err := s.UpdateNotificationEndpoint(ctx, tt.args.id,
				tt.args.notificationEndpoint, tt.args.userID)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(edp, tt.wants.notificationEndpoint, notificationEndpointCmpOptions...); tt.wants.err == nil && diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}
			if err != nil {
				return
			}
			scrt := &Secret{
				OrganizationID: tt.args.orgID,
				Env:            make(map[string]string),
			}
			for _, fld := range edp.SecretFields() {
				scrtValue, err := s.LoadSecret(ctx, tt.args.orgID, fld.Key)
				if err != nil {
					t.Fatalf("failed to retrieve keys")
				}
				scrt.Env[fld.Key] = scrtValue
			}
			if diff := cmp.Diff(scrt, tt.wants.secret, secretCmpOptions); diff != "" {
				t.Errorf("secret is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// PatchNotificationEndpoint testing.
func PatchNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
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
		err                  error
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							Status: influxdb.Active,
							OrgID:  MustIDBase16(fourID),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
					Base: endpoint.Base{
						ID:     MustIDBase16(twoID),
						Name:   name3,
						Status: status3,
						OrgID:  MustIDBase16(fourID),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					URL:        "example-pagerduty.com",
					RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			edp, err := s.PatchNotificationEndpoint(ctx, tt.args.id, tt.args.upd)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(edp, tt.wants.notificationEndpoint, notificationEndpointCmpOptions...); tt.wants.err == nil && diff != "" {
				t.Errorf("notificationEndpoints are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteNotificationEndpoint testing.
func DeleteNotificationEndpoint(
	init func(NotificationEndpointFields, *testing.T) (influxdb.NotificationEndpointService, func()),
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
		secrets               []string
		err                   error
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
					Msg:  "provided notification endpoint ID has invalid format",
				},
				secrets: []string{
					oneID + "-token",
					twoID + "-routing-key",
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
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL: "example-pagerduty.com", RoutingKey: influxdb.SecretField{Key: twoID + "-routing-key"},
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
				secrets: []string{
					oneID + "-token",
					twoID + "-routing-key",
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
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
				Secrets: []Secret{
					{
						OrganizationID: MustIDBase16(fourID),
						Env: map[string]string{
							oneID + "-token":       "slack-secret-1",
							twoID + "-routing-key": "pager-duty-secret-2",
						},
					},
				},
				NotificationEndpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						Base: endpoint.Base{
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
						Base: endpoint.Base{
							ID:     MustIDBase16(twoID),
							Name:   "name2",
							OrgID:  MustIDBase16(fourID),
							Status: influxdb.Active,
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						URL:        "example-pagerduty.com",
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
				secrets: []string{
					oneID + "-token",
				},
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
						Base: endpoint.Base{
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
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteNotificationEndpoint(ctx, tt.args.id)
			ErrorsEqual(t, err, tt.wants.err)

			filter := influxdb.NotificationEndpointFilter{}
			edps, n, err := s.FindNotificationEndpoints(ctx, filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if n != len(tt.wants.notificationEndpoints) {
				t.Fatalf("notification endpoints length is different got %d, want %d", n, len(tt.wants.notificationEndpoints))
			}
			if diff := cmp.Diff(edps, tt.wants.notificationEndpoints, notificationEndpointCmpOptions...); diff != "" {
				t.Errorf("notification endpoints are different -got/+want\ndiff %s", diff)
			}

			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationEndpointResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMappings, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
			scrtKeys, err := s.GetSecretKeys(ctx, tt.args.orgID)
			if err != nil {
				t.Fatalf("failed to retrieve secret keys: %v", err)
			}
			if diff := cmp.Diff(scrtKeys, tt.wants.secrets, secretCmpOptions...); diff != "" {
				t.Errorf("secret keys are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
