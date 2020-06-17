package testing

import (
	"context"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
)

// NotificationRuleFields includes prepopulated data for mapping tests.
type NotificationRuleFields struct {
	IDGenerator          influxdb.IDGenerator
	TimeGenerator        influxdb.TimeGenerator
	NotificationRules    []influxdb.NotificationRule
	Orgs                 []*influxdb.Organization
	UserResourceMappings []*influxdb.UserResourceMapping
	Tasks                []influxdb.TaskCreate
	Endpoints            []influxdb.NotificationEndpoint
}

var notificationRuleCmpOptions = cmp.Options{
	cmpopts.IgnoreFields(rule.Base{}, "TaskID"),
	cmp.Transformer("Sort", func(in []influxdb.NotificationRule) []influxdb.NotificationRule {
		out := append([]influxdb.NotificationRule(nil), in...)
		sort.Slice(out, func(i, j int) bool {
			return out[i].GetID() > out[j].GetID()
		})
		return out
	}),
}

// NotificationRuleStore tests all the service functions.
func NotificationRuleStore(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
			t *testing.T)
	}{
		{
			name: "CreateNotificationRule",
			fn:   CreateNotificationRule,
		},
		{
			name: "FindNotificationRuleByID",
			fn:   FindNotificationRuleByID,
		},
		{
			name: "FindNotificationRules",
			fn:   FindNotificationRules,
		},
		{
			name: "UpdateNotificationRule",
			fn:   UpdateNotificationRule,
		},
		{
			name: "PatchNotificationRule",
			fn:   PatchNotificationRule,
		},
		{
			name: "DeleteNotificationRule",
			fn:   DeleteNotificationRule,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(init, t)
		})
	}
}

// CreateNotificationRule testing.
func CreateNotificationRule(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {
	type args struct {
		notificationRule influxdb.NotificationRule
		userID           influxdb.ID
	}
	type wants struct {
		err                 error
		notificationRules   []influxdb.NotificationRule
		userResourceMapping []*influxdb.UserResourceMapping
	}

	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "basic create notification rule",
			fields: NotificationRuleFields{
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				TimeGenerator: fakeGenerator,
				Orgs: []*influxdb.Organization{
					{
						Name: "org",
						ID:   MustIDBase16(fourID),
					},
				},
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: strPtr("abc123"),
						},
						Base: endpoint.Base{
							OrgID:  MustIDBase16Ptr(fourID),
							Name:   "foo",
							Status: influxdb.Active,
						},
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
							},
							TagRules: []notification.TagRule{
								{
									Tag: influxdb.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: influxdb.NotEqual,
								},
								{
									Tag: influxdb.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: influxdb.RegexEqual,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				notificationRule: &rule.Slack{
					Base: rule.Base{
						OwnerID:     MustIDBase16(sixID),
						Name:        "name2",
						OrgID:       MustIDBase16(fourID),
						EndpointID:  MustIDBase16(twoID),
						RunbookLink: "runbooklink1",
						SleepUntil:  &time3,
						Every:       mustDuration("1h"),
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Critical,
							},
						},
						TagRules: []notification.TagRule{
							{
								Tag: influxdb.Tag{
									Key:   "k1",
									Value: "v1",
								},
								Operator: influxdb.NotEqual,
							},
							{
								Tag: influxdb.Tag{
									Key:   "k2",
									Value: "v2",
								},
								Operator: influxdb.RegexEqual,
							},
						},
					},
					MessageTemplate: "msg1",
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
							},
							TagRules: []notification.TagRule{
								{
									Tag: influxdb.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: influxdb.NotEqual,
								},
								{
									Tag: influxdb.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: influxdb.RegexEqual,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
							},
							TagRules: []notification.TagRule{
								{
									Tag: influxdb.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: influxdb.NotEqual,
								},
								{
									Tag: influxdb.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: influxdb.RegexEqual,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: fakeDate,
								UpdatedAt: fakeDate,
							},
						},
						MessageTemplate: "msg1",
					},
				},
				userResourceMapping: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
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
			nrc := influxdb.NotificationRuleCreate{
				NotificationRule: tt.args.notificationRule,
				Status:           influxdb.Active,
			}
			err := s.CreateNotificationRule(ctx, nrc, tt.args.userID)
			if (err != nil) != (tt.wants.err != nil) {
				t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
			}
			if tt.wants.err == nil && !tt.args.notificationRule.GetID().Valid() {
				t.Fatalf("notification rule ID not set from CreateNotificationRule")
			}

			if err != nil && tt.wants.err != nil {
				if influxdb.ErrorCode(err) != influxdb.ErrorCode(tt.wants.err) {
					t.Fatalf("expected error messages to match '%v' got '%v'", influxdb.ErrorCode(tt.wants.err), influxdb.ErrorCode(err))
				}
			}

			urmFilter := influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationRuleResourceType,
			}

			filter := influxdb.NotificationRuleFilter{
				UserResourceMappingFilter: urmFilter,
			}
			nrs, _, err := s.FindNotificationRules(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve notification rules: %v", err)
			}
			if diff := cmp.Diff(nrs, tt.wants.notificationRules, notificationRuleCmpOptions...); diff != "" {
				t.Errorf("notificationRules are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationRuleByID testing.
func FindNotificationRuleByID(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err              error
		notificationRule influxdb.NotificationRule
	}

	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id: influxdb.ID(0),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "provided notification rule ID has invalid format",
				},
			},
		},
		{
			name: "not found",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification rule not found",
				},
			},
		},
		{
			name: "basic find telegraf config by id",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				notificationRule: &rule.Slack{
					Base: rule.Base{
						ID:          MustIDBase16(twoID),
						Name:        "name2",
						OwnerID:     MustIDBase16(sixID),
						OrgID:       MustIDBase16(fourID),
						EndpointID:  MustIDBase16(twoID),
						RunbookLink: "runbooklink2",
						SleepUntil:  &time3,
						Every:       mustDuration("1h"),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: timeGen2.Now(),
						},
					},
					MessageTemplate: "msg",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			nr, err := s.FindNotificationRuleByID(ctx, tt.args.id)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(nr, tt.wants.notificationRule, notificationRuleCmpOptions...); diff != "" {
				t.Errorf("notification rule is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationRules testing
func FindNotificationRules(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {
	type args struct {
		filter influxdb.NotificationRuleFilter
		opts   influxdb.FindOptions
	}

	type wants struct {
		notificationRules []influxdb.NotificationRule
		err               error
	}
	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "find nothing (empty set)",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{},
				NotificationRules:    []influxdb.NotificationRule{},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{},
			},
		},
		{
			name: "find all notification rules",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "find owners only",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserType:     influxdb.Owner,
					},
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
				},
			},
		},
		{
			name: "filter by organization id only",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "filter by organization name only",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					Organization: strPtr("org4"),
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
				},
			},
		},
		// {
		// 	name: "filter by tags",
		// 	fields: NotificationRuleFields{
		// 		Orgs: []*influxdb.Organization{
		// 			{
		// 				ID:   MustIDBase16(oneID),
		// 				Name: "org1",
		// 			},
		// 			{
		// 				ID:   MustIDBase16(fourID),
		// 				Name: "org4",
		// 			},
		// 		},
		// 		UserResourceMappings: []*influxdb.UserResourceMapping{
		// 			{
		// 				ResourceID:   MustIDBase16(oneID),
		// 				ResourceType: influxdb.NotificationRuleResourceType,
		// 				UserID:       MustIDBase16(sixID),
		// 				UserType:     influxdb.Owner,
		// 			},
		// 			{
		// 				ResourceID:   MustIDBase16(twoID),
		// 				ResourceType: influxdb.NotificationRuleResourceType,
		// 				UserID:       MustIDBase16(sixID),
		// 				UserType:     influxdb.Member,
		// 			},
		// 			{
		// 				ResourceID:   MustIDBase16(fourID),
		// 				ResourceType: influxdb.NotificationRuleResourceType,
		// 				UserID:       MustIDBase16(sixID),
		// 				UserType:     influxdb.Owner,
		// 			},
		// 		},
		// 		NotificationRules: []influxdb.NotificationRule{
		// 			&rule.Slack{
		// 				Base: rule.Base{
		// 					ID:         MustIDBase16(oneID),
		// 					OrgID:      MustIDBase16(fourID),
		// 					OwnerID:    MustIDBase16(sixID),
		// 					EndpointID: 1,
		// 					Status:     influxdb.Active,
		// 					Name:       "nr1",
		// 					TagRules: []influxdb.TagRule{
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "environment",
		// 								Value: "production",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 					},
		// 				},
		// 				Channel:         "ch1",
		// 				MessageTemplate: "msg1",
		// 			},
		// 			&rule.Slack{
		// 				Base: rule.Base{
		// 					ID:         MustIDBase16(twoID),
		// 					OrgID:      MustIDBase16(fourID),
		// 					OwnerID:    MustIDBase16(sixID),
		// 					EndpointID: 1,
		// 					Status:     influxdb.Active,
		// 					Name:       "nr2",
		// 					TagRules: []influxdb.TagRule{
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "environment",
		// 								Value: "production",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "location",
		// 								Value: "paris",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 					},
		// 				},
		// 				MessageTemplate: "body2",
		// 			},
		// 			&rule.Slack{
		// 				Base: rule.Base{
		// 					ID:         MustIDBase16(fourID),
		// 					OrgID:      MustIDBase16(oneID),
		// 					OwnerID:    MustIDBase16(sixID),
		// 					EndpointID: 1,
		// 					Status:     influxdb.Active,
		// 					Name:       "nr3",
		// 					TagRules: []influxdb.TagRule{
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "environment",
		// 								Value: "production",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "location",
		// 								Value: "paris",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 					},
		// 				},
		// 				MessageTemplate: "msg",
		// 			},
		// 		},
		// 	},
		// 	args: args{
		// 		filter: influxdb.NotificationRuleFilter{
		// 			Organization: strPtr("org4"),
		// 			Tags: []influxdb.Tag{
		// 				{
		// 					Key:   "environment",
		// 					Value: "production",
		// 				},
		// 				{
		// 					Key:   "location",
		// 					Value: "paris",
		// 				},
		// 			},
		// 		},
		// 	},
		// 	wants: wants{
		// 		notificationRules: []influxdb.NotificationRule{
		// 			&rule.Slack{
		// 				Base: rule.Base{
		// 					ID:         MustIDBase16(fourID),
		// 					OrgID:      MustIDBase16(oneID),
		// 					OwnerID:    MustIDBase16(sixID),
		// 					EndpointID: 1,
		// 					Status:     influxdb.Active,
		// 					Name:       "nr3",
		// 					TagRules: []influxdb.TagRule{
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "environment",
		// 								Value: "production",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 						{
		// 							Tag: influxdb.Tag{
		// 								Key:   "location",
		// 								Value: "paris",
		// 							},
		// 							Operator: notification.Equal,
		// 						},
		// 					},
		// 				},
		// 				MessageTemplate: "msg",
		// 			},
		// 		},
		// 	},
		// },
		{
			name: "find owners and restrict by organization",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(sixID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserType:     influxdb.Owner,
					},
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "look for organization not bound to any notification rule",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{},
			},
		},
		{
			name: "find options limit",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
				opts: influxdb.FindOptions{
					Limit: 2,
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
				},
			},
		},
		{
			name: "find options offset",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					OrgID: idPtr(MustIDBase16(oneID)),
				},
				opts: influxdb.FindOptions{
					Offset: 1,
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(oneID),
							EndpointID: 1,
							OwnerID:    MustIDBase16(sixID),
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "find nothing",
			fields: NotificationRuleFields{
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
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
					},
					{
						ResourceID:   MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(oneID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr2",
						},
						MessageTemplate: "body2",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(fourID),
							OrgID:      MustIDBase16(fourID),
							OwnerID:    MustIDBase16(sixID),
							EndpointID: 1,
							Name:       "nr3",
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{
					UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
						UserID:       MustIDBase16(fourID),
						ResourceType: influxdb.NotificationRuleResourceType,
					},
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

			nrs, n, err := s.FindNotificationRules(ctx, tt.args.filter, tt.args.opts)
			ErrorsEqual(t, err, tt.wants.err)
			if n != len(tt.wants.notificationRules) {
				t.Fatalf("notification rules length is different got %d, want %d", n, len(tt.wants.notificationRules))
			}

			if diff := cmp.Diff(nrs, tt.wants.notificationRules, notificationRuleCmpOptions...); diff != "" {
				t.Errorf("notification rules are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateNotificationRule testing.
func UpdateNotificationRule(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {
	type args struct {
		userID           influxdb.ID
		id               influxdb.ID
		notificationRule influxdb.NotificationRule
	}

	type wants struct {
		notificationRule influxdb.NotificationRule
		err              error
	}
	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "can't find the id",
			fields: NotificationRuleFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(fourID),
				notificationRule: &rule.Slack{
					Base: rule.Base{
						ID:          MustIDBase16(twoID),
						Name:        "name2",
						OwnerID:     MustIDBase16(sixID),
						OrgID:       MustIDBase16(fourID),
						EndpointID:  MustIDBase16(twoID),
						RunbookLink: "runbooklink3",
						SleepUntil:  &time3,
						Every:       mustDuration("2h"),
					},
					MessageTemplate: "msg2",
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification rule not found",
				},
			},
		},
		{
			name: "regular update",
			fields: NotificationRuleFields{
				TimeGenerator: fakeGenerator,
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				Tasks: []influxdb.TaskCreate{
					{
						OwnerID:        MustIDBase16(sixID),
						OrganizationID: MustIDBase16(fourID),
						Flux: `from(bucket: "foo") |> range(start: -1m)
						option task = {name: "bar", every: 1m}
						`,
					},
				},
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: strPtr("abc123"),
						},
						Base: endpoint.Base{
							OrgID:  MustIDBase16Ptr(fourID),
							Name:   "foo",
							Status: influxdb.Active,
						},
					},
				},
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(fourID),
						Name: "foo",
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:      MustIDBase16(oneID),
							Name:    "name1",
							OwnerID: MustIDBase16(sixID),
							OrgID:   MustIDBase16(fourID),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Info,
								},
							},
							EndpointID:  MustIDBase16(twoID),
							TaskID:      MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:      MustIDBase16(twoID),
							Name:    "name2",
							OwnerID: MustIDBase16(sixID),
							OrgID:   MustIDBase16(fourID),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Info,
								},
							},
							TaskID:      MustIDBase16(twoID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				notificationRule: &rule.Slack{
					Base: rule.Base{
						OwnerID:    MustIDBase16(sixID),
						Name:       "name3",
						OrgID:      MustIDBase16(fourID),
						EndpointID: MustIDBase16(twoID),
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Info,
							},
						},
						RunbookLink: "runbooklink3",
						SleepUntil:  &time3,
						Every:       mustDuration("2h"),
					},
					MessageTemplate: "msg2",
				},
			},
			wants: wants{
				notificationRule: &rule.Slack{
					Base: rule.Base{
						ID:      MustIDBase16(twoID),
						Name:    "name3",
						OwnerID: MustIDBase16(sixID),
						OrgID:   MustIDBase16(fourID),
						TaskID:  MustIDBase16(twoID),
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Info,
							},
						},
						EndpointID:  MustIDBase16(twoID),
						RunbookLink: "runbooklink3",
						SleepUntil:  &time3,
						Every:       mustDuration("2h"),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					MessageTemplate: "msg2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			nrc := influxdb.NotificationRuleCreate{
				NotificationRule: tt.args.notificationRule,
				Status:           influxdb.Active,
			}

			tc, err := s.UpdateNotificationRule(ctx, tt.args.id,
				nrc, tt.args.userID)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(tc, tt.wants.notificationRule, notificationRuleCmpOptions...); tt.wants.err == nil && diff != "" {
				t.Errorf("notificationRules are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// PatchNotificationRule testing.
func PatchNotificationRule(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {

	name3 := "name2"
	status3 := influxdb.Inactive

	type args struct {
		//userID           influxdb.ID
		id  influxdb.ID
		upd influxdb.NotificationRuleUpdate
	}

	type wants struct {
		notificationRule influxdb.NotificationRule
		err              error
	}
	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "can't find the id",
			fields: NotificationRuleFields{
				TimeGenerator: fakeGenerator,
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id: MustIDBase16(fourID),
				upd: influxdb.NotificationRuleUpdate{
					Name:   &name3,
					Status: &status3,
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification rule not found",
				},
			},
		},
		{
			name: "patch without status",
			fields: NotificationRuleFields{
				TimeGenerator: fakeGenerator,
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				Tasks: []influxdb.TaskCreate{
					{
						OwnerID:        MustIDBase16(sixID),
						OrganizationID: MustIDBase16(fourID),
						Flux: `from(bucket: "foo") |> range(start: -1m)
						option task = {name: "bar", every: 1m}
						`,
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: strPtr("abc123"),
						},
						Base: endpoint.Base{
							OrgID:  MustIDBase16Ptr(fourID),
							Name:   "foo",
							Status: influxdb.Active,
						},
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
								{
									CurrentLevel: notification.Info,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							Name:       "name2",
							OwnerID:    MustIDBase16(sixID),
							EndpointID: MustIDBase16(twoID),
							TaskID:     MustIDBase16(twoID),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
								{
									CurrentLevel: notification.Info,
								},
							},
							OrgID:       MustIDBase16(fourID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(fourID),
						Name: "foo",
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
				upd: influxdb.NotificationRuleUpdate{
					Name: &name3,
				},
			},
			wants: wants{
				notificationRule: &rule.Slack{
					Base: rule.Base{
						ID:         MustIDBase16(twoID),
						Name:       name3,
						OwnerID:    MustIDBase16(sixID),
						OrgID:      MustIDBase16(fourID),
						EndpointID: MustIDBase16(twoID),
						TaskID:     MustIDBase16(twoID),
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Critical,
							},
							{
								CurrentLevel: notification.Info,
							},
						},
						RunbookLink: "runbooklink2",
						SleepUntil:  &time3,
						Every:       mustDuration("1h"),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					MessageTemplate: "msg",
				},
			},
		},
		{
			name: "regular patch",
			fields: NotificationRuleFields{
				TimeGenerator: fakeGenerator,
				IDGenerator:   mock.NewIDGenerator(twoID, t),
				Tasks: []influxdb.TaskCreate{
					{
						OwnerID:        MustIDBase16(sixID),
						OrganizationID: MustIDBase16(fourID),
						Flux: `from(bucket: "foo") |> range(start: -1m)
						option task = {name: "bar", every: 1m}
						`,
					},
				},
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: strPtr("abc123"),
						},
						Base: endpoint.Base{
							OrgID:  MustIDBase16Ptr(fourID),
							Name:   "foo",
							Status: influxdb.Active,
						},
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
								{
									CurrentLevel: notification.Info,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:         MustIDBase16(twoID),
							Name:       "name2",
							OwnerID:    MustIDBase16(sixID),
							EndpointID: MustIDBase16(twoID),
							TaskID:     MustIDBase16(twoID),
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
								{
									CurrentLevel: notification.Info,
								},
							},
							OrgID:       MustIDBase16(fourID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(fourID),
						Name: "foo",
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
				upd: influxdb.NotificationRuleUpdate{
					Name:   &name3,
					Status: &status3,
				},
			},
			wants: wants{
				notificationRule: &rule.Slack{
					Base: rule.Base{
						ID:         MustIDBase16(twoID),
						Name:       name3,
						OwnerID:    MustIDBase16(sixID),
						OrgID:      MustIDBase16(fourID),
						EndpointID: MustIDBase16(twoID),
						TaskID:     MustIDBase16(twoID),
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Critical,
							},
							{
								CurrentLevel: notification.Info,
							},
						},
						RunbookLink: "runbooklink2",
						SleepUntil:  &time3,
						Every:       mustDuration("1h"),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					MessageTemplate: "msg",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			tc, err := s.PatchNotificationRule(ctx, tt.args.id, tt.args.upd)
			ErrorsEqual(t, err, tt.wants.err)
			if diff := cmp.Diff(tc, tt.wants.notificationRule, notificationRuleCmpOptions...); tt.wants.err == nil && diff != "" {
				t.Errorf("notificationRules are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteNotificationRule testing.
func DeleteNotificationRule(
	init func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, func()),
	t *testing.T,
) {
	type args struct {
		id     influxdb.ID
		userID influxdb.ID
	}

	type wants struct {
		notificationRules    []influxdb.NotificationRule
		userResourceMappings []*influxdb.UserResourceMapping
		err                  error
	}
	tests := []struct {
		name   string
		fields NotificationRuleFields
		args   args
		wants  wants
	}{
		{
			name: "bad id",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id:     influxdb.ID(0),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "provided notification rule ID has invalid format",
				},
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							EndpointID:  MustIDBase16(twoID),
							OrgID:       MustIDBase16(fourID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "none existing config",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				IDGenerator: mock.NewIDGenerator(twoID, t),
				Tasks: []influxdb.TaskCreate{
					{
						OwnerID:        MustIDBase16(sixID),
						OrganizationID: MustIDBase16(fourID),
						Flux: `from(bucket: "foo") |> range(start: -1m)
						option task = {name: "bar", every: 1m}
						`,
					},
				},
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(fourID),
						Name: "foo",
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							TaskID:      MustIDBase16(twoID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id:     MustIDBase16(fourID),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification rule not found",
				},
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink2",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
		},
		{
			name: "regular delete",
			fields: NotificationRuleFields{
				UserResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
					{
						ResourceID:   MustIDBase16(twoID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Member,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				Tasks: []influxdb.TaskCreate{
					{
						OwnerID:        MustIDBase16(sixID),
						OrganizationID: MustIDBase16(fourID),
						Flux: `from(bucket: "foo") |> range(start: -1m)
						option task = {name: "bar", every: 1m}
						`,
					},
				},
				IDGenerator: mock.NewIDGenerator(twoID, t),
				Orgs: []*influxdb.Organization{
					{
						ID:   MustIDBase16(fourID),
						Name: "foo",
					},
				},
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							EndpointID:  MustIDBase16(twoID),
							TaskID:      MustIDBase16(twoID),
							OrgID:       MustIDBase16(fourID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(twoID),
							Name:        "name2",
							OwnerID:     MustIDBase16(sixID),
							TaskID:      MustIDBase16(twoID),
							OrgID:       MustIDBase16(fourID),
							RunbookLink: "runbooklink2",
							EndpointID:  MustIDBase16(twoID),
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemplate: "msg",
					},
				},
			},
			args: args{
				id:     MustIDBase16(twoID),
				userID: MustIDBase16(sixID),
			},
			wants: wants{
				userResourceMappings: []*influxdb.UserResourceMapping{
					{
						ResourceID:   MustIDBase16(oneID),
						UserID:       MustIDBase16(sixID),
						UserType:     influxdb.Owner,
						ResourceType: influxdb.NotificationRuleResourceType,
					},
				},
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:          MustIDBase16(oneID),
							Name:        "name1",
							OwnerID:     MustIDBase16(sixID),
							OrgID:       MustIDBase16(fourID),
							TaskID:      MustIDBase16(twoID),
							EndpointID:  MustIDBase16(twoID),
							RunbookLink: "runbooklink1",
							SleepUntil:  &time3,
							Every:       mustDuration("1h"),
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
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
			err := s.DeleteNotificationRule(ctx, tt.args.id)
			ErrorsEqual(t, err, tt.wants.err)

			filter := influxdb.NotificationRuleFilter{
				UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
					UserID:       tt.args.userID,
					ResourceType: influxdb.NotificationRuleResourceType,
				},
			}
			nrs, n, err := s.FindNotificationRules(ctx, filter)
			if err != nil && tt.wants.err == nil {
				t.Fatalf("expected errors to be nil got '%v'", err)
			}

			if err != nil && tt.wants.err != nil {
				if want, got := tt.wants.err.Error(), err.Error(); want != got {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}
			}

			if n != len(tt.wants.notificationRules) {
				t.Fatalf("notification rules length is different got %d, want %d", n, len(tt.wants.notificationRules))
			}
			if diff := cmp.Diff(nrs, tt.wants.notificationRules, notificationRuleCmpOptions...); diff != "" {
				t.Errorf("notification rules are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
