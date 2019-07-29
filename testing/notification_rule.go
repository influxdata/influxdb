package testing

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification"
	"github.com/influxdata/influxdb/notification/rule"
)

// NotificationRuleFields includes prepopulated data for mapping tests.
type NotificationRuleFields struct {
	IDGenerator          influxdb.IDGenerator
	TimeGenerator        influxdb.TimeGenerator
	NotificationRules    []influxdb.NotificationRule
	Orgs                 []*influxdb.Organization
	UserResourceMappings []*influxdb.UserResourceMapping
}

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
var time3 = time.Date(2006, time.July, 15, 5, 23, 53, 10, time.UTC)

var notificationRuleCmpOptions = cmp.Options{
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
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:              MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Name:            "name1",
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							TagRules: []notification.TagRule{
								{
									Tag: notification.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: notification.NotEqual,
								},
								{
									Tag: notification.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: notification.RegexEqual,
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
				notificationRule: &rule.SMTP{
					Base: rule.Base{
						AuthorizationID: MustIDBase16(threeID),
						Name:            "name2",
						OrgID:           MustIDBase16(fourID),
						EndpointID:      IDPtr(MustIDBase16(fiveID)),
						Status:          influxdb.Active,
						RunbookLink:     "runbooklink1",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour},
						TagRules: []notification.TagRule{
							{
								Tag: notification.Tag{
									Key:   "k1",
									Value: "v1",
								},
								Operator: notification.NotEqual,
							},
							{
								Tag: notification.Tag{
									Key:   "k2",
									Value: "v2",
								},
								Operator: notification.RegexEqual,
							},
						},
					},
					SubjectTemp: "subject1",
					To:          "example@host.com",
					BodyTemp:    "msg1",
				},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							TagRules: []notification.TagRule{
								{
									Tag: notification.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: notification.NotEqual,
								},
								{
									Tag: notification.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: notification.RegexEqual,
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
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							EndpointID:      IDPtr(MustIDBase16(fiveID)),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							TagRules: []notification.TagRule{
								{
									Tag: notification.Tag{
										Key:   "k1",
										Value: "v1",
									},
									Operator: notification.NotEqual,
								},
								{
									Tag: notification.Tag{
										Key:   "k2",
										Value: "v2",
									},
									Operator: notification.RegexEqual,
								},
							},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: fakeDate,
								UpdatedAt: fakeDate,
							},
						},
						SubjectTemp: "subject1",
						To:          "example@host.com",
						BodyTemp:    "msg1",
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
			err := s.CreateNotificationRule(ctx, tt.args.notificationRule, tt.args.userID)
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
					},
				},
			},
			args: args{
				id: MustIDBase16(twoID),
			},
			wants: wants{
				notificationRule: &rule.PagerDuty{
					Base: rule.Base{
						ID:              MustIDBase16(twoID),
						Name:            "name2",
						AuthorizationID: MustIDBase16(threeID),
						OrgID:           MustIDBase16(fourID),
						Status:          influxdb.Active,
						RunbookLink:     "runbooklink2",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: timeGen2.Now(),
						},
					},
					MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
				},
			},
		},
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(oneID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr1",
						},
						Channel:         "ch1",
						MessageTemplate: "msg1",
					},
					&rule.SMTP{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr2",
						},
						SubjectTemp: "subject2",
						To:          "astA@fadac.com",
						BodyTemp:    "body2",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(fourID),
							OrgID:           MustIDBase16(fourID),
							AuthorizationID: MustIDBase16(threeID),
							Status:          influxdb.Active,
							Name:            "nr3",
						},
						MessageTemp: "msg",
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

			nrs, n, err := s.FindNotificationRules(ctx, tt.args.filter)
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(fourID),
				notificationRule: &rule.PagerDuty{
					Base: rule.Base{
						ID:              MustIDBase16(twoID),
						Name:            "name2",
						AuthorizationID: MustIDBase16(threeID),
						OrgID:           MustIDBase16(fourID),
						Status:          influxdb.Inactive,
						RunbookLink:     "runbooklink3",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour * 2},
					},
					MessageTemp: "msg2",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
					},
				},
			},
			args: args{
				userID: MustIDBase16(sixID),
				id:     MustIDBase16(twoID),
				notificationRule: &rule.PagerDuty{
					Base: rule.Base{
						AuthorizationID: MustIDBase16(threeID),
						Name:            "name3",
						OrgID:           MustIDBase16(fourID),
						Status:          influxdb.Inactive,
						RunbookLink:     "runbooklink3",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour * 2},
					},
					MessageTemp: "msg2",
				},
			},
			wants: wants{
				notificationRule: &rule.PagerDuty{
					Base: rule.Base{
						ID:              MustIDBase16(twoID),
						Name:            "name3",
						AuthorizationID: MustIDBase16(threeID),
						OrgID:           MustIDBase16(fourID),
						Status:          influxdb.Inactive,
						RunbookLink:     "runbooklink3",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour * 2},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					MessageTemp: "msg2",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			tc, err := s.UpdateNotificationRule(ctx, tt.args.id,
				tt.args.notificationRule, tt.args.userID)
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
			name: "regular update",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							Status:          influxdb.Active,
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							Status:          influxdb.Active,
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
				notificationRule: &rule.PagerDuty{
					Base: rule.Base{
						ID:              MustIDBase16(twoID),
						Name:            name3,
						Status:          status3,
						AuthorizationID: MustIDBase16(threeID),
						OrgID:           MustIDBase16(fourID),
						RunbookLink:     "runbooklink2",
						SleepUntil:      &time3,
						Every:           influxdb.Duration{Duration: time.Hour},
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: timeGen1.Now(),
							UpdatedAt: fakeDate,
						},
					},
					MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
				NotificationRules: []influxdb.NotificationRule{
					&rule.Slack{
						Base: rule.Base{
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						Channel:         "channel1",
						MessageTemplate: "msg1",
					},
					&rule.PagerDuty{
						Base: rule.Base{
							ID:              MustIDBase16(twoID),
							Name:            "name2",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink2",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
							CRUDLog: influxdb.CRUDLog{
								CreatedAt: timeGen1.Now(),
								UpdatedAt: timeGen2.Now(),
							},
						},
						MessageTemp: "msg",
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
							ID:              MustIDBase16(oneID),
							Name:            "name1",
							AuthorizationID: MustIDBase16(threeID),
							OrgID:           MustIDBase16(fourID),
							Status:          influxdb.Active,
							RunbookLink:     "runbooklink1",
							SleepUntil:      &time3,
							Every:           influxdb.Duration{Duration: time.Hour},
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

			urms, _, err := s.FindUserResourceMappings(ctx, influxdb.UserResourceMappingFilter{
				UserID:       tt.args.userID,
				ResourceType: influxdb.NotificationRuleResourceType,
			})
			if err != nil {
				t.Fatalf("failed to retrieve user resource mappings: %v", err)
			}
			if diff := cmp.Diff(urms, tt.wants.userResourceMappings, userResourceMappingCmpOptions...); diff != "" {
				t.Errorf("user resource mappings are different -got/+want\ndiff %s", diff)
			}
		})
	}
}
