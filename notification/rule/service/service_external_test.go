package service

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/endpoint"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkg/pointer"
)

const (
	oneID   = "020f755c3c082000"
	twoID   = "020f755c3c082001"
	threeID = "020f755c3c082002"
	fourID  = "020f755c3c082003"
	fiveID  = "020f755c3c082004"
	sixID   = "020f755c3c082005"
)

var (
	fakeDate      = time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)
	fakeGenerator = mock.TimeGenerator{FakeValue: fakeDate}
	timeGen1      = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
	timeGen2      = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
	time3         = time.Date(2006, time.July, 15, 5, 23, 53, 10, time.UTC)
)

// NotificationRuleFields includes prepopulated data for mapping tests.
type NotificationRuleFields struct {
	IDGenerator       influxdb.IDGenerator
	TimeGenerator     influxdb.TimeGenerator
	NotificationRules []influxdb.NotificationRule
	Orgs              []*influxdb.Organization
	Tasks             []influxdb.TaskCreate
	Endpoints         []influxdb.NotificationEndpoint
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

var taskCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	// skip comparing permissions
	cmpopts.IgnoreFields(
		influxdb.Task{},
		"LatestCompleted",
		"LatestScheduled",
		"CreatedAt",
		"UpdatedAt",
	),
	cmp.Transformer("Sort", func(in []*influxdb.Task) []*influxdb.Task {
		out := append([]*influxdb.Task{}, in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID > out[j].ID
		})
		return out
	}),
}

type notificationRuleFactory func(NotificationRuleFields, *testing.T) (influxdb.NotificationRuleStore, influxdb.TaskService, func())

// NotificationRuleStore tests all the service functions.
func NotificationRuleStore(
	init notificationRuleFactory, t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(notificationRuleFactory, *testing.T)
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
	init notificationRuleFactory,
	t *testing.T,
) {
	type args struct {
		notificationRule influxdb.NotificationRule
		userID           influxdb.ID
	}
	type wants struct {
		err              error
		notificationRule influxdb.NotificationRule
		task             *influxdb.Task
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
							Value: pointer.String("abc123"),
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
				notificationRule: &rule.Slack{
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
				task: &influxdb.Task{
					ID:             MustIDBase16("020f755c3c082001"),
					Type:           "slack",
					OrganizationID: MustIDBase16("020f755c3c082003"),
					Organization:   "org",
					OwnerID:        MustIDBase16("020f755c3c082005"),
					Name:           "name2",
					Status:         "active",
					Flux:           "import \"influxdata/influxdb/monitor\"\nimport \"slack\"\nimport \"influxdata/influxdb/secrets\"\nimport \"experimental\"\n\noption task = {name: \"name2\", every: 1h}\n\nslack_secret = secrets[\"get\"](key: \"020f755c3c082001-token\")\nslack_endpoint = slack[\"endpoint\"](token: slack_secret, url: \"http://localhost:7777\")\nnotification = {_notification_rule_id: \"020f755c3c082001\", _notification_rule_name: \"name2\", _notification_endpoint_id: \"020f755c3c082001\", _notification_endpoint_name: \"foo\"}\nstatuses = monitor[\"from\"](start: -2h, fn: (r) => r[\"k1\"] == \"v1\" and r[\"k2\"] == \"v2\")\ncrit = statuses |> filter(fn: (r) => r[\"_level\"] == \"crit\")\nall_statuses = crit |> filter(fn: (r) => r[\"_time\"] >= experimental[\"subDuration\"](from: now(), d: 1h))\n\nall_statuses |> monitor[\"notify\"](data: notification, endpoint: slack_endpoint(mapFn: (r) => ({channel: \"\", text: \"msg1\", color: if r[\"_level\"] == \"crit\" then \"danger\" else if r[\"_level\"] == \"warn\" then \"warning\" else \"good\"})))",
					Every:          "1h",
				},
			},
		},
		{
			name: "invalid tag rule value",
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
							Value: pointer.String("abc123"),
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
			},
			args: args{
				userID: MustIDBase16(sixID),
				notificationRule: &rule.Slack{
					Base: rule.Base{
						OwnerID:     MustIDBase16(sixID),
						OrgID:       MustIDBase16(fourID),
						Name:        "name2",
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
									Key: "k2",
									// empty tag value to trigger validation error
									Value: "",
								},
								Operator: influxdb.RegexEqual,
							},
						},
					},
					MessageTemplate: "msg1",
				},
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "tag must contain a key and a value",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, tasks, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			nrc := influxdb.NotificationRuleCreate{
				NotificationRule: tt.args.notificationRule,
				Status:           influxdb.Active,
			}
			err := s.CreateNotificationRule(ctx, nrc, tt.args.userID)
			if tt.wants.err != nil {
				// expected error case
				if !reflect.DeepEqual(tt.wants.err, err) {
					t.Fatalf("expected error '%v' got '%v'", tt.wants.err, err)
				}

				// ensure no rules can be located
				_, n, err := s.FindNotificationRules(ctx, influxdb.NotificationRuleFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if existing := len(tt.fields.NotificationRules); n > existing {
					t.Errorf("expected no rules to be created, found %d", n-existing)
				}
			} else {
				nr, err := s.FindNotificationRuleByID(ctx, tt.args.notificationRule.GetID())
				if err != nil {
					t.Errorf("failed to retrieve notification rules: %v", err)
				}

				if diff := cmp.Diff(nr, tt.wants.notificationRule, notificationRuleCmpOptions...); diff != "" {
					t.Errorf("notificationRules are different -got/+want\ndiff %s", diff)
				}
			}

			if tt.wants.task == nil || !tt.wants.task.ID.Valid() {
				// if not tasks or a task with an invalid ID is provided (0) then assume
				// no tasks should be persisted
				_, n, err := tasks.FindTasks(ctx, influxdb.TaskFilter{})
				if err != nil {
					t.Fatal(err)
				}

				if n > 0 {
					t.Errorf("expected zero tasks to be created, instead found %d", n)
				}

				return
			}

			task, err := tasks.FindTaskByID(ctx, tt.wants.task.ID)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(task, tt.wants.task, taskCmpOptions...); diff != "" {
				t.Errorf("task is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindNotificationRuleByID testing.
func FindNotificationRuleByID(
	init notificationRuleFactory,
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
			s, _, done := init(tt.fields, t)
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
	init notificationRuleFactory,
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
				NotificationRules: []influxdb.NotificationRule{},
			},
			args: args{
				filter: influxdb.NotificationRuleFilter{},
			},
			wants: wants{
				notificationRules: []influxdb.NotificationRule{},
			},
		},
		{
			name: "find all notification rules",
			fields: NotificationRuleFields{
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
				filter: influxdb.NotificationRuleFilter{},
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
					OrgID: MustIDBase16Ptr(oneID),
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
					Organization: pointer.String("org4"),
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
					OrgID: MustIDBase16Ptr(oneID),
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
					OrgID: MustIDBase16Ptr(threeID),
				},
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, done := init(tt.fields, t)
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
	init notificationRuleFactory,
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
							Value: pointer.String("abc123"),
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
			s, _, done := init(tt.fields, t)
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
	init notificationRuleFactory,
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
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: pointer.String("abc123"),
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
				Endpoints: []influxdb.NotificationEndpoint{
					&endpoint.Slack{
						URL: "http://localhost:7777",
						Token: influxdb.SecretField{
							// TODO(desa): not sure why this has to end in token, but it does
							Key:   "020f755c3c082001-token",
							Value: pointer.String("abc123"),
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
			s, _, done := init(tt.fields, t)
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
	init notificationRuleFactory,
	t *testing.T,
) {
	type args struct {
		id    influxdb.ID
		orgID influxdb.ID
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
			name: "bad id",
			fields: NotificationRuleFields{
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
				id:    influxdb.ID(0),
				orgID: MustIDBase16(fourID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EInvalid,
					Msg:  "provided notification rule ID has invalid format",
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
				id:    MustIDBase16(fourID),
				orgID: MustIDBase16(fourID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "notification rule not found",
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
				id:    MustIDBase16(twoID),
				orgID: MustIDBase16(fourID),
			},
			wants: wants{
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
			s, _, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteNotificationRule(ctx, tt.args.id)
			ErrorsEqual(t, err, tt.wants.err)

			filter := influxdb.NotificationRuleFilter{
				OrgID: &tt.args.orgID,
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

// MustIDBase16 is an helper to ensure a correct ID is built during testing.
func MustIDBase16(s string) influxdb.ID {
	id, err := influxdb.IDFromString(s)
	if err != nil {
		panic(err)
	}
	return *id
}

// MustIDBase16Ptr is an helper to ensure a correct *ID is built during testing.
func MustIDBase16Ptr(s string) *influxdb.ID {
	id := MustIDBase16(s)
	return &id
}

// ErrorsEqual checks to see if the provided errors are equivalent.
func ErrorsEqual(t *testing.T, actual, expected error) {
	t.Helper()
	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		t.Errorf("unexpected error %s", actual.Error())
	}

	if expected != nil && actual == nil {
		t.Errorf("expected error %s but received nil", expected.Error())
	}

	if influxdb.ErrorCode(expected) != influxdb.ErrorCode(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error code %q but received %q", influxdb.ErrorCode(expected), influxdb.ErrorCode(actual))
	}

	if influxdb.ErrorMessage(expected) != influxdb.ErrorMessage(actual) {
		t.Logf("\nexpected: %v\nactual: %v\n\n", expected, actual)
		t.Errorf("expected error message %q but received %q", influxdb.ErrorMessage(expected), influxdb.ErrorMessage(actual))
	}
}

func idPtr(id influxdb.ID) *influxdb.ID {
	return &id
}

func mustDuration(d string) *notification.Duration {
	dur, err := time.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	ndur, err := notification.FromTimeDuration(dur)
	if err != nil {
		panic(err)
	}

	// Filter out the zero values from the duration.
	durs := make([]ast.Duration, 0, len(ndur.Values))
	for _, d := range ndur.Values {
		if d.Magnitude != 0 {
			durs = append(durs, d)
		}
	}
	ndur.Values = durs
	return &ndur
}
