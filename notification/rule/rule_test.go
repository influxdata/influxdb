package rule_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/influxdb/mock"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/notification"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/rule"
	influxTesting "github.com/influxdata/influxdb/testing"
)

const (
	id1 = "020f755c3c082000"
	id2 = "020f755c3c082001"
	id3 = "020f755c3c082002"
)

var goodBase = rule.Base{
	ID:              influxTesting.MustIDBase16(id1),
	Name:            "name1",
	AuthorizationID: influxTesting.MustIDBase16(id2),
	OrgID:           influxTesting.MustIDBase16(id3),
	Status:          influxdb.Inactive,
}

func TestValidRule(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.NotificationRule
		err  error
	}{
		{
			name: "invalid rule id",
			src:  &rule.Slack{},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule ID is invalid",
			},
		},
		{
			name: "empty name",
			src: &rule.SMTP{
				Base: rule.Base{
					ID: influxTesting.MustIDBase16(id1),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule Name can't be empty",
			},
		},
		{
			name: "invalid auth id",
			src: &rule.SMTP{
				Base: rule.Base{
					ID:   influxTesting.MustIDBase16(id1),
					Name: "name1",
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule AuthorizationID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule OrgID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &rule.Slack{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
					EndpointID:      influxTesting.IDPtr(influxdb.InvalidID()),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Notification Rule EndpointID is invalid",
			},
		},
		{
			name: "invalid status",
			src: &rule.Slack{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
				},
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "invalid status",
			},
		},
		{
			name: "empty slack message",
			src: &rule.Slack{
				Base:    goodBase,
				Channel: "channel1",
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "slack msg template is empty",
			},
		},
		{
			name: "empty smtp email",
			src: &rule.SMTP{
				Base: goodBase,
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "smtp email is empty",
			},
		},
		{
			name: "bad smtp email",
			src: &rule.SMTP{
				Base: goodBase,
				To:   "bad@@dfa.com,good@dfa.com",
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "smtp invalid email address: bad@@dfa.com",
			},
		},
		{
			name: "bad smtp subject",
			src: &rule.SMTP{
				Base: goodBase,
				To:   "good1@dfa.com, good2@dfa.com",
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "smtp empty subject template",
			},
		},
		{
			name: "empty pagerDuty message",
			src: &rule.PagerDuty{
				Base: goodBase,
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "pagerduty invalid message template",
			},
		},
		{
			name: "bad tag rule",
			src: &rule.SMTP{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					AuthorizationID: influxTesting.MustIDBase16(id2),
					Name:            "name1",
					OrgID:           influxTesting.MustIDBase16(id3),
					Status:          influxdb.Active,
					TagRules: []notification.TagRule{
						{
							Tag: notification.Tag{
								Key:   "k1",
								Value: "v1",
							},
							Operator: notification.Operator("bad"),
						},
					},
				},
				SubjectTemp: "subject 1 {var1}",
				BodyTemp:    "body {var2}",
				To:          "good1@dfa.com, good2@dfa.com",
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `Operator "bad" is invalid`,
			},
		},
		{
			name: "bad limit",
			src: &rule.SMTP{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
					Name:            "name1",
					Status:          influxdb.Active,
					TagRules: []notification.TagRule{
						{
							Tag: notification.Tag{
								Key:   "k1",
								Value: "v1",
							},
							Operator: notification.RegexEqual,
						},
					},
					Limit: &influxdb.Limit{
						Rate: 3,
					},
				},
				SubjectTemp: "subject 1 {var1}",
				BodyTemp:    "body {var2}",
				To:          "good1@dfa.com, good2@dfa.com",
			},
			err: &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  `if limit is set, limit and limitEvery must be larger than 0`,
			},
		},
	}
	for _, c := range cases {
		got := c.src.Valid()
		influxTesting.ErrorsEqual(t, got, c.err)
	}
}

var timeGen1 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 13, 4, 19, 10, 0, time.UTC)}
var timeGen2 = mock.TimeGenerator{FakeValue: time.Date(2006, time.July, 14, 5, 23, 53, 10, time.UTC)}
var time3 = time.Date(2006, time.July, 15, 5, 23, 53, 10, time.UTC)

func TestJSON(t *testing.T) {
	cases := []struct {
		name string
		src  influxdb.NotificationRule
	}{
		{
			name: "simple slack",
			src: &rule.Slack{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					AuthorizationID: influxTesting.MustIDBase16(id2),
					Name:            "name1",
					OrgID:           influxTesting.MustIDBase16(id3),
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
		{
			name: "simple smtp",
			src: &rule.SMTP{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
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
				SubjectTemp: "subject1",
				To:          "example@host.com",
				BodyTemp:    "msg1",
			},
		},
		{
			name: "simple pagerDuty",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:              influxTesting.MustIDBase16(id1),
					Name:            "name1",
					AuthorizationID: influxTesting.MustIDBase16(id2),
					OrgID:           influxTesting.MustIDBase16(id3),
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
					},
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel:  notification.LevelRule{CheckLevel: notification.Warn, Operation: true},
							PreviousLevel: &notification.LevelRule{CheckLevel: notification.Critical, Operation: false},
							Count:         3,
							Period:        influxdb.Duration{Duration: time.Minute * 13},
						},
						{
							CurrentLevel: notification.LevelRule{CheckLevel: notification.Critical, Operation: true},
						},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: timeGen1.Now(),
						UpdatedAt: timeGen2.Now(),
					},
				},
				MessageTemp: "msg1",
			},
		},
	}
	for _, c := range cases {
		b, err := json.Marshal(c.src)
		if err != nil {
			t.Fatalf("%s marshal failed, err: %s", c.name, err.Error())
		}
		got, err := rule.UnmarshalJSON(b)
		if err != nil {
			t.Fatalf("%s unmarshal failed, err: %s", c.name, err.Error())
		}
		if diff := cmp.Diff(got, c.src); diff != "" {
			t.Errorf("failed %s, notification rule are different -got/+want\ndiff %s", c.name, diff)
		}
	}
}
