package rule_test

import (
	"encoding/json"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/rule"
	"github.com/influxdata/influxdb/v2/pkg/testing/assert"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
)

func lvlPtr(l notification.CheckLevel) *notification.CheckLevel {
	return &l
}

const (
	id1 = "020f755c3c082000"
	id2 = "020f755c3c082001"
	id3 = "020f755c3c082002"
)

var goodBase = rule.Base{
	ID:         influxTesting.MustIDBase16(id1),
	Name:       "name1",
	OwnerID:    influxTesting.MustIDBase16(id2),
	OrgID:      influxTesting.MustIDBase16(id3),
	EndpointID: 1,
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
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Notification Rule ID is invalid",
			},
		},
		{
			name: "empty name",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID: influxTesting.MustIDBase16(id1),
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Notification Rule Name can't be empty",
			},
		},
		{
			name: "invalid auth id",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:   influxTesting.MustIDBase16(id1),
					Name: "name1",
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Notification Rule OwnerID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:      influxTesting.MustIDBase16(id1),
					Name:    "name1",
					OwnerID: influxTesting.MustIDBase16(id2),
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Notification Rule OrgID is invalid",
			},
		},
		{
			name: "invalid org id",
			src: &rule.Slack{
				Base: rule.Base{
					ID:         influxTesting.MustIDBase16(id1),
					Name:       "name1",
					OwnerID:    influxTesting.MustIDBase16(id2),
					OrgID:      influxTesting.MustIDBase16(id3),
					EndpointID: 0,
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Notification Rule EndpointID is invalid",
			},
		},
		{
			name: "offset greater then interval",
			src: &rule.Slack{
				Base: rule.Base{
					ID:         influxTesting.MustIDBase16(id1),
					Name:       "name1",
					OwnerID:    influxTesting.MustIDBase16(id2),
					OrgID:      influxTesting.MustIDBase16(id3),
					EndpointID: 1,
					Every:      mustDuration("1m"),
					Offset:     mustDuration("2m"),
				},
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "Offset should not be equal or greater than the interval",
			},
		},
		{
			name: "empty slack message",
			src: &rule.Slack{
				Base:    goodBase,
				Channel: "channel1",
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "slack msg template is empty",
			},
		},
		{
			name: "empty pagerDuty message",
			src: &rule.PagerDuty{
				Base: goodBase,
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  "pagerduty invalid message template",
			},
		},
		{
			name: "bad tag rule",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:         influxTesting.MustIDBase16(id1),
					OwnerID:    influxTesting.MustIDBase16(id2),
					Name:       "name1",
					OrgID:      influxTesting.MustIDBase16(id3),
					EndpointID: 1,
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "k1",
								Value: "v1",
							},
							Operator: -5,
						},
					},
				},
				MessageTemplate: "body {var2}",
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `Operator is invalid`,
			},
		},
		{
			name: "bad limit",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:         influxTesting.MustIDBase16(id1),
					OwnerID:    influxTesting.MustIDBase16(id2),
					OrgID:      influxTesting.MustIDBase16(id3),
					EndpointID: 1,
					Name:       "name1",
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "k1",
								Value: "v1",
							},
							Operator: influxdb.RegexEqual,
						},
					},
					Limit: &influxdb.Limit{
						Rate: 3,
					},
				},
				MessageTemplate: "body {var2}",
			},
			err: &errors.Error{
				Code: errors.EInvalid,
				Msg:  `if limit is set, limit and limitEvery must be larger than 0`,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.src.Valid()
			influxTesting.ErrorsEqual(t, got, c.err)
		})
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
					ID:          influxTesting.MustIDBase16(id1),
					OwnerID:     influxTesting.MustIDBase16(id2),
					Name:        "name1",
					OrgID:       influxTesting.MustIDBase16(id3),
					RunbookLink: "runbooklink1",
					SleepUntil:  &time3,
					Every:       mustDuration("1h"),
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
		{
			name: "simple smtp",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:          influxTesting.MustIDBase16(id1),
					Name:        "name1",
					OwnerID:     influxTesting.MustIDBase16(id2),
					OrgID:       influxTesting.MustIDBase16(id3),
					RunbookLink: "runbooklink1",
					SleepUntil:  &time3,
					Every:       mustDuration("1h"),
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
				MessageTemplate: "msg1",
			},
		},
		{
			name: "simple pagerDuty",
			src: &rule.PagerDuty{
				Base: rule.Base{
					ID:          influxTesting.MustIDBase16(id1),
					Name:        "name1",
					OwnerID:     influxTesting.MustIDBase16(id2),
					OrgID:       influxTesting.MustIDBase16(id3),
					RunbookLink: "runbooklink1",
					SleepUntil:  &time3,
					Every:       mustDuration("1h"),
					TagRules: []notification.TagRule{
						{
							Tag: influxdb.Tag{
								Key:   "k1",
								Value: "v1",
							},
							Operator: influxdb.NotEqual,
						},
					},
					StatusRules: []notification.StatusRule{
						{
							CurrentLevel:  notification.Warn,
							PreviousLevel: lvlPtr(notification.Critical),
						},
						{
							CurrentLevel: notification.Critical,
						},
					},
					CRUDLog: influxdb.CRUDLog{
						CreatedAt: timeGen1.Now(),
						UpdatedAt: timeGen2.Now(),
					},
				},
				MessageTemplate: "msg1",
			},
		},
		{
			name: "simple telegram",
			src: &rule.Telegram{
				Base: rule.Base{
					ID:          influxTesting.MustIDBase16(id1),
					OwnerID:     influxTesting.MustIDBase16(id2),
					Name:        "name1",
					OrgID:       influxTesting.MustIDBase16(id3),
					RunbookLink: "runbooklink1",
					SleepUntil:  &time3,
					Every:       mustDuration("1h"),
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
				MessageTemplate: "blah",
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

func TestMatchingRules(t *testing.T) {
	cases := []struct {
		name       string
		tagRules   []notification.TagRule
		filterTags []influxdb.Tag
		exp        bool
	}{
		{
			name: "Matches when tagrules and filterTags are the same. ",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "a",
						Value: "b",
					},
					Operator: influxdb.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.Equal,
				},
			},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "d"},
			},
			exp: true,
		},
		{
			name: "Matches when tagrules are subset of filterTags. ",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "a",
						Value: "b",
					},
					Operator: influxdb.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.Equal,
				},
			},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "d"},
				{Key: "e", Value: "f"},
			},
			exp: true,
		},
		{
			name: "Does not match when filterTags are missing tags that are in tag rules.",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "a",
						Value: "b",
					},
					Operator: influxdb.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.Equal,
				},
			},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
			},
			exp: false,
		},
		{
			name: "Does not match when tagrule has key value pair that does not match value of same key in filterTags",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "a",
						Value: "b",
					},
					Operator: influxdb.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.Equal,
				},
			},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "X"},
			},
			exp: false,
		},
		{
			name: "Match when tagrule has key value pair that does not match value of same key in filterTags, if tagrule has notEqual operator",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "a",
						Value: "b",
					},
					Operator: influxdb.Equal,
				},
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.NotEqual,
				},
			},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "X"},
			},
			exp: true,
		},
		{
			name:     "Empty tag rule matches filterTags",
			tagRules: []notification.TagRule{},
			filterTags: []influxdb.Tag{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "X"},
			},
			exp: true,
		},
		{
			name: "Non empty tag rule matches empty filter tags",
			tagRules: []notification.TagRule{
				{
					Tag: influxdb.Tag{
						Key:   "c",
						Value: "d",
					},
					Operator: influxdb.NotEqual,
				},
			},
			filterTags: []influxdb.Tag{},
			exp:        true,
		},
		{
			name:       "Empty tag rule matches empty filter tags",
			tagRules:   []notification.TagRule{},
			filterTags: []influxdb.Tag{},
			exp:        true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			r := rule.Base{TagRules: c.tagRules}

			assert.Equal(t, r.MatchesTags(c.filterTags), c.exp, "expected NR tags to be subset of filterTags")
		})
	}
}
