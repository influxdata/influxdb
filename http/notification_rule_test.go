package http

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/influxdata/influxdb/notification"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/notification/rule"
	influxTesting "github.com/influxdata/influxdb/testing"
)

func Test_newNotificationRuleResponses(t *testing.T) {
	type args struct {
		opt    influxdb.FindOptions
		filter influxdb.NotificationRuleFilter
		nrs    []influxdb.NotificationRule
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				opt: influxdb.FindOptions{
					Limit:      50,
					Offset:     0,
					Descending: true,
				},
				filter: influxdb.NotificationRuleFilter{
					OrgID: influxTesting.IDPtr(influxdb.ID(2)),
				},
				nrs: []influxdb.NotificationRule{
					&rule.Slack{
						Channel:         "ch1",
						MessageTemplate: "message 1{var1}",
						Base: rule.Base{
							ID:          influxdb.ID(1),
							OrgID:       influxdb.ID(2),
							OwnerID:     influxdb.ID(3),
							EndpointID:  4,
							Name:        "name1",
							Description: "desc1",
							Status:      influxdb.Active,
							Every:       mustDuration("5m"),
							Offset:      mustDuration("15s"),
							TagRules: []notification.TagRule{
								{
									Tag:      influxdb.Tag{Key: "k1", Value: "v1"},
									Operator: notification.Equal,
								},
								{
									Tag:      influxdb.Tag{Key: "k2", Value: "v2"},
									Operator: notification.NotRegexEqual,
								},
							},
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.Critical,
								},
								{
									CurrentLevel: notification.Warn,
								},
							},
						},
					},
					&rule.PagerDuty{
						MessageTemplate: "body 2{var2}",
						Base: rule.Base{
							ID:          influxdb.ID(11),
							OrgID:       influxdb.ID(2),
							OwnerID:     influxdb.ID(33),
							EndpointID:  44,
							Name:        "name2",
							Description: "desc2",
							Status:      influxdb.Inactive,
						},
					},
				},
			},
			want: `{
  "links": {
    "self": "/api/v2/notificationRules?descending=true&limit=50&offset=0&orgID=0000000000000002"
  },
  "notificationRules": [
    {
      "channel": "ch1",
      "createdAt": "0001-01-01T00:00:00Z",
      "description": "desc1",
      "endpointID": "0000000000000004",
      "every": "5m",
      "id": "0000000000000001",
      "labels": [
      ],
      "links": {
        "labels": "/api/v2/notificationRules/0000000000000001/labels",
        "members": "/api/v2/notificationRules/0000000000000001/members",
        "owners": "/api/v2/notificationRules/0000000000000001/owners",
        "self": "/api/v2/notificationRules/0000000000000001"
      },
      "messageTemplate": "message 1{var1}",
      "name": "name1",
      "offset": "15s",
      "orgID": "0000000000000002",
      "ownerID": "0000000000000003",
      "runbookLink": "",
      "status": "active",
      "statusRules": [
        {
          "currentLevel": "CRIT",
          "previousLevel": null
        },
        {
          "currentLevel": "WARN",
          "previousLevel": null
        }
      ],
      "tagRules": [
        {
          "key": "k1",
          "operator": "equal",
          "value": "v1"
        },
        {
          "key": "k2",
          "operator": "notequalregex",
          "value": "v2"
        }
      ],
      "type": "slack",
      "updatedAt": "0001-01-01T00:00:00Z"
    },
    {
      "createdAt": "0001-01-01T00:00:00Z",
      "description": "desc2",
      "endpointID": "000000000000002c",
      "id": "000000000000000b",
      "labels": [
      ],
      "links": {
        "labels": "/api/v2/notificationRules/000000000000000b/labels",
        "members": "/api/v2/notificationRules/000000000000000b/members",
        "owners": "/api/v2/notificationRules/000000000000000b/owners",
        "self": "/api/v2/notificationRules/000000000000000b"
      },
      "messageTemplate": "body 2{var2}",
      "name": "name2",
      "orgID": "0000000000000002",
      "ownerID": "0000000000000021",
      "runbookLink": "",
      "status": "inactive",
      "type": "pagerduty",
      "updatedAt": "0001-01-01T00:00:00Z"
    }
  ]
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			res := newNotificationRulesResponse(ctx, tt.args.nrs, mock.NewLabelService(), tt.args.filter, tt.args.opt)
			got, err := json.Marshal(res)
			if err != nil {
				t.Fatalf("newNotificationRulesResponse() JSON marshal %v", err)
			}
			if eq, diff, _ := jsonEqual(string(got), tt.want); tt.want != "" && !eq {
				t.Errorf("%q. newNotificationRulesResponse() = ***%s***", tt.name, diff)
			}
		})
	}
}

func Test_newNotificationRuleResponse(t *testing.T) {
	type args struct {
		nr influxdb.NotificationRule
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				nr: &rule.Slack{
					Channel:         "ch1",
					MessageTemplate: "message 1{var1}",
					Base: rule.Base{
						ID:          influxdb.ID(1),
						OrgID:       influxdb.ID(2),
						OwnerID:     influxdb.ID(3),
						EndpointID:  4,
						Name:        "name1",
						Description: "desc1",
						Status:      influxdb.Active,
						Every:       mustDuration("5m"),
						Offset:      mustDuration("15s"),
						TagRules: []notification.TagRule{
							{
								Tag:      influxdb.Tag{Key: "k1", Value: "v1"},
								Operator: notification.Equal,
							},
							{
								Tag:      influxdb.Tag{Key: "k2", Value: "v2"},
								Operator: notification.NotRegexEqual,
							},
						},
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.Critical,
							},
							{
								CurrentLevel: notification.Warn,
							},
						},
					},
				},
			},
			want: `{
 "channel": "ch1",
 "createdAt": "0001-01-01T00:00:00Z",
 "description": "desc1",
 "endpointID": "0000000000000004",
 "every": "5m",
 "id": "0000000000000001",
 "labels": [
 ],
 "links": {
   "labels": "/api/v2/notificationRules/0000000000000001/labels",
   "members": "/api/v2/notificationRules/0000000000000001/members",
   "owners": "/api/v2/notificationRules/0000000000000001/owners",
   "self": "/api/v2/notificationRules/0000000000000001"
 },
 "messageTemplate": "message 1{var1}",
 "name": "name1",
 "offset": "15s",
 "orgID": "0000000000000002",
 "ownerID": "0000000000000003",
 "runbookLink": "",
 "status": "active",
 "statusRules": [
   {
     "currentLevel": "CRIT",
     "previousLevel": null
   },
   {
     "currentLevel": "WARN",
     "previousLevel": null
   }
 ],
 "tagRules": [
   {
     "key": "k1",
     "operator": "equal",
     "value": "v1"
   },
   {
     "key": "k2",
     "operator": "notequalregex",
     "value": "v2"
   }
 ],
 "type": "slack",
 "updatedAt": "0001-01-01T00:00:00Z"
}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := newNotificationRuleResponse(tt.args.nr, []*influxdb.Label{})
			got, err := json.Marshal(res)
			if err != nil {
				t.Fatalf("newNotificationRuleResponse() JSON marshal %v", err)
			}
			if eq, diff, _ := jsonEqual(string(got), tt.want); tt.want != "" && !eq {
				t.Errorf("%q. newNotificationRuleResponse() = ***%s***", tt.name, diff)
			}
		})
	}
}
