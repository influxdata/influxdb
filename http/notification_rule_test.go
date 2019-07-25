package http

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
							ID:              influxdb.ID(1),
							OrgID:           influxdb.ID(2),
							AuthorizationID: influxdb.ID(3),
							EndpointID:      influxTesting.IDPtr(influxdb.ID(4)),
							Name:            "name1",
							Description:     "desc1",
							Status:          influxdb.Active,
							Every:           influxdb.Duration{Duration: time.Minute * 5},
							Offset:          influxdb.Duration{Duration: time.Second * 15},
							TagRules: []notification.TagRule{
								{
									Tag:      notification.Tag{Key: "k1", Value: "v1"},
									Operator: notification.Equal,
								},
								{
									Tag:      notification.Tag{Key: "k2", Value: "v2"},
									Operator: notification.NotRegexEqual,
								},
							},
							StatusRules: []notification.StatusRule{
								{
									CurrentLevel: notification.LevelRule{CheckLevel: notification.Critical, Operation: true},
									Count:        3,
									Period:       influxdb.Duration{Duration: time.Hour},
								},
								{
									CurrentLevel: notification.LevelRule{CheckLevel: notification.Warn, Operation: false},
									Count:        30,
									Period:       influxdb.Duration{Duration: time.Minute * 30},
								},
							},
						},
					},
					&rule.SMTP{
						To:          "example@domain1.com, example@domain2.com",
						SubjectTemp: "subject 2{var2}",
						BodyTemp:    "body 2{var2}",
						Base: rule.Base{
							ID:              influxdb.ID(11),
							OrgID:           influxdb.ID(2),
							AuthorizationID: influxdb.ID(33),
							EndpointID:      influxTesting.IDPtr(influxdb.ID(44)),
							Name:            "name2",
							Description:     "desc2",
							Status:          influxdb.Inactive,
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
					      "authorizationID": "0000000000000003",
					      "channel": "ch1",
					      "createdAt": "0001-01-01T00:00:00Z",
					      "description": "desc1",
					      "endpointID": "0000000000000004",
					      "every": "5m0s",
					      "id": "0000000000000001",
					      "messageTemplate": "message 1{var1}",
					      "name": "name1",
					      "offset": "15s",
					      "orgID": "0000000000000002",
					      "runbookLink": "",
					      "status": "active",
					      "statusRules": [
					        {
					          "count": 3,
					          "currentLevel": {
								"level": "CRIT",
								"operation": "equal"
							  },
					          "period": "1h0m0s",
					          "previousLevel": null
					        },
					        {
					          "count": 30,
					          "currentLevel": {
								"level": "WARN",
								"operation": "notequal"
							  },
					          "period": "30m0s",
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
						  "updatedAt": "0001-01-01T00:00:00Z",
						  "labels": [],
                  		  "links": {
                  		    "labels": "/api/v2/notificationRules/0000000000000001/labels",
                  		    "members": "/api/v2/notificationRules/0000000000000001/members",
                  		    "owners": "/api/v2/notificationRules/0000000000000001/owners",
                  		    "self": "/api/v2/notificationRules/0000000000000001"
                  		  }
					    },
					    {
					      "authorizationID": "0000000000000021",
					      "bodyTemplate": "body 2{var2}",
					      "createdAt": "0001-01-01T00:00:00Z",
					      "description": "desc2",
					      "endpointID": "000000000000002c",
					      "every": "0s",
					      "id": "000000000000000b",
					      "name": "name2",
					      "offset": "0s",
					      "orgID": "0000000000000002",
					      "runbookLink": "",
					      "status": "inactive",
					      "subjectTemplate": "subject 2{var2}",
					      "to": "example@domain1.com, example@domain2.com",
					      "type": "smtp",
						  "updatedAt": "0001-01-01T00:00:00Z",
						  "labels": [],
                  		  "links": {
                  		    "labels": "/api/v2/notificationRules/000000000000000b/labels",
                  		    "members": "/api/v2/notificationRules/000000000000000b/members",
                  		    "owners": "/api/v2/notificationRules/000000000000000b/owners",
                  		    "self": "/api/v2/notificationRules/000000000000000b"
                  		  }
					    }
					  ]
        }`},
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
						ID:              influxdb.ID(1),
						OrgID:           influxdb.ID(2),
						AuthorizationID: influxdb.ID(3),
						EndpointID:      influxTesting.IDPtr(influxdb.ID(4)),
						Name:            "name1",
						Description:     "desc1",
						Status:          influxdb.Active,
						Every:           influxdb.Duration{Duration: time.Minute * 5},
						Offset:          influxdb.Duration{Duration: time.Second * 15},
						TagRules: []notification.TagRule{
							{
								Tag:      notification.Tag{Key: "k1", Value: "v1"},
								Operator: notification.Equal,
							},
							{
								Tag:      notification.Tag{Key: "k2", Value: "v2"},
								Operator: notification.NotRegexEqual,
							},
						},
						StatusRules: []notification.StatusRule{
							{
								CurrentLevel: notification.LevelRule{CheckLevel: notification.Critical, Operation: true},
								Count:        3,
								Period:       influxdb.Duration{Duration: time.Hour},
							},
							{
								CurrentLevel: notification.LevelRule{CheckLevel: notification.Warn, Operation: true},
								Count:        30,
								Period:       influxdb.Duration{Duration: time.Minute * 30},
							},
						},
					},
				},
			},
			want: `{
				"channel": "ch1",
				"messageTemplate": "message 1{var1}",
				"id": "0000000000000001",
	  			"orgID": "0000000000000002",
	  			"authorizationID": "0000000000000003",
	  			"endpointID": "0000000000000004",
      			"name": "name1",
	  			"description": "desc1",
				"every": "5m0s",
				"offset": "15s",
				"type": "slack",
				"runbookLink": "",
              	"status": "active",
              	"statusRules": [
                {
                  "count": 3,
                  "currentLevel": {
				    "level": "CRIT",
				    "operation": "equal"
				  },
                  "period": "1h0m0s",
                  "previousLevel": null
                },
                {
                  "count": 30,
                  "currentLevel": {
				    "level": "WARN",
				    "operation": "equal"
				  },
                  "period": "30m0s",
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
	  			"createdAt": "0001-01-01T00:00:00Z",
				"updatedAt": "0001-01-01T00:00:00Z",
				"labels": [
            	],
              "links": {
                "labels": "/api/v2/notificationRules/0000000000000001/labels",
                "members": "/api/v2/notificationRules/0000000000000001/members",
                "owners": "/api/v2/notificationRules/0000000000000001/owners",
                "self": "/api/v2/notificationRules/0000000000000001"
              }
					
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
