package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/influxdata/flux/parser"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/notification"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/pkg/testttp"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	influxTesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

// NewMockCheckBackend returns a CheckBackend with mock services.
func NewMockCheckBackend(t *testing.T) *CheckBackend {
	return &CheckBackend{
		log: zaptest.NewLogger(t),

		CheckService:               mock.NewCheckService(),
		UserResourceMappingService: mock.NewUserResourceMappingService(),
		LabelService:               mock.NewLabelService(),
		UserService:                mock.NewUserService(),
		OrganizationService:        mock.NewOrganizationService(),
		FluxLanguageService:        fluxlang.DefaultService,
	}
}

func TestService_handleGetChecks(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
		LabelService influxdb.LabelService
	}
	type args struct {
		queryParams map[string][]string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	fl1 := 100.32
	fl2 := 200.64
	fl4 := 100.1
	fl5 := 3023.2

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get all checks",
			fields: fields{
				&mock.CheckService{
					FindChecksFn: func(ctx context.Context, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
						return []influxdb.Check{
							&check.Deadman{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("0b501e7e557ab1ed"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16("50f7ba1150f7ba11"),
									TaskID: 3,
								},
								Level: notification.Info,
							},
							&check.Threshold{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("c0175f0077a77005"),
									Name:   "example",
									OrgID:  influxTesting.MustIDBase16("7e55e118dbabb1ed"),
									TaskID: 3,
								},
								Thresholds: []check.ThresholdConfig{
									&check.Greater{
										Value:               fl1,
										ThresholdConfigBase: check.ThresholdConfigBase{Level: notification.Critical},
									},
									&check.Lesser{
										Value:               fl2,
										ThresholdConfigBase: check.ThresholdConfigBase{Level: notification.Info},
									},
									&check.Range{Min: fl4, Max: fl5, Within: true},
								},
							},
						}, 2, nil
					},
				},
				&mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxTesting.MustIDBase16("fc3dc670a4be9b9a"),
								Name: "label",
								Properties: map[string]string{
									"color": "fff000",
								},
							},
						}
						return labels, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"limit": {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/checks?descending=false&limit=1&offset=0",
    "next": "/api/v2/checks?descending=false&limit=1&offset=1"
  },
  "checks": [
    {
      "links": {
        "self": "/api/v2/checks/0b501e7e557ab1ed",
        "labels": "/api/v2/checks/0b501e7e557ab1ed/labels",
        "query": "/api/v2/checks/0b501e7e557ab1ed/query",
        "owners": "/api/v2/checks/0b501e7e557ab1ed/owners",
        "members": "/api/v2/checks/0b501e7e557ab1ed/members"
      },
			"createdAt": "0001-01-01T00:00:00Z",
			"updatedAt": "0001-01-01T00:00:00Z",
      "id": "0b501e7e557ab1ed",
			"orgID": "50f7ba1150f7ba11",
			"name": "hello",
			"level": "INFO",
			"query": {
				"builderConfig": {
					"aggregateWindow": {
						"fillValues": false,
						"period": ""
					},
					"buckets": [],
					"functions": [],
					"tags": []
				},
				"editMode": "",
				"name": "",
				"text": ""
			},
			"reportZero": false,
			"statusMessageTemplate": "",
			"tags": null,
			"taskID": "0000000000000003",
			"type": "deadman",
      "labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
			],
			"status": "active",
			"latestCompleted": "0001-01-01T00:00:00Z",
			"latestScheduled": "0001-01-01T00:00:00Z"
    },
    {
      "links": {
        "self": "/api/v2/checks/c0175f0077a77005",
        "labels": "/api/v2/checks/c0175f0077a77005/labels",
        "members": "/api/v2/checks/c0175f0077a77005/members",
        "owners": "/api/v2/checks/c0175f0077a77005/owners",
        "query": "/api/v2/checks/c0175f0077a77005/query"
      },
			"createdAt": "0001-01-01T00:00:00Z",
			"updatedAt": "0001-01-01T00:00:00Z",
      "id": "c0175f0077a77005",
      "orgID": "7e55e118dbabb1ed",
      "name": "example",
			"query": {
				"builderConfig": {
					"aggregateWindow": {
						"fillValues": false,
						"period": ""
					},
					"buckets": [],
					"functions": [],
					"tags": []
				},
				"editMode": "",
				"name": "",
				"text": ""
			},
			"statusMessageTemplate": "",
			"tags": null,
			"taskID": "0000000000000003",
			"thresholds": [
				{
					"allValues": false,
				"level": "CRIT",
				"type": "greater",
					"value": 100.32
				},
				{
					"allValues": false,
					"level": "INFO",
					"type": "lesser",
					"value": 200.64
				},
				{
					"allValues": false,
					"level": "UNKNOWN",
					"max": 3023.2,
					"min": 100.1,
					"type": "range",
					"within": true
				}
			],
			"type": "threshold",
			"labels": [
				{
					"id": "fc3dc670a4be9b9a",
					"name": "label",
					"properties": {
						"color": "fff000"
					}
				}
			],
			"status": "active",
			"latestCompleted": "0001-01-01T00:00:00Z",
			"latestScheduled": "0001-01-01T00:00:00Z"
		}
	]
}
`,
			},
		},
		{
			name: "get all checks when there are none",
			fields: fields{
				&mock.CheckService{
					FindChecksFn: func(ctx context.Context, filter influxdb.CheckFilter, opts ...influxdb.FindOptions) ([]influxdb.Check, int, error) {
						return []influxdb.Check{}, 0, nil
					},
				},
				&mock.LabelService{},
			},
			args: args{
				map[string][]string{
					"limit": {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/checks?descending=false&limit=1&offset=0"
  },
  "checks": []
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.LabelService = tt.fields.LabelService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{Status: "active"}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: influxTesting.MustIDBase16("6f626f7274697321")}))

			w := httptest.NewRecorder()

			h.handleGetChecks(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetChecks() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetChecks() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
				t.Errorf("%v", err)
				t.Errorf("%q. handleGetChecks() = ***%v***", tt.name, diff)
			}
		})
	}
}

func mustDuration(d string) *notification.Duration {
	dur, err := parser.ParseDuration(d)
	if err != nil {
		panic(err)
	}

	return (*notification.Duration)(dur)
}

func TestService_handleGetCheckQuery(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	var l float64 = 10
	var u float64 = 40
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a check query by id",
			fields: fields{
				&mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return &check.Threshold{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									TaskID: 3,
									Tags: []influxdb.Tag{
										{Key: "aaa", Value: "vaaa"},
										{Key: "bbb", Value: "vbbb"},
									},
									Every:                 mustDuration("1h"),
									StatusMessageTemplate: "whoa! {check.yeah}",
									Query: influxdb.DashboardQuery{
										Text: `from(bucket: "foo") |> range(start: -1d, stop: now()) |> filter(fn: (r) => r._field == "usage_idle") |> aggregateWindow(every: 1m, fn: mean) |> yield()`,
									},
								},
								Thresholds: []check.ThresholdConfig{
									check.Greater{
										ThresholdConfigBase: check.ThresholdConfigBase{
											Level: notification.Ok,
										},
										Value: l,
									},
									check.Lesser{
										ThresholdConfigBase: check.ThresholdConfigBase{
											Level: notification.Info,
										},
										Value: u,
									},
									check.Range{
										ThresholdConfigBase: check.ThresholdConfigBase{
											Level: notification.Warn,
										},
										Min:    l,
										Max:    u,
										Within: true,
									},
									check.Range{
										ThresholdConfigBase: check.ThresholdConfigBase{
											Level: notification.Critical,
										},
										Min:    l,
										Max:    u,
										Within: true,
									},
								},
							}, nil
						}
						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        "{\"flux\":\"package main\\nimport \\\"influxdata/influxdb/monitor\\\"\\nimport \\\"influxdata/influxdb/v1\\\"\\n\\ndata = from(bucket: \\\"foo\\\")\\n\\t|\\u003e range(start: -1h)\\n\\t|\\u003e filter(fn: (r) =\\u003e\\n\\t\\t(r._field == \\\"usage_idle\\\"))\\n\\t|\\u003e aggregateWindow(every: 1h, fn: mean, createEmpty: false)\\n\\noption task = {name: \\\"hello\\\", every: 1h}\\n\\ncheck = {\\n\\t_check_id: \\\"020f755c3c082000\\\",\\n\\t_check_name: \\\"hello\\\",\\n\\t_type: \\\"threshold\\\",\\n\\ttags: {aaa: \\\"vaaa\\\", bbb: \\\"vbbb\\\"},\\n}\\nok = (r) =\\u003e\\n\\t(r[\\\"usage_idle\\\"] \\u003e 10.0)\\ninfo = (r) =\\u003e\\n\\t(r[\\\"usage_idle\\\"] \\u003c 40.0)\\nwarn = (r) =\\u003e\\n\\t(r[\\\"usage_idle\\\"] \\u003c 40.0 and r[\\\"usage_idle\\\"] \\u003e 10.0)\\ncrit = (r) =\\u003e\\n\\t(r[\\\"usage_idle\\\"] \\u003c 40.0 and r[\\\"usage_idle\\\"] \\u003e 10.0)\\nmessageFn = (r) =\\u003e\\n\\t(\\\"whoa! {check.yeah}\\\")\\n\\ndata\\n\\t|\\u003e v1[\\\"fieldsAsCols\\\"]()\\n\\t|\\u003e monitor[\\\"check\\\"](\\n\\t\\tdata: check,\\n\\t\\tmessageFn: messageFn,\\n\\t\\tok: ok,\\n\\t\\tinfo: info,\\n\\t\\twarn: warn,\\n\\t\\tcrit: crit,\\n\\t)\"}\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{}, nil
				},
			}

			testttp.
				Get(t, path.Join(prefixChecks, tt.args.id, "/query")).
				Do(NewCheckHandler(zaptest.NewLogger(t), checkBackend)).
				ExpectStatus(tt.wants.statusCode).
				Expect(func(resp *testttp.Resp) {
					content := resp.Rec.Header().Get("Content-Type")
					if tt.wants.contentType != "" && content != tt.wants.contentType {
						t.Errorf("%q. handleGetCheckQuery() = %v, want %v", tt.name, content, tt.wants.contentType)
					}
				}).
				ExpectBody(func(body *bytes.Buffer) {
					if eq, diff, err := jsonEqual(body.String(), tt.wants.body); err != nil || tt.wants.body != "" && !eq {
						fmt.Printf("%q\n", body.String())
						t.Errorf("%q. handleGetChecks() = ***%v***", tt.name, diff)
					}
				})
		})
	}
}

func TestService_handleGetCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "get a check by id",
			fields: fields{
				&mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return &check.Deadman{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									Every:  mustDuration("3h"),
									TaskID: 3,
								},
								Level: notification.Critical,
							}, nil
						}
						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/checks/020f755c3c082000",
		    "labels": "/api/v2/checks/020f755c3c082000/labels",
		    "members": "/api/v2/checks/020f755c3c082000/members",
		    "owners": "/api/v2/checks/020f755c3c082000/owners",
		    "query": "/api/v2/checks/020f755c3c082000/query"
		  },
		  "labels": [],
		  "level": "CRIT",
		  "every": "3h",
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "query": {
            "builderConfig": {
              "aggregateWindow": {
								"fillValues": false,
								"period": ""
              },
              "buckets": [],
              "functions": [],
              "tags": []
            },
            "editMode": "",
            "name": "",
            "text": ""
          },
          "reportZero": false,
          "status": "active",
          "statusMessageTemplate": "",
          "tags": null,
          "taskID": "0000000000000003",
          "type": "deadman",
		  "orgID": "020f755c3c082000",
			"name": "hello",
			"status": "active",
			"latestCompleted": "0001-01-01T00:00:00Z",
			"latestScheduled": "0001-01-01T00:00:00Z"
		}
		`,
			},
		},
		{
			name: "not found",
			fields: fields{
				&mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (influxdb.Check, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "check not found",
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{Status: "active"}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleGetCheck(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)
			t.Logf(res.Header.Get("X-Influx-Error"))

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetCheck() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetCheck() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetCheck(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetCheck() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostCheck(t *testing.T) {
	type fields struct {
		CheckService        influxdb.CheckService
		OrganizationService influxdb.OrganizationService
	}
	type args struct {
		userID platform.ID
		check  influxdb.Check
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "create a new check",
			fields: fields{
				CheckService: &mock.CheckService{
					CreateCheckFn: func(ctx context.Context, c influxdb.CheckCreate, userID platform.ID) error {
						c.SetID(influxTesting.MustIDBase16("020f755c3c082000"))
						c.SetOwnerID(userID)
						return nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{ID: influxTesting.MustIDBase16("6f626f7274697320")}, nil
					},
				},
			},
			args: args{
				userID: influxTesting.MustIDBase16("6f626f7274697321"),
				check: &check.Deadman{
					Base: check.Base{
						Name:                  "hello",
						OrgID:                 influxTesting.MustIDBase16("6f626f7274697320"),
						OwnerID:               influxTesting.MustIDBase16("6f626f7274697321"),
						Description:           "desc1",
						StatusMessageTemplate: "msg1",
						Every:                 mustDuration("5m"),
						TaskID:                3,
						Tags: []influxdb.Tag{
							{Key: "k1", Value: "v1"},
							{Key: "k2", Value: "v2"},
						},
					},
					TimeSince:  mustDuration("13s"),
					ReportZero: true,
					Level:      notification.Warn,
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/checks/020f755c3c082000",
    "labels": "/api/v2/checks/020f755c3c082000/labels",
    "members": "/api/v2/checks/020f755c3c082000/members",
    "owners": "/api/v2/checks/020f755c3c082000/owners",
    "query": "/api/v2/checks/020f755c3c082000/query"
  },
  "reportZero": true,
  "statusMessageTemplate": "msg1",
  "tags": [
    {
      "key": "k1",
      "value": "v1"
    },
    {
      "key": "k2",
      "value": "v2"
    }
  ],
  "query": {
  	"builderConfig": {
    "aggregateWindow": {
      "fillValues": false,
      "period": ""
    },
    "buckets": [],
    "functions": [],
    "tags": []
  },
  "editMode": "",
  "name": "",
  "text": ""
},
  "taskID": "0000000000000003",
  "type": "deadman",
  "timeSince": "13s",
  "createdAt": "0001-01-01T00:00:00Z",
  "updatedAt": "0001-01-01T00:00:00Z",
  "id": "020f755c3c082000",
  "orgID": "6f626f7274697320",
  "name": "hello",
  "ownerID": "6f626f7274697321",
  "description": "desc1",
  "every": "5m",
  "level": "WARN",
	"labels": [],
	"status": "active",
	"latestCompleted": "0001-01-01T00:00:00Z",
	"latestScheduled": "0001-01-01T00:00:00Z"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.OrganizationService = tt.fields.OrganizationService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{Status: "active"}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			b, err := json.Marshal(tt.args.check)
			if err != nil {
				t.Fatalf("failed to unmarshal check: %v", err)
			}
			r := httptest.NewRequest("GET", "http://any.url?org=30", bytes.NewReader(b))
			w := httptest.NewRecorder()
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: tt.args.userID}))

			h.handlePostCheck(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostCheck() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostCheck() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostCheck(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostCheck() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleDeleteCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	type args struct {
		id string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "remove a check by id",
			fields: fields{
				&mock.CheckService{
					DeleteCheckFn: func(ctx context.Context, id platform.ID) error {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "check not found",
			fields: fields{
				&mock.CheckService{
					DeleteCheckFn: func(ctx context.Context, id platform.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  "check not found",
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handleDeleteCheck(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteCheck() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteCheck() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteCheck(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteCheck() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePatchCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	type args struct {
		id   string
		name string
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update a check name",
			fields: fields{
				&mock.CheckService{
					PatchCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							d := &check.Deadman{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									TaskID: 3,
								},
								Level: notification.Critical,
							}

							if upd.Name != nil {
								d.Name = *upd.Name
							}

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				name: "example",
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/checks/020f755c3c082000",
		    "labels": "/api/v2/checks/020f755c3c082000/labels",
		    "members": "/api/v2/checks/020f755c3c082000/members",
		    "owners": "/api/v2/checks/020f755c3c082000/owners",
		    "query": "/api/v2/checks/020f755c3c082000/query"
		  },
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "orgID": "020f755c3c082000",
		  "level": "CRIT",
		  "name": "example",
		  "query": {
				"builderConfig": {
					"aggregateWindow": {
						"fillValues": false,
						"period": ""
					},
					"buckets": [],
					"functions": [],
					"tags": []
				},
				"editMode": "",
				"name": "",
				"text": ""
			},
			"reportZero": false,
			"status": "active",
			"statusMessageTemplate": "",
			"tags": null,
			"taskID": "0000000000000003",
			"type": "deadman",
			"labels": [],
			"latestCompleted": "0001-01-01T00:00:00Z",
			"latestScheduled": "0001-01-01T00:00:00Z"
		}
		`,
			},
		},
		{
			name: "check not found",
			fields: fields{
				&mock.CheckService{
					PatchCheckFn: func(ctx context.Context, id platform.ID, upd influxdb.CheckUpdate) (influxdb.Check, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "check not found",
						}
					},
				},
			},
			args: args{
				id:   "020f755c3c082000",
				name: "hello",
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{Status: "active"}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			upd := influxdb.CheckUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}

			b, err := json.Marshal(upd)
			if err != nil {
				t.Fatalf("failed to unmarshal check update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePatchCheck(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchCheck() = %v, want %v %v", tt.name, res.StatusCode, tt.wants.statusCode, w.Header())
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchCheck() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchCheck(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchCheck() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleUpdateCheck(t *testing.T) {
	type fields struct {
		CheckService influxdb.CheckService
	}
	type args struct {
		id  string
		chk influxdb.Check
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "update a check name",
			fields: fields{
				CheckService: &mock.CheckService{
					UpdateCheckFn: func(ctx context.Context, id platform.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
						if id == influxTesting.MustIDBase16("020f755c3c082000") {
							d := &check.Deadman{
								Base: check.Base{
									ID:     influxTesting.MustIDBase16("020f755c3c082000"),
									Name:   "hello",
									OrgID:  influxTesting.MustIDBase16("020f755c3c082000"),
									TaskID: 3,
									Every:  mustDuration("1m"),
								},
							}

							d = chk.Check.(*check.Deadman)
							d.SetID(influxTesting.MustIDBase16("020f755c3c082000"))
							d.SetOrgID(influxTesting.MustIDBase16("020f755c3c082000"))

							return d, nil
						}

						return nil, fmt.Errorf("not found")
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				chk: &check.Deadman{
					Base: check.Base{
						Name:    "example",
						TaskID:  3,
						OwnerID: 42,
						OrgID:   influxTesting.MustIDBase16("020f755c3c082000"),
						Every:   mustDuration("1m"),
					},
					Level: notification.Critical,
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
		{
		  "links": {
		    "self": "/api/v2/checks/020f755c3c082000",
		    "labels": "/api/v2/checks/020f755c3c082000/labels",
		    "members": "/api/v2/checks/020f755c3c082000/members",
		    "owners": "/api/v2/checks/020f755c3c082000/owners",
		    "query": "/api/v2/checks/020f755c3c082000/query"
		  },
		  "createdAt": "0001-01-01T00:00:00Z",
		  "updatedAt": "0001-01-01T00:00:00Z",
		  "id": "020f755c3c082000",
		  "every": "1m",
		  "orgID": "020f755c3c082000",
		  "ownerID": "000000000000002a",
		  "level": "CRIT",
		  "name": "example",
		  "query": {
            "builderConfig": {
              "aggregateWindow": {
                "fillValues": false,
                "period": ""
              },
              "buckets": [],
              "functions": [],
              "tags": []
            },
            "editMode": "",
            "name": "",
            "text": ""
          },
          "reportZero": false,
          "status": "active",
          "statusMessageTemplate": "",
          "tags": null,
          "taskID": "0000000000000003",
          "type": "deadman",
					"labels": [],
					"latestCompleted": "0001-01-01T00:00:00Z",
					"latestScheduled": "0001-01-01T00:00:00Z"
		}
		`,
			},
		},
		{
			name: "check not found",
			fields: fields{
				CheckService: &mock.CheckService{
					UpdateCheckFn: func(ctx context.Context, id platform.ID, chk influxdb.CheckCreate) (influxdb.Check, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "check not found",
						}
					},
				},
			},
			args: args{
				id: "020f755c3c082000",
				chk: &check.Deadman{
					Base: check.Base{
						Name:    "example",
						OwnerID: 42,
						OrgID:   influxTesting.MustIDBase16("020f755c3c082000"),
						Every:   mustDuration("1m"),
					},
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{Status: "active"}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			b, err := json.Marshal(tt.args.chk)
			if err != nil {
				t.Fatalf("failed to unmarshal check update: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.url", bytes.NewReader(b))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))

			w := httptest.NewRecorder()

			h.handlePutCheck(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePutCheck() = %v, want %v %v %v", tt.name, res.StatusCode, tt.wants.statusCode, w.Header(), string(body))
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePutCheck() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePutCheck(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePutCheck() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostCheckMember(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		checkID string
		user    *influxdb.User
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "add a check member",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:     id,
							Name:   "name",
							Status: influxdb.Active,
						}, nil
					},
				},
			},
			args: args{
				checkID: "020f755c3c082000",
				user: &influxdb.User{
					ID: influxTesting.MustIDBase16("6f626f7274697320"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "logs": "/api/v2/users/6f626f7274697320/logs",
    "self": "/api/v2/users/6f626f7274697320"
  },
  "role": "member",
  "id": "6f626f7274697320",
	"name": "name",
	"status": "active"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.UserService = tt.fields.UserService
			checkBackend.TaskService = &mock.TaskService{
				FindTaskByIDFn: func(ctx context.Context, id platform.ID) (*taskmodel.Task, error) {
					return &taskmodel.Task{}, nil
				},
			}
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			b, err := json.Marshal(tt.args.user)
			if err != nil {
				t.Fatalf("failed to marshal user: %v", err)
			}

			path := fmt.Sprintf("/api/v2/checks/%s/members", tt.args.checkID)
			r := httptest.NewRequest("POST", path, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostCheckMember() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostCheckMember() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostCheckMember(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostCheckMember() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestService_handlePostCheckOwner(t *testing.T) {
	type fields struct {
		UserService influxdb.UserService
	}
	type args struct {
		checkID string
		user    *influxdb.User
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "add a check owner",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:     id,
							Name:   "name",
							Status: influxdb.Active,
						}, nil
					},
				},
			},
			args: args{
				checkID: "020f755c3c082000",
				user: &influxdb.User{
					ID: influxTesting.MustIDBase16("6f626f7274697320"),
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "logs": "/api/v2/users/6f626f7274697320/logs",
    "self": "/api/v2/users/6f626f7274697320"
  },
  "role": "owner",
  "id": "6f626f7274697320",
	"name": "name",
	"status": "active"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend(t)
			checkBackend.UserService = tt.fields.UserService
			h := NewCheckHandler(zaptest.NewLogger(t), checkBackend)

			b, err := json.Marshal(tt.args.user)
			if err != nil {
				t.Fatalf("failed to marshal user: %v", err)
			}

			path := fmt.Sprintf("/api/v2/checks/%s/owners", tt.args.checkID)
			r := httptest.NewRequest("POST", path, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostCheckOwner() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostCheckOwner() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostCheckOwner(). error unmarshalling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostCheckOwner() = ***%s***", tt.name, diff)
			}
		})
	}
}
