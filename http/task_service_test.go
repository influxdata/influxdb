package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	_ "github.com/influxdata/influxdb/v2/query/builtin"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// NewMockTaskBackend returns a TaskBackend with mock services.
func NewMockTaskBackend(t *testing.T) *TaskBackend {
	t.Helper()
	return &TaskBackend{
		log: zaptest.NewLogger(t).With(zap.String("handler", "task")),

		AlgoWProxy:                &NoopProxyHandler{},
		AuthorizationService: mock.NewAuthorizationService(),
		TaskService:          &mock.TaskService{},
		OrganizationService: &mock.OrganizationService{
			FindOrganizationByIDF: func(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
				return &influxdb.Organization{ID: id, Name: "test"}, nil
			},
			FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
				org := &influxdb.Organization{}
				if filter.Name != nil {
					if *filter.Name == "non-existent-org" {
						return nil, &influxdb.Error{
							Err:  errors.New("org not found or unauthorized"),
							Msg:  "org " + *filter.Name + " not found or unauthorized",
							Code: influxdb.ENotFound,
						}
					}
					org.Name = *filter.Name
				}
				if filter.ID != nil {
					org.ID = *filter.ID
				}

				return org, nil
			},
		},
		UserResourceMappingService: newInMemKVSVC(t),
		LabelService:               mock.NewLabelService(),
		UserService:                mock.NewUserService(),
	}
}

func TestTaskHandler_handleGetTasks(t *testing.T) {
	type fields struct {
		taskService  influxdb.TaskService
		labelService influxdb.LabelService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name      string
		getParams string
		fields    fields
		wants     wants
	}{
		{
			name: "get tasks",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
						tasks := []*influxdb.Task{
							{
								ID:              1,
								Name:            "task1",
								Description:     "A little Task",
								OrganizationID:  1,
								OwnerID:         1,
								Organization:    "test",
								AuthorizationID: 0x100,
							},
							{
								ID:              2,
								Name:            "task2",
								OrganizationID:  2,
								OwnerID:         2,
								Organization:    "test",
								AuthorizationID: 0x200,
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
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
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?limit=100"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000001",
        "owners": "/api/v2/tasks/0000000000000001/owners",
        "members": "/api/v2/tasks/0000000000000001/members",
        "labels": "/api/v2/tasks/0000000000000001/labels",
        "runs": "/api/v2/tasks/0000000000000001/runs",
        "logs": "/api/v2/tasks/0000000000000001/logs"
      },
      "id": "0000000000000001",
      "name": "task1",
	  "description": "A little Task",
	  "labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
      "orgID": "0000000000000001",
      "ownerID": "0000000000000001",
      "org": "test",
      "status": "",
      "flux": ""
    },
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
	  "org": "test",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by after and limit",
			getParams: "after=0000000000000001&limit=1",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
						tasks := []*influxdb.Task{
							{
								ID:              2,
								Name:            "task2",
								OrganizationID:  2,
								OwnerID:         2,
								Organization:    "test",
								AuthorizationID: 0x200,
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
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
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?after=0000000000000001&limit=1",
    "next": "/api/v2/tasks?after=0000000000000002&limit=1"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
      "org": "test",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by org name",
			getParams: "org=test2",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
						tasks := []*influxdb.Task{
							{
								ID:              2,
								Name:            "task2",
								OrganizationID:  2,
								OwnerID:         2,
								Organization:    "test2",
								AuthorizationID: 0x200,
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
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
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks?limit=100&org=test2"
  },
  "tasks": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "labels": "/api/v2/tasks/0000000000000002/labels",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
			"labels": [
        {
          "id": "fc3dc670a4be9b9a",
          "name": "label",
          "properties": {
            "color": "fff000"
          }
        }
      ],
	  "orgID": "0000000000000002",
	  "ownerID": "0000000000000002",
	  "org": "test2",
      "status": "",
      "flux": ""
    }
  ]
}`,
			},
		},
		{
			name:      "get tasks by org name bad",
			getParams: "org=non-existent-org",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f influxdb.TaskFilter) ([]*influxdb.Task, int, error) {
						tasks := []*influxdb.Task{
							{
								ID:              1,
								Name:            "task1",
								OrganizationID:  1,
								OwnerID:         1,
								Organization:    "test2",
								AuthorizationID: 0x100,
							},
							{
								ID:              2,
								Name:            "task2",
								OrganizationID:  2,
								OwnerID:         2,
								Organization:    "test2",
								AuthorizationID: 0x200,
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f influxdb.LabelMappingFilter) ([]*influxdb.Label, error) {
						labels := []*influxdb.Label{
							{
								ID:   influxdbtesting.MustIDBase16("fc3dc670a4be9b9a"),
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
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
"code": "invalid",
"message": "failed to decode request: org non-existent-org not found or unauthorized: org not found or unauthorized"
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url?"+tt.getParams, nil)
			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			taskBackend.TaskService = tt.fields.taskService
			taskBackend.LabelService = tt.fields.labelService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetTasks(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handleGetTasks(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetTasks() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handlePostTasks(t *testing.T) {
	type args struct {
		taskCreate influxdb.TaskCreate
	}
	type fields struct {
		taskService influxdb.TaskService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		args   args
		fields fields
		wants  wants
	}{
		{
			name: "create task",
			args: args{
				taskCreate: influxdb.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
						return &influxdb.Task{
							ID:             1,
							Name:           "task1",
							Description:    "Brand New Task",
							OrganizationID: 1,
							OwnerID:        1,
							Organization:   "test",
							Flux:           "abc",
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001",
    "owners": "/api/v2/tasks/0000000000000001/owners",
    "members": "/api/v2/tasks/0000000000000001/members",
    "labels": "/api/v2/tasks/0000000000000001/labels",
    "runs": "/api/v2/tasks/0000000000000001/runs",
    "logs": "/api/v2/tasks/0000000000000001/logs"
  },
  "id": "0000000000000001",
  "name": "task1",
  "description": "Brand New Task",
  "labels": [],
  "orgID": "0000000000000001",
  "ownerID": "0000000000000001",
  "org": "test",
  "status": "",
  "flux": "abc"
}
`,
			},
		},
		{
			name: "create task - influxdb error creating task",
			args: args{
				taskCreate: influxdb.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
						return nil, influxdb.NewError(
							influxdb.WithErrorErr(errors.New("something went wrong")),
							influxdb.WithErrorMsg("something really went wrong"),
							influxdb.WithErrorCode(influxdb.EInvalid),
						)
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `
{
    "code": "invalid",
    "message": "something really went wrong: something went wrong"
}
`,
			},
		},
		{
			name: "create task - error creating task",
			args: args{
				taskCreate: influxdb.TaskCreate{
					OrganizationID: 1,
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
						return nil, errors.New("something bad happened")
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusInternalServerError,
				contentType: "application/json; charset=utf-8",
				body: `
{
    "code": "internal error",
    "message": "failed to create task: something bad happened"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.args.taskCreate)
			if err != nil {
				t.Fatalf("failed to unmarshal task: %v", err)
			}

			r := httptest.NewRequest("POST", "http://any.url", bytes.NewReader(b))
			ctx := pcontext.SetAuthorizer(context.TODO(), new(influxdb.Authorization))
			r = r.WithContext(ctx)

			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handlePostTask(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostTask() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostTask() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handlePostTask(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostTask() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handleGetRun(t *testing.T) {
	type fields struct {
		taskService influxdb.TaskService
	}
	type args struct {
		taskID influxdb.ID
		runID  influxdb.ID
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
			name: "get a run by id",
			fields: fields{
				taskService: &mock.TaskService{
					FindRunByIDFn: func(ctx context.Context, taskID influxdb.ID, runID influxdb.ID) (*influxdb.Run, error) {
						scheduledFor, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						startedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:03.155645Z")
						finishedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:13.155645Z")
						requestedAt, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						run := influxdb.Run{
							ID:           runID,
							TaskID:       taskID,
							Status:       "success",
							ScheduledFor: scheduledFor,
							StartedAt:    startedAt,
							FinishedAt:   finishedAt,
							RequestedAt:  requestedAt,
						}
						return &run, nil
					},
				},
			},
			args: args{
				taskID: 1,
				runID:  2,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001/runs/0000000000000002",
    "task": "/api/v2/tasks/0000000000000001",
    "retry": "/api/v2/tasks/0000000000000001/runs/0000000000000002/retry",
    "logs": "/api/v2/tasks/0000000000000001/runs/0000000000000002/logs"
  },
  "id": "0000000000000002",
  "taskID": "0000000000000001",
  "status": "success",
  "scheduledFor": "2018-12-01T17:00:13Z",
  "startedAt": "2018-12-01T17:00:03.155645Z",
  "finishedAt": "2018-12-01T17:00:13.155645Z",
  "requestedAt": "2018-12-01T17:00:13Z"
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.taskID.String(),
					},
					{
						Key:   "rid",
						Value: tt.args.runID.String(),
					},
				}))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{Permissions: influxdb.OperPermissions()}))
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetRun(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetRun() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetRun() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetRun(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetRun() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_handleGetRuns(t *testing.T) {
	type fields struct {
		taskService influxdb.TaskService
	}
	type args struct {
		taskID influxdb.ID
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
			name: "get runs by task id",
			fields: fields{
				taskService: &mock.TaskService{
					FindRunsFn: func(ctx context.Context, f influxdb.RunFilter) ([]*influxdb.Run, int, error) {
						scheduledFor, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						startedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:03.155645Z")
						finishedAt, _ := time.Parse(time.RFC3339Nano, "2018-12-01T17:00:13.155645Z")
						requestedAt, _ := time.Parse(time.RFC3339, "2018-12-01T17:00:13Z")
						runs := []*influxdb.Run{
							{
								ID:           influxdb.ID(2),
								TaskID:       f.Task,
								Status:       "success",
								ScheduledFor: scheduledFor,
								StartedAt:    startedAt,
								FinishedAt:   finishedAt,
								RequestedAt:  requestedAt,
							},
						}
						return runs, len(runs), nil
					},
				},
			},
			args: args{
				taskID: 1,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/tasks/0000000000000001/runs",
    "task": "/api/v2/tasks/0000000000000001"
  },
  "runs": [
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000001/runs/0000000000000002",
        "task": "/api/v2/tasks/0000000000000001",
        "retry": "/api/v2/tasks/0000000000000001/runs/0000000000000002/retry",
        "logs": "/api/v2/tasks/0000000000000001/runs/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "taskID": "0000000000000001",
      "status": "success",
      "scheduledFor": "2018-12-01T17:00:13Z",
      "startedAt": "2018-12-01T17:00:03.155645Z",
      "finishedAt": "2018-12-01T17:00:13.155645Z",
      "requestedAt": "2018-12-01T17:00:13Z"
    }
  ]
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.taskID.String(),
					},
				}))
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{Permissions: influxdb.OperPermissions()}))
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
			h.handleGetRuns(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetRuns() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetRuns() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetRuns(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetRuns() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestTaskHandler_NotFoundStatus(t *testing.T) {
	// Ensure that the HTTP handlers return 404s for missing resources, and OKs for matching.

	im := newInMemKVSVC(t)
	taskBackend := NewMockTaskBackend(t)
	taskBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	h := NewTaskHandler(zaptest.NewLogger(t), taskBackend)
	h.UserResourceMappingService = im
	h.LabelService = im
	h.UserService = im
	h.OrganizationService = im

	o := influxdb.Organization{Name: "o"}
	ctx := context.Background()
	if err := h.OrganizationService.CreateOrganization(ctx, &o); err != nil {
		t.Fatal(err)
	}

	// Create a session to associate with the contexts, so authorization checks pass.
	authz := &influxdb.Authorization{Permissions: influxdb.OperPermissions()}

	const taskID, runID = influxdb.ID(0xCCCCCC), influxdb.ID(0xAAAAAA)

	var (
		okTask    = []interface{}{taskID}
		okTaskRun = []interface{}{taskID, runID}

		notFoundTask = [][]interface{}{
			{taskID + 1},
		}
		notFoundTaskRun = [][]interface{}{
			{taskID, runID + 1},
			{taskID + 1, runID},
			{taskID + 1, runID + 1},
		}
	)

	tcs := []struct {
		name             string
		svc              *mock.TaskService
		method           string
		body             string
		pathFmt          string
		okPathArgs       []interface{}
		notFoundPathArgs [][]interface{}
	}{
		{
			name: "get task",
			svc: &mock.TaskService{
				FindTaskByIDFn: func(_ context.Context, id influxdb.ID) (*influxdb.Task, error) {
					if id == taskID {
						return &influxdb.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, influxdb.ErrTaskNotFound
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "update task",
			svc: &mock.TaskService{
				UpdateTaskFn: func(_ context.Context, id influxdb.ID, _ influxdb.TaskUpdate) (*influxdb.Task, error) {
					if id == taskID {
						return &influxdb.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, influxdb.ErrTaskNotFound
				},
			},
			method:           http.MethodPatch,
			body:             `{"status": "active"}`,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "delete task",
			svc: &mock.TaskService{
				DeleteTaskFn: func(_ context.Context, id influxdb.ID) error {
					if id == taskID {
						return nil
					}

					return influxdb.ErrTaskNotFound
				},
			},
			method:           http.MethodDelete,
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get task logs",
			svc: &mock.TaskService{
				FindLogsFn: func(_ context.Context, f influxdb.LogFilter) ([]*influxdb.Log, int, error) {
					if f.Task == taskID {
						return nil, 0, nil
					}

					return nil, 0, influxdb.ErrTaskNotFound
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/logs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get run logs",
			svc: &mock.TaskService{
				FindLogsFn: func(_ context.Context, f influxdb.LogFilter) ([]*influxdb.Log, int, error) {
					if f.Task != taskID {
						return nil, 0, influxdb.ErrTaskNotFound
					}
					if *f.Run != runID {
						return nil, 0, influxdb.ErrNoRunsFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs/%s/logs",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "get runs: task not found",
			svc: &mock.TaskService{
				FindRunsFn: func(_ context.Context, f influxdb.RunFilter) ([]*influxdb.Run, int, error) {
					if f.Task != taskID {
						return nil, 0, influxdb.ErrTaskNotFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get runs: task found but no runs found",
			svc: &mock.TaskService{
				FindRunsFn: func(_ context.Context, f influxdb.RunFilter) ([]*influxdb.Run, int, error) {
					if f.Task != taskID {
						return nil, 0, influxdb.ErrNoRunsFound
					}

					return nil, 0, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "force run",
			svc: &mock.TaskService{
				ForceRunFn: func(_ context.Context, tid influxdb.ID, _ int64) (*influxdb.Run, error) {
					if tid != taskID {
						return nil, influxdb.ErrTaskNotFound
					}

					return &influxdb.Run{ID: runID, TaskID: taskID, Status: influxdb.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodPost,
			body:             "{}",
			pathFmt:          "/tasks/%s/runs",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "get run",
			svc: &mock.TaskService{
				FindRunByIDFn: func(_ context.Context, tid, rid influxdb.ID) (*influxdb.Run, error) {
					if tid != taskID {
						return nil, influxdb.ErrTaskNotFound
					}
					if rid != runID {
						return nil, influxdb.ErrRunNotFound
					}

					return &influxdb.Run{ID: runID, TaskID: taskID, Status: influxdb.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodGet,
			pathFmt:          "/tasks/%s/runs/%s",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "retry run",
			svc: &mock.TaskService{
				RetryRunFn: func(_ context.Context, tid, rid influxdb.ID) (*influxdb.Run, error) {
					if tid != taskID {
						return nil, influxdb.ErrTaskNotFound
					}
					if rid != runID {
						return nil, influxdb.ErrRunNotFound
					}

					return &influxdb.Run{ID: runID, TaskID: taskID, Status: influxdb.RunScheduled.String()}, nil
				},
			},
			method:           http.MethodPost,
			pathFmt:          "/tasks/%s/runs/%s/retry",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
		{
			name: "cancel run",
			svc: &mock.TaskService{
				CancelRunFn: func(_ context.Context, tid, rid influxdb.ID) error {
					if tid != taskID {
						return influxdb.ErrTaskNotFound
					}
					if rid != runID {
						return influxdb.ErrRunNotFound
					}

					return nil
				},
			},
			method:           http.MethodDelete,
			pathFmt:          "/tasks/%s/runs/%s",
			okPathArgs:       okTaskRun,
			notFoundPathArgs: notFoundTaskRun,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h.TaskService = tc.svc

			okPath := fmt.Sprintf(tc.pathFmt, tc.okPathArgs...)
			t.Run("matching ID: "+tc.method+" "+okPath, func(t *testing.T) {
				w := httptest.NewRecorder()
				r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+okPath, strings.NewReader(tc.body)).WithContext(
					pcontext.SetAuthorizer(context.Background(), authz),
				)

				h.ServeHTTP(w, r)

				res := w.Result()
				defer res.Body.Close()

				if res.StatusCode < 200 || res.StatusCode > 299 {
					t.Errorf("expected OK, got %d", res.StatusCode)
					b, _ := ioutil.ReadAll(res.Body)
					t.Fatalf("body: %s", string(b))
				}
			})

			t.Run("mismatched ID", func(t *testing.T) {
				for _, nfa := range tc.notFoundPathArgs {
					path := fmt.Sprintf(tc.pathFmt, nfa...)
					t.Run(tc.method+" "+path, func(t *testing.T) {
						w := httptest.NewRecorder()
						r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+path, strings.NewReader(tc.body)).WithContext(
							pcontext.SetAuthorizer(context.Background(), authz),
						)

						h.ServeHTTP(w, r)

						res := w.Result()
						defer res.Body.Close()

						if res.StatusCode != http.StatusNotFound {
							t.Errorf("expected Not Found, got %d", res.StatusCode)
							b, _ := ioutil.ReadAll(res.Body)
							t.Fatalf("body: %s", string(b))
						}
					})
				}
			})
		})
	}
}

func TestService_handlePostTaskLabel(t *testing.T) {
	type fields struct {
		LabelService influxdb.LabelService
	}
	type args struct {
		labelMapping *influxdb.LabelMapping
		taskID       influxdb.ID
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
			name: "add label to task",
			fields: fields{
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						return &influxdb.Label{
							ID:   1,
							Name: "label",
							Properties: map[string]string{
								"color": "fff000",
							},
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, m *influxdb.LabelMapping) error { return nil },
				},
			},
			args: args{
				labelMapping: &influxdb.LabelMapping{
					ResourceID: 100,
					LabelID:    1,
				},
				taskID: 100,
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "label": {
    "id": "0000000000000001",
    "name": "label",
    "properties": {
      "color": "fff000"
    }
  },
  "links": {
    "self": "/api/v2/labels/0000000000000001"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskBE := NewMockTaskBackend(t)
			taskBE.LabelService = tt.fields.LabelService
			h := NewTaskHandler(zaptest.NewLogger(t), taskBE)

			b, err := json.Marshal(tt.args.labelMapping)
			if err != nil {
				t.Fatalf("failed to unmarshal label mapping: %v", err)
			}

			url := fmt.Sprintf("http://localhost:9999/api/v2/tasks/%s/labels", tt.args.taskID)
			r := httptest.NewRequest("POST", url, bytes.NewReader(b))
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("got %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("got %v, want %v", content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostTaskLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostTaskLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

// Test that org name to org ID translation happens properly in the HTTP layer.
// Regression test for https://github.com/influxdata/influxdb/issues/12089.
func TestTaskHandler_CreateTaskWithOrgName(t *testing.T) {
	i := newInMemKVSVC(t)
	ctx := context.Background()

	// Set up user and org.
	u := &influxdb.User{Name: "u"}
	if err := i.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: "o"}
	if err := i.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Source and destination buckets for use in task.
	bSrc := influxdb.Bucket{OrgID: o.ID, Name: "b-src"}
	if err := i.CreateBucket(ctx, &bSrc); err != nil {
		t.Fatal(err)
	}
	bDst := influxdb.Bucket{OrgID: o.ID, Name: "b-dst"}
	if err := i.CreateBucket(ctx, &bDst); err != nil {
		t.Fatal(err)
	}

	authz := influxdb.Authorization{OrgID: o.ID, UserID: u.ID, Permissions: influxdb.OperPermissions()}
	if err := i.CreateAuthorization(ctx, &authz); err != nil {
		t.Fatal(err)
	}

	ts := &mock.TaskService{
		CreateTaskFn: func(_ context.Context, tc influxdb.TaskCreate) (*influxdb.Task, error) {
			if tc.OrganizationID != o.ID {
				t.Fatalf("expected task to be created with org ID %s, got %s", o.ID, tc.OrganizationID)
			}

			return &influxdb.Task{ID: 9, OrganizationID: o.ID, OwnerID: o.ID, AuthorizationID: authz.ID, Name: "x", Flux: tc.Flux}, nil
		},
	}

	h := NewTaskHandler(zaptest.NewLogger(t), &TaskBackend{
		log: zaptest.NewLogger(t),

		TaskService:                ts,
		AuthorizationService:       i,
		OrganizationService:        i,
		UserResourceMappingService: i,
		LabelService:               i,
		UserService:                i,
		BucketService:              i,
	})

	const script = `option task = {name:"x", every:1m} from(bucket:"b-src") |> range(start:-1m) |> to(bucket:"b-dst", org:"o")`

	url := "http://localhost:9999/api/v2/tasks"

	b, err := json.Marshal(influxdb.TaskCreate{
		Flux:         script,
		Organization: o.Name,
	})
	if err != nil {
		t.Fatal(err)
	}

	r := httptest.NewRequest("POST", url, bytes.NewReader(b)).WithContext(
		pcontext.SetAuthorizer(ctx, &authz),
	)
	w := httptest.NewRecorder()

	h.handlePostTask(w, r)

	res := w.Result()
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusCreated {
		t.Logf("response body: %s", body)
		t.Fatalf("expected status created, got %v", res.StatusCode)
	}

	// The task should have been created with a valid token.
	var createdTask influxdb.Task
	if err := json.Unmarshal([]byte(body), &createdTask); err != nil {
		t.Fatal(err)
	}
	if createdTask.Flux != script {
		t.Fatalf("Unexpected script returned:\n got: %s\nwant: %s", createdTask.Flux, script)
	}
}

func TestTaskHandler_Sessions(t *testing.T) {
	t.Skip("rework these")
	// Common setup to get a working base for using tasks.
	i := newInMemKVSVC(t)

	ctx := context.Background()

	// Set up user and org.
	u := &influxdb.User{Name: "u"}
	if err := i.CreateUser(ctx, u); err != nil {
		t.Fatal(err)
	}
	o := &influxdb.Organization{Name: "o"}
	if err := i.CreateOrganization(ctx, o); err != nil {
		t.Fatal(err)
	}

	// Map user to org.
	if err := i.CreateUserResourceMapping(ctx, &influxdb.UserResourceMapping{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   o.ID,
		UserID:       u.ID,
		UserType:     influxdb.Owner,
	}); err != nil {
		t.Fatal(err)
	}

	// Source and destination buckets for use in task.
	bSrc := influxdb.Bucket{OrgID: o.ID, Name: "b-src"}
	if err := i.CreateBucket(ctx, &bSrc); err != nil {
		t.Fatal(err)
	}
	bDst := influxdb.Bucket{OrgID: o.ID, Name: "b-dst"}
	if err := i.CreateBucket(ctx, &bDst); err != nil {
		t.Fatal(err)
	}

	sessionAllPermsCtx := pcontext.SetAuthorizer(context.Background(), &influxdb.Session{
		UserID:      u.ID,
		Permissions: influxdb.OperPermissions(),
		ExpiresAt:   time.Now().Add(24 * time.Hour),
	})

	newHandler := func(t *testing.T, ts *mock.TaskService) *TaskHandler {
		return NewTaskHandler(zaptest.NewLogger(t), &TaskBackend{
			HTTPErrorHandler: kithttp.ErrorHandler(0),
			log:              zaptest.NewLogger(t),

			TaskService:                ts,
			AuthorizationService:       i,
			OrganizationService:        i,
			UserResourceMappingService: i,
			LabelService:               i,
			UserService:                i,
			BucketService:              i,
		})
	}

	t.Run("get runs for a task", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := i.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = influxdb.ID(12345)
		const runID = influxdb.ID(9876)

		var findRunsCtx context.Context
		ts := &mock.TaskService{
			FindRunsFn: func(ctx context.Context, f influxdb.RunFilter) ([]*influxdb.Run, int, error) {
				findRunsCtx = ctx
				if f.Task != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, f.Task)
				}

				return []*influxdb.Run{
					{ID: runID, TaskID: taskID},
				}, 1, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
				if id != taskID {
					return nil, influxdb.ErrTaskNotFound
				}

				return &influxdb.Task{
					ID:              taskID,
					OrganizationID:  o.ID,
					AuthorizationID: taskAuth.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:9999/api/v2/tasks/%s/runs", taskID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{{Key: "id", Value: taskID.String()}})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res := w.Result()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		authr, err := pcontext.GetAuthorizer(findRunsCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}

		orgID := authr.(*influxdb.Authorization).OrgID

		if orgID != o.ID {
			t.Fatalf("expected context's authorizer org ID to be %v, got %v", o.ID, orgID)
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := i.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("get single run for a task", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := i.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = influxdb.ID(12345)
		const runID = influxdb.ID(9876)

		var findRunByIDCtx context.Context
		ts := &mock.TaskService{
			FindRunByIDFn: func(ctx context.Context, tid, rid influxdb.ID) (*influxdb.Run, error) {
				findRunByIDCtx = ctx
				if tid != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, tid)
				}
				if rid != runID {
					t.Fatalf("expected run ID %v, got %v", runID, rid)
				}

				return &influxdb.Run{ID: runID, TaskID: taskID}, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
				if id != taskID {
					return nil, influxdb.ErrTaskNotFound
				}

				return &influxdb.Task{
					ID:              taskID,
					OrganizationID:  o.ID,
					AuthorizationID: taskAuth.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:9999/api/v2/tasks/%s/runs/%s", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetRun(w, r)

		res := w.Result()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.FindRunByID must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(findRunByIDCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := i.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("get logs for a run", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := i.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = influxdb.ID(12345)
		const runID = influxdb.ID(9876)

		var findLogsCtx context.Context
		ts := &mock.TaskService{
			FindLogsFn: func(ctx context.Context, f influxdb.LogFilter) ([]*influxdb.Log, int, error) {
				findLogsCtx = ctx
				if f.Task != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, f.Task)
				}
				if *f.Run != runID {
					t.Fatalf("expected run ID %v, got %v", runID, *f.Run)
				}

				line := influxdb.Log{Time: "time", Message: "a log line"}
				return []*influxdb.Log{&line}, 1, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
				if id != taskID {
					return nil, influxdb.ErrTaskNotFound
				}

				return &influxdb.Task{
					ID:              taskID,
					OrganizationID:  o.ID,
					AuthorizationID: taskAuth.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:9999/api/v2/tasks/%s/runs/%s/logs", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleGetLogs(w, r)

		res := w.Result()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.FindLogs must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(findLogsCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := i.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("GET", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})

	t.Run("retry a run", func(t *testing.T) {
		// Unique authorization to associate with our fake task.
		taskAuth := &influxdb.Authorization{OrgID: o.ID, UserID: u.ID}
		if err := i.CreateAuthorization(ctx, taskAuth); err != nil {
			t.Fatal(err)
		}

		const taskID = influxdb.ID(12345)
		const runID = influxdb.ID(9876)

		var retryRunCtx context.Context
		ts := &mock.TaskService{
			RetryRunFn: func(ctx context.Context, tid, rid influxdb.ID) (*influxdb.Run, error) {
				retryRunCtx = ctx
				if tid != taskID {
					t.Fatalf("expected task ID %v, got %v", taskID, tid)
				}
				if rid != runID {
					t.Fatalf("expected run ID %v, got %v", runID, rid)
				}

				return &influxdb.Run{ID: 10 * runID, TaskID: taskID}, nil
			},

			FindTaskByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Task, error) {
				if id != taskID {
					return nil, influxdb.ErrTaskNotFound
				}

				return &influxdb.Task{
					ID:              taskID,
					OrganizationID:  o.ID,
					AuthorizationID: taskAuth.ID,
				}, nil
			},
		}

		h := newHandler(t, ts)
		url := fmt.Sprintf("http://localhost:9999/api/v2/tasks/%s/runs/%s/retry", taskID, runID)
		valCtx := context.WithValue(sessionAllPermsCtx, httprouter.ParamsKey, httprouter.Params{
			{Key: "id", Value: taskID.String()},
			{Key: "rid", Value: runID.String()},
		})
		r := httptest.NewRequest("POST", url, nil).WithContext(valCtx)
		w := httptest.NewRecorder()
		h.handleRetryRun(w, r)

		res := w.Result()
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status OK, got %v", res.StatusCode)
		}

		// The context passed to TaskService.RetryRun must be a valid authorization (not a session).
		authr, err := pcontext.GetAuthorizer(retryRunCtx)
		if err != nil {
			t.Fatal(err)
		}
		if authr.Kind() != influxdb.AuthorizationKind {
			t.Fatalf("expected context's authorizer to be of kind %q, got %q", influxdb.AuthorizationKind, authr.Kind())
		}
		if authr.Identifier() != taskAuth.ID {
			t.Fatalf("expected context's authorizer ID to be %v, got %v", taskAuth.ID, authr.Identifier())
		}

		// Other user without permissions on the task or authorization should be disallowed.
		otherUser := &influxdb.User{Name: "other-" + t.Name()}
		if err := i.CreateUser(ctx, otherUser); err != nil {
			t.Fatal(err)
		}

		valCtx = pcontext.SetAuthorizer(valCtx, &influxdb.Session{
			UserID:    otherUser.ID,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		})

		r = httptest.NewRequest("POST", url, nil).WithContext(valCtx)
		w = httptest.NewRecorder()
		h.handleGetRuns(w, r)

		res = w.Result()
		body, err = ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Logf("response body: %s", body)
			t.Fatalf("expected status unauthorized, got %v", res.StatusCode)
		}
	})
}
