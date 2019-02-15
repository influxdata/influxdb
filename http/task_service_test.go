package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	platform "github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/backend"
	platformtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// NewMockTaskBackend returns a TaskBackend with mock services.
func NewMockTaskBackend(t *testing.T) *TaskBackend {
	return &TaskBackend{
		Logger: zaptest.NewLogger(t).With(zap.String("handler", "task")),

		AuthorizationService: mock.NewAuthorizationService(),
		TaskService:          &mock.TaskService{},
		OrganizationService: &mock.OrganizationService{
			FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*platform.Organization, error) {
				return &platform.Organization{ID: id, Name: "test"}, nil
			},
			FindOrganizationF: func(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
				org := &platform.Organization{}
				if filter.Name != nil {
					org.Name = *filter.Name
				}
				if filter.ID != nil {
					org.ID = *filter.ID
				}

				return org, nil
			},
		},
		UserResourceMappingService: inmem.NewService(),
		LabelService:               mock.NewLabelService(),
		UserService:                mock.NewUserService(),
	}
}

func TestTaskHandler_handleGetTasks(t *testing.T) {
	type fields struct {
		taskService  platform.TaskService
		labelService platform.LabelService
	}
	type wants struct {
		statusCode  int
		contentType string
		body        string
	}

	tests := []struct {
		name   string
		fields fields
		wants  wants
	}{
		{
			name: "get tasks",
			fields: fields{
				taskService: &mock.TaskService{
					FindTasksFn: func(ctx context.Context, f platform.TaskFilter) ([]*platform.Task, int, error) {
						tasks := []*platform.Task{
							{
								ID:              1,
								Name:            "task1",
								OrganizationID:  1,
								Owner:           platform.User{ID: 1, Name: "user1"},
								AuthorizationID: 0x100,
							},
							{
								ID:              2,
								Name:            "task2",
								OrganizationID:  2,
								Owner:           platform.User{ID: 2, Name: "user2"},
								AuthorizationID: 0x200,
							},
						}
						return tasks, len(tasks), nil
					},
				},
				labelService: &mock.LabelService{
					FindResourceLabelsFn: func(ctx context.Context, f platform.LabelMappingFilter) ([]*platform.Label, error) {
						labels := []*platform.Label{
							{
								ID:   platformtesting.MustIDBase16("fc3dc670a4be9b9a"),
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
    "self": "/api/v2/tasks"
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
      "org": "test",
      "status": "",
			"authorizationID": "0000000000000100",
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
	  "org": "test",
      "status": "",
			"authorizationID": "0000000000000200",
      "flux": ""
    }
  ]
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.TaskService = tt.fields.taskService
			taskBackend.LabelService = tt.fields.labelService
			h := NewTaskHandler(taskBackend)
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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetTasks() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestTaskHandler_handlePostTasks(t *testing.T) {
	type args struct {
		taskCreate platform.TaskCreate
	}
	type fields struct {
		taskService platform.TaskService
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
				taskCreate: platform.TaskCreate{
					OrganizationID: 1,
					Token:          "mytoken",
					Flux:           "abc",
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, tc platform.TaskCreate) (*platform.Task, error) {
						return &platform.Task{
							ID:              1,
							Name:            "task1",
							OrganizationID:  1,
							Organization:    "test",
							Owner:           platform.User{ID: 1, Name: "user1"},
							AuthorizationID: 0x100,
							Flux:            "abc",
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
  "labels": [],
  "orgID": "0000000000000001",
  "org": "test",
  "status": "",
	"authorizationID": "0000000000000100",
  "flux": "abc"
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
			ctx := pcontext.SetAuthorizer(context.TODO(), new(platform.Authorization))
			r = r.WithContext(ctx)

			w := httptest.NewRecorder()

			taskBackend := NewMockTaskBackend(t)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(taskBackend)
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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostTask() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestTaskHandler_handleGetRun(t *testing.T) {
	type fields struct {
		taskService platform.TaskService
	}
	type args struct {
		taskID platform.ID
		runID  platform.ID
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
					FindRunByIDFn: func(ctx context.Context, taskID platform.ID, runID platform.ID) (*platform.Run, error) {
						run := platform.Run{
							ID:           runID,
							TaskID:       taskID,
							Status:       "success",
							ScheduledFor: "2018-12-01T17:00:13Z",
							StartedAt:    "2018-12-01T17:00:03.155645Z",
							FinishedAt:   "2018-12-01T17:00:13.155645Z",
							RequestedAt:  "2018-12-01T17:00:13Z",
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
  "requestedAt": "2018-12-01T17:00:13Z",
  "log": ""
}`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://any.url", nil)
			r = r.WithContext(context.WithValue(
				context.TODO(),
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
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(taskBackend)
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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetRun() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestTaskHandler_handleGetRuns(t *testing.T) {
	type fields struct {
		taskService platform.TaskService
	}
	type args struct {
		taskID platform.ID
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
					FindRunsFn: func(ctx context.Context, f platform.RunFilter) ([]*platform.Run, int, error) {
						runs := []*platform.Run{
							{
								ID:           platform.ID(2),
								TaskID:       *f.Task,
								Status:       "success",
								ScheduledFor: "2018-12-01T17:00:13Z",
								StartedAt:    "2018-12-01T17:00:03.155645Z",
								FinishedAt:   "2018-12-01T17:00:13.155645Z",
								RequestedAt:  "2018-12-01T17:00:13Z",
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
      "requestedAt": "2018-12-01T17:00:13Z",
      "log": ""
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
				context.TODO(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.taskID.String(),
					},
				}))
			w := httptest.NewRecorder()
			taskBackend := NewMockTaskBackend(t)
			taskBackend.TaskService = tt.fields.taskService
			h := NewTaskHandler(taskBackend)
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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetRuns() = ***%s***", tt.name, diff)
			}
		})
	}
}

func TestTaskHandler_NotFoundStatus(t *testing.T) {
	// Ensure that the HTTP handlers return 404s for missing resources, and OKs for matching.

	im := inmem.NewService()
	taskBackend := NewMockTaskBackend(t)
	h := NewTaskHandler(taskBackend)
	h.UserResourceMappingService = im
	h.LabelService = im
	h.UserService = im
	h.OrganizationService = im

	o := platform.Organization{Name: "o"}
	ctx := context.Background()
	if err := h.OrganizationService.CreateOrganization(ctx, &o); err != nil {
		t.Fatal(err)
	}

	const taskID, runID = platform.ID(0xCCCCCC), platform.ID(0xAAAAAA)

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
				FindTaskByIDFn: func(_ context.Context, id platform.ID) (*platform.Task, error) {
					if id == taskID {
						return &platform.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, backend.ErrTaskNotFound
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
				UpdateTaskFn: func(_ context.Context, id platform.ID, _ platform.TaskUpdate) (*platform.Task, error) {
					if id == taskID {
						return &platform.Task{ID: taskID, Organization: "o"}, nil
					}

					return nil, backend.ErrTaskNotFound
				},
			},
			method:           http.MethodPatch,
			body:             "{}",
			pathFmt:          "/tasks/%s",
			okPathArgs:       okTask,
			notFoundPathArgs: notFoundTask,
		},
		{
			name: "delete task",
			svc: &mock.TaskService{
				DeleteTaskFn: func(_ context.Context, id platform.ID) error {
					if id == taskID {
						return nil
					}

					return backend.ErrTaskNotFound
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
				FindLogsFn: func(_ context.Context, f platform.LogFilter) ([]*platform.Log, int, error) {
					if *f.Task == taskID {
						return nil, 0, nil
					}

					return nil, 0, backend.ErrTaskNotFound
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
				FindLogsFn: func(_ context.Context, f platform.LogFilter) ([]*platform.Log, int, error) {
					if *f.Task != taskID {
						return nil, 0, backend.ErrTaskNotFound
					}
					if *f.Run != runID {
						return nil, 0, backend.ErrRunNotFound
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
			name: "get runs",
			svc: &mock.TaskService{
				FindRunsFn: func(_ context.Context, f platform.RunFilter) ([]*platform.Run, int, error) {
					if *f.Task != taskID {
						return nil, 0, backend.ErrTaskNotFound
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
				ForceRunFn: func(_ context.Context, tid platform.ID, _ int64) (*platform.Run, error) {
					if tid != taskID {
						return nil, backend.ErrTaskNotFound
					}

					return &platform.Run{ID: runID, TaskID: taskID, Status: backend.RunScheduled.String()}, nil
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
				FindRunByIDFn: func(_ context.Context, tid, rid platform.ID) (*platform.Run, error) {
					if tid != taskID {
						return nil, backend.ErrTaskNotFound
					}
					if rid != runID {
						return nil, backend.ErrRunNotFound
					}

					return &platform.Run{ID: runID, TaskID: taskID, Status: backend.RunScheduled.String()}, nil
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
				RetryRunFn: func(_ context.Context, tid, rid platform.ID) (*platform.Run, error) {
					if tid != taskID {
						return nil, backend.ErrTaskNotFound
					}
					if rid != runID {
						return nil, backend.ErrRunNotFound
					}

					return &platform.Run{ID: runID, TaskID: taskID, Status: backend.RunScheduled.String()}, nil
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
				CancelRunFn: func(_ context.Context, tid, rid platform.ID) error {
					if tid != taskID {
						return backend.ErrTaskNotFound
					}
					if rid != runID {
						return backend.ErrRunNotFound
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
				r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+okPath, strings.NewReader(tc.body))

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
						r := httptest.NewRequest(tc.method, "http://task.example/api/v2"+path, strings.NewReader(tc.body))

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
		LabelService platform.LabelService
	}
	type args struct {
		labelMapping *platform.LabelMapping
		taskID       platform.ID
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
					FindLabelByIDFn: func(ctx context.Context, id platform.ID) (*platform.Label, error) {
						return &platform.Label{
							ID:   1,
							Name: "label",
							Properties: map[string]string{
								"color": "fff000",
							},
						}, nil
					},
					CreateLabelMappingFn: func(ctx context.Context, m *platform.LabelMapping) error { return nil },
				},
			},
			args: args{
				labelMapping: &platform.LabelMapping{
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
			h := NewTaskHandler(taskBE)

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
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("Diff\n%s", diff)
			}
		})
	}
}
