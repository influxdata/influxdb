package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/influxdata/platform"
	pcontext "github.com/influxdata/platform/context"
	"github.com/influxdata/platform/logger"
	"github.com/influxdata/platform/mock"
	_ "github.com/influxdata/platform/query/builtin"
)

func TestTaskHandler_handleGetTasks(t *testing.T) {
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
								ID:           1,
								Name:         "task1",
								Organization: 1,
								Owner:        platform.User{ID: 1, Name: "user1"},
							},
							{
								ID:           2,
								Name:         "task2",
								Organization: 2,
								Owner:        platform.User{ID: 2, Name: "user2"},
							},
						}
						return tasks, len(tasks), nil
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
        "runs": "/api/v2/tasks/0000000000000001/runs",
        "logs": "/api/v2/tasks/0000000000000001/logs"
      },
      "id": "0000000000000001",
      "name": "task1",
      "organizationId": "0000000000000001",
      "status": "",
      "flux": "",
      "owner": {
		"id":"0000000000000001",
		"name":"user1"
      }
    },
    {
      "links": {
        "self": "/api/v2/tasks/0000000000000002",
        "owners": "/api/v2/tasks/0000000000000002/owners",
        "members": "/api/v2/tasks/0000000000000002/members",
        "runs": "/api/v2/tasks/0000000000000002/runs",
        "logs": "/api/v2/tasks/0000000000000002/logs"
      },
      "id": "0000000000000002",
      "name": "task2",
      "organizationId": "0000000000000002",
      "status": "",
      "flux": "",
      "owner": {
		"id":"0000000000000002",
		"name":"user2"
      }
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

			h := NewTaskHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), logger.New(os.Stdout))
			h.TaskService = tt.fields.taskService
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
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetTasks() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}

func TestTaskHandler_handlePostTasks(t *testing.T) {
	type args struct {
		task platform.Task
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
				task: platform.Task{
					Name:         "task1",
					Organization: 1,
					Owner: platform.User{
						ID:   1,
						Name: "user1",
					},
				},
			},
			fields: fields{
				taskService: &mock.TaskService{
					CreateTaskFn: func(ctx context.Context, t *platform.Task) error {
						t.ID = 1
						return nil
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
    "runs": "/api/v2/tasks/0000000000000001/runs",
    "logs": "/api/v2/tasks/0000000000000001/logs"
  },
  "id": "0000000000000001",
  "name": "task1",
  "organizationId": "0000000000000001",
  "status": "",
  "flux": "",
  "owner": {
    "id": "0000000000000001",
    "name": "user1"
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.args.task)
			if err != nil {
				t.Fatalf("failed to unmarshal task: %v", err)
			}

			r := httptest.NewRequest("POST", "http://any.url", bytes.NewReader(b))
			ctx := pcontext.SetAuthorizer(context.TODO(), new(platform.Authorization))
			r = r.WithContext(ctx)

			w := httptest.NewRecorder()

			h := NewTaskHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), logger.New(os.Stdout))
			h.TaskService = tt.fields.taskService
			h.handlePostTask(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetTasks() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetTasks() = \n***%v***\n,\nwant\n***%v***", tt.name, string(body), tt.wants.body)
			}
		})
	}
}
