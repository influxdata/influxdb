package influxdb_test

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func TestView_MarshalJSON(t *testing.T) {
	type args struct {
		view platform.View
	}
	type wants struct {
		json string
	}
	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			args: args{
				view: platform.View{
					ViewContents: platform.ViewContents{
						ID:   platformtesting.MustIDBase16("f01dab1ef005ba11"),
						Name: "hello",
					},
					Properties: platform.XYViewProperties{
						Type: "xy",
					},
				},
			},
			wants: wants{
				json: `
{
  "id": "f01dab1ef005ba11",
  "name": "hello",
  "properties": {
    "shape": "chronograf-v2",
    "queries": null,
    "axes": null,
    "type": "xy",
    "colors": null,
    "legend": {},
    "geom": "",
    "note": "",
    "showNoteWhenEmpty": false,
    "xColumn": "",
    "yColumn": "",
    "shadeBelow": false
  }
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.MarshalIndent(tt.args.view, "", "  ")
			if err != nil {
				t.Fatalf("error marshalling json")
			}

			eq, err := jsonEqual(string(b), tt.wants.json)
			if err != nil {
				t.Fatalf("error marshalling json %v", err)
			}
			if !eq {
				t.Errorf("JSON did not match\nexpected:%s\ngot:\n%s\n", tt.wants.json, string(b))
			}
		})
	}
}

func TestService_handlePostCheckMember(t *testing.T) {
	type fields struct {
		UserService      platform.UserService
		DashboardService platform.DashboardService
	}
	type args struct {
		checkID string
		user    *platform.User
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
					FindUserByIDFn: func(ctx context.Context, id platform.ID) (*platform.User, error) {
						return &platform.User{
							ID:     id,
							Name:   "name",
							Status: platform.Active,
						}, nil
					},
				},
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id platform.ID) (platform.Check, error) {
						return &check.Deadman{}, nil
					},
				},
			},
			args: args{
				checkID: "020f755c3c082000",
				user: &platform.User{
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
		{
			name: "adding a check member fails when the check does not exist",
			fields: fields{
				UserService: &mock.UserService{
					FindUserByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.User, error) {
						return &influxdb.User{
							ID:     id,
							Name:   "name",
							Status: influxdb.Active,
						}, nil
					},
				},
				CheckService: &mock.CheckService{
					FindCheckByIDFn: func(ctx context.Context, id influxdb.ID) (influxdb.Check, error) {
						return nil, &influxdb.Error{
							Code: platform.ENotFound,
							Msg:  "check not found",
						}
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
				statusCode:  http.StatusNotFound,
				contentType: "application/json; charset=utf-8",
				body: `{
	"code": "not found",
	"message": "check not found"
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checkBackend := NewMockCheckBackend()
			checkBackend.UserService = tt.fields.UserService
			checkBackend.CheckService = tt.fields.CheckService
			checkBackend.HTTPErrorHandler = ErrorHandler(0)

			h := NewCheckHandler(checkBackend)

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
			if eq, err := jsonEqual(string(body), tt.wants.body); err != nil {
				t.Errorf("%q, handlePostCheckMember(). error unmarshaling json %v", tt.name, err)
			} else if tt.wants.body != "" && !eq {
				t.Errorf("%q. handlePostCheckMember() = ***%s***", tt.name, diff)
			}
		})
	}
}

func jsonEqual(s1, s2 string) (eq bool, err error) {
	var o1, o2 interface{}

	if err = json.Unmarshal([]byte(s1), &o1); err != nil {
		return
	}
	if err = json.Unmarshal([]byte(s2), &o2); err != nil {
		return
	}

	return cmp.Equal(o1, o2), nil
}
