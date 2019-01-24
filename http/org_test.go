package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/inmem"
	"github.com/influxdata/influxdb/mock"
	platformtesting "github.com/influxdata/influxdb/testing"
)

func initOrganizationService(f platformtesting.OrganizationFields, t *testing.T) (platform.OrganizationService, string, func()) {
	t.Helper()
	svc := inmem.NewService()
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	handler := NewOrgHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), mock.NewUserService())
	handler.OrganizationService = svc
	server := httptest.NewServer(handler)
	client := OrganizationService{
		Addr:     server.URL,
		OpPrefix: inmem.OpPrefix,
	}
	done := server.Close

	return &client, inmem.OpPrefix, done
}
func TestOrganizationService(t *testing.T) {
	t.Parallel()
	platformtesting.OrganizationService(initOrganizationService, t)
}

func TestSecretService_handleGetSecrets(t *testing.T) {
	type fields struct {
		SecretService platform.SecretService
	}
	type args struct {
		orgID platform.ID
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
			name: "get basic secrets",
			fields: fields{
				&mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string{"hello", "world"}, nil
					},
				},
			},
			args: args{
				orgID: 1,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "org": "/api/v2/orgs/0000000000000001",
    "secrets": "/api/v2/orgs/0000000000000001/secrets"
  },
  "secrets": [
    "hello",
    "world"
  ]
}
`,
			},
		},
		{
			name: "get secrets when there are none",
			fields: fields{
				&mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string{}, nil
					},
				},
			},
			args: args{
				orgID: 1,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "org": "/api/v2/orgs/0000000000000001",
    "secrets": "/api/v2/orgs/0000000000000001/secrets"
  },
  "secrets": []
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewOrgHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), mock.NewUserService())
			h.SecretService = tt.fields.SecretService

			u := fmt.Sprintf("http://any.url/api/v2/orgs/%s/secrets", tt.args.orgID)
			r := httptest.NewRequest("GET", u, nil)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("handleGetSecrets() = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("handleGetSecrets() = %v, want %v", content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("handleGetSecrets() = ***%s***", diff)
			}

		})
	}
}

func TestSecretService_handlePatchSecrets(t *testing.T) {
	type fields struct {
		SecretService platform.SecretService
	}
	type args struct {
		orgID   platform.ID
		secrets map[string]string
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
			name: "get basic secrets",
			fields: fields{
				&mock.SecretService{
					PatchSecretsFn: func(ctx context.Context, orgID platform.ID, s map[string]string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 1,
				secrets: map[string]string{
					"abc": "123",
				},
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewOrgHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), mock.NewUserService())
			h.SecretService = tt.fields.SecretService

			b, err := json.Marshal(tt.args.secrets)
			if err != nil {
				t.Fatalf("failed to marshal secrets: %v", err)
			}

			buf := bytes.NewReader(b)
			u := fmt.Sprintf("http://any.url/api/v2/orgs/%s/secrets", tt.args.orgID)
			r := httptest.NewRequest("PATCH", u, buf)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("handlePatchSecrets() = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("handlePatchSecrets() = %v, want %v", content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("handlePatchSecrets() = ***%s***", diff)
			}

		})
	}
}

func TestSecretService_handleDeleteSecrets(t *testing.T) {
	type fields struct {
		SecretService platform.SecretService
	}
	type args struct {
		orgID   platform.ID
		secrets []string
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
			name: "get basic secrets",
			fields: fields{
				&mock.SecretService{
					DeleteSecretFn: func(ctx context.Context, orgID platform.ID, s ...string) error {
						return nil
					},
				},
			},
			args: args{
				orgID: 1,
				secrets: []string{
					"abc",
				},
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewOrgHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), mock.NewUserService())
			h.SecretService = tt.fields.SecretService

			b, err := json.Marshal(tt.args.secrets)
			if err != nil {
				t.Fatalf("failed to marshal secrets: %v", err)
			}

			buf := bytes.NewReader(b)
			u := fmt.Sprintf("http://any.url/api/v2/orgs/%s/secrets/delete", tt.args.orgID)
			r := httptest.NewRequest("POST", u, buf)
			w := httptest.NewRecorder()

			h.ServeHTTP(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("handleDeleteSecrets() = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("handleDeleteSecrets() = %v, want %v", content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("handleDeleteSecrets() = ***%s***", diff)
			}

		})
	}
}

func TestOrganizationService_FindOrganizations(t *testing.T) {
	type fields struct {
		OrganizationService platform.OrganizationService
	}
	type args struct {
		queryParams map[string][]string
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
			name: "get all orgs",
			fields: fields{
				&mock.OrganizationService{
					FindOrganizationsF: func(ctx context.Context, f platform.OrganizationFilter, o ...platform.FindOptions) ([]*platform.Organization, int, error) {
						return []*platform.Organization{
							{
								ID:   platformtesting.MustIDBase16("0000000000000001"),
								Name: "abc",
							},
							{
								ID:   platformtesting.MustIDBase16("0000000000000002"),
								Name: "def",
							},
							{
								ID:   platformtesting.MustIDBase16("0000000000000003"),
								Name: "xyz",
							},
						}, 3, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "self": "/api/v2/orgs?descending=false&limit=20&offset=0"
  },
  "orgs": [
    {
      "links": {
        "buckets": "/api/v2/buckets?org=abc",
        "dashboards": "/api/v2/dashboards?org=abc",
        "labels": "/api/v2/orgs/0000000000000001/labels",
        "log": "/api/v2/orgs/0000000000000001/log",
        "members": "/api/v2/orgs/0000000000000001/members",
        "secrets": "/api/v2/orgs/0000000000000001/secrets",
        "self": "/api/v2/orgs/0000000000000001",
        "tasks": "/api/v2/tasks?org=abc"
      },
      "id": "0000000000000001",
      "name": "abc"
    },
    {
      "links": {
        "buckets": "/api/v2/buckets?org=def",
        "dashboards": "/api/v2/dashboards?org=def",
        "labels": "/api/v2/orgs/0000000000000002/labels",
        "log": "/api/v2/orgs/0000000000000002/log",
        "members": "/api/v2/orgs/0000000000000002/members",
        "secrets": "/api/v2/orgs/0000000000000002/secrets",
        "self": "/api/v2/orgs/0000000000000002",
        "tasks": "/api/v2/tasks?org=def"
      },
      "id": "0000000000000002",
      "name": "def"
    },
    {
      "links": {
        "buckets": "/api/v2/buckets?org=xyz",
        "dashboards": "/api/v2/dashboards?org=xyz",
        "labels": "/api/v2/orgs/0000000000000003/labels",
        "log": "/api/v2/orgs/0000000000000003/log",
        "members": "/api/v2/orgs/0000000000000003/members",
        "secrets": "/api/v2/orgs/0000000000000003/secrets",
        "self": "/api/v2/orgs/0000000000000003",
        "tasks": "/api/v2/tasks?org=xyz"
      },
      "id": "0000000000000003",
      "name": "xyz"
    }
  ]
}
`,
			},
		},
		{
			name: "get orgs by offset and limit",
			fields: fields{
				&mock.OrganizationService{
					FindOrganizationsF: func(ctx context.Context, f platform.OrganizationFilter, o ...platform.FindOptions) ([]*platform.Organization, int, error) {
						return []*platform.Organization{
							{
								ID:   platformtesting.MustIDBase16("0000000000000002"),
								Name: "def",
							},
						}, 1, nil
					},
				},
			},
			args: args{
				map[string][]string{
					"offset": {"1"},
					"limit":  {"1"},
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `
{
  "links": {
    "prev": "/api/v2/orgs?descending=false&limit=1&offset=0",
    "self": "/api/v2/orgs?descending=false&limit=1&offset=1",
    "next": "/api/v2/orgs?descending=false&limit=1&offset=2"
  },
  "orgs": [
    {
      "links": {
        "buckets": "/api/v2/buckets?org=def",
        "dashboards": "/api/v2/dashboards?org=def",
        "labels": "/api/v2/orgs/0000000000000002/labels",
        "log": "/api/v2/orgs/0000000000000002/log",
        "members": "/api/v2/orgs/0000000000000002/members",
        "secrets": "/api/v2/orgs/0000000000000002/secrets",
        "self": "/api/v2/orgs/0000000000000002",
        "tasks": "/api/v2/tasks?org=def"
      },
      "id": "0000000000000002",
      "name": "def"
    }
  ]
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewOrgHandler(mock.NewUserResourceMappingService(), mock.NewLabelService(), mock.NewUserService())
			h.OrganizationService = tt.fields.OrganizationService

			r := httptest.NewRequest("GET", "http://any.url", nil)
			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()
			w := httptest.NewRecorder()
			h.handleGetOrgs(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("handleGetSecrets() = %v, want %v", res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("handleGetSecrets() = %v, want %v", content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("handleGetSecrets() = ***%s***", diff)
			}

		})
	}
}
