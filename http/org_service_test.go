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

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/inmem"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

// NewMockOrgBackend returns a OrgBackend with mock services.
func NewMockOrgBackend(t *testing.T) *OrgBackend {
	return &OrgBackend{
		log: zaptest.NewLogger(t),

		OrganizationService:             mock.NewOrganizationService(),
		OrganizationOperationLogService: mock.NewOrganizationOperationLogService(),
		UserResourceMappingService:      mock.NewUserResourceMappingService(),
		SecretService:                   mock.NewSecretService(),
		LabelService:                    mock.NewLabelService(),
		UserService:                     mock.NewUserService(),
	}
}

func initOrganizationService(f influxdbtesting.OrganizationFields, t *testing.T) (influxdb.OrganizationService, string, func()) {
	t.Helper()

	ctx := context.Background()
	logger := zaptest.NewLogger(t)
	store := NewTestInmemStore(t)
	svc := kv.NewService(logger, store)
	svc.IDGenerator = f.IDGenerator
	svc.OrgIDs = f.OrgBucketIDs
	svc.BucketIDs = f.OrgBucketIDs
	svc.TimeGenerator = f.TimeGenerator
	if f.TimeGenerator == nil {
		svc.TimeGenerator = influxdb.RealTimeGenerator{}
	}

	for _, o := range f.Organizations {
		// PutOrgs no longer creates an ID
		// that is what CreateOrganization does
		// so we have to generate one
		o.ID = svc.OrgIDs.ID()
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}

	orgBackend := NewMockOrgBackend(t)
	orgBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	orgBackend.OrganizationService = svc
	orgBackend.SecretService = svc
	handler := NewOrgHandler(zaptest.NewLogger(t), orgBackend)
	server := httptest.NewServer(handler)
	client := OrganizationService{
		Client: mustNewHTTPClient(t, server.URL, ""),
	}
	done := server.Close

	return &client, "", done
}

func initSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	t.Helper()

	ctx := context.Background()
	store := inmem.NewKVStore()
	logger := zaptest.NewLogger(t)
	if err := all.Up(ctx, logger, store); err != nil {
		t.Fatal(err)
	}

	svc := kv.NewService(logger, store)

	for _, ss := range f.Secrets {
		if err := svc.PutSecrets(ctx, ss.OrganizationID, ss.Env); err != nil {
			t.Fatalf("failed to populate secrets")
		}
	}

	scrBackend := NewMockOrgBackend(t)
	scrBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	scrBackend.SecretService = svc
	handler := NewOrgHandler(zaptest.NewLogger(t), scrBackend)
	server := httptest.NewServer(handler)
	client := SecretService{
		Client: mustNewHTTPClient(t, server.URL, ""),
	}
	done := server.Close

	return &client, done
}

func TestOrganizationService(t *testing.T) {
	t.Parallel()
	influxdbtesting.OrganizationService(initOrganizationService, t)
}

func TestSecretService(t *testing.T) {
	t.Parallel()
	influxdbtesting.DeleteSecrets(initSecretService, t)
	influxdbtesting.GetSecretKeys(initSecretService, t)
	influxdbtesting.PatchSecrets(initSecretService, t)
}

func TestSecretService_handleGetSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		orgID influxdb.ID
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
					GetSecretKeysFn: func(ctx context.Context, orgID influxdb.ID) ([]string, error) {
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
    "self": "/api/v2/orgs/0000000000000001/secrets"
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
					GetSecretKeysFn: func(ctx context.Context, orgID influxdb.ID) ([]string, error) {
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
    "self": "/api/v2/orgs/0000000000000001/secrets"
  },
  "secrets": []
}
`,
			},
		},
		{
			name: "get secrets when organization has no secret keys",
			fields: fields{
				&mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID influxdb.ID) ([]string, error) {
						return []string{}, &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  "organization has no secret keys",
						}

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
    "self": "/api/v2/orgs/0000000000000001/secrets"
  },
  "secrets": []
}
`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orgBackend := NewMockOrgBackend(t)
			orgBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			orgBackend.SecretService = tt.fields.SecretService
			h := NewOrgHandler(zaptest.NewLogger(t), orgBackend)

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
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetSecrets(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetSecrets() = ***%s***", tt.name, diff)
				}
			}

		})
	}
}

func TestSecretService_handlePatchSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		orgID   influxdb.ID
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
					PatchSecretsFn: func(ctx context.Context, orgID influxdb.ID, s map[string]string) error {
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
			orgBackend := NewMockOrgBackend(t)
			orgBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			orgBackend.SecretService = tt.fields.SecretService
			h := NewOrgHandler(zaptest.NewLogger(t), orgBackend)

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
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchSecrets(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchSecrets() = ***%s***", tt.name, diff)
				}
			}

		})
	}
}

func TestSecretService_handleDeleteSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
	}
	type args struct {
		orgID   influxdb.ID
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
					DeleteSecretFn: func(ctx context.Context, orgID influxdb.ID, s ...string) error {
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
			orgBackend := NewMockOrgBackend(t)
			orgBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			orgBackend.SecretService = tt.fields.SecretService
			h := NewOrgHandler(zaptest.NewLogger(t), orgBackend)

			b, err := json.Marshal(struct {
				Secrets []string `json:"secrets"`
			}{
				Secrets: tt.args.secrets,
			})
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
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteSecrets(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteSecrets() = ***%s***", tt.name, diff)
				}
			}

		})
	}
}
