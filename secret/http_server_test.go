package secret

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	influxdbhttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/inmem"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func initSecretService(f influxdbtesting.SecretServiceFields, t *testing.T) (influxdb.SecretService, func()) {
	t.Helper()
	s := inmem.NewKVStore()

	ctx := context.Background()
	if err := all.Up(ctx, zaptest.NewLogger(t), s); err != nil {
		t.Fatal(err)
	}

	storage, err := NewStore(s)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(storage)

	for _, s := range f.Secrets {
		if err := svc.PutSecrets(context.Background(), s.OrganizationID, s.Env); err != nil {
			t.Fatalf("failed to populate users")
		}
	}

	for _, ss := range f.Secrets {
		if err := svc.PutSecrets(ctx, ss.OrganizationID, ss.Env); err != nil {
			t.Fatalf("failed to populate secrets")
		}
	}

	handler := NewHandler(zaptest.NewLogger(t), "id", svc)
	router := chi.NewRouter()
	router.Mount("/api/v2/orgs/{id}/secrets", handler)
	server := httptest.NewServer(router)
	httpClient, err := influxdbhttp.NewHTTPClient(server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	client := Client{
		Client: httpClient,
	}
	return &client, server.Close
}

func TestSecretService(t *testing.T) {
	t.Parallel()
	influxdbtesting.GetSecretKeys(initSecretService, t)
	influxdbtesting.PatchSecrets(initSecretService, t)
	influxdbtesting.DeleteSecrets(initSecretService, t)
}

func TestSecretService_handleGetSecrets(t *testing.T) {
	type fields struct {
		SecretService influxdb.SecretService
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
				body:        "{\n\t\"links\": {\n\t\t\"org\": \"/api/v2/orgs/0000000000000001\",\n\t\t\"self\": \"/api/v2/orgs/0000000000000001/secrets\"\n\t},\n\t\"secrets\": [\n\t\t\"hello\",\n\t\t\"world\"\n\t]\n}",
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
				body:        "{\n\t\"links\": {\n\t\t\"org\": \"/api/v2/orgs/0000000000000001\",\n\t\t\"self\": \"/api/v2/orgs/0000000000000001/secrets\"\n\t},\n\t\"secrets\": []\n}",
			},
		},
		{
			name: "get secrets when organization has no secret keys",
			fields: fields{
				&mock.SecretService{
					GetSecretKeysFn: func(ctx context.Context, orgID platform.ID) ([]string, error) {
						return []string{}, &errors.Error{
							Code: errors.ENotFound,
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
				body:        "{\n\t\"links\": {\n\t\t\"org\": \"/api/v2/orgs/0000000000000001\",\n\t\t\"self\": \"/api/v2/orgs/0000000000000001/secrets\"\n\t},\n\t\"secrets\": []\n}",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(zaptest.NewLogger(t), "id", tt.fields.SecretService)
			router := chi.NewRouter()
			router.Mount("/api/v2/orgs/{id}/secrets", h)

			u := fmt.Sprintf("http://any.url/api/v2/orgs/%s/secrets", tt.args.orgID)
			r := httptest.NewRequest("GET", u, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, r)

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
				if string(body) != tt.wants.body {
					t.Errorf("%q. handleGetSecrets() invalid body: %q", tt.name, body)
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
			h := NewHandler(zaptest.NewLogger(t), "id", tt.fields.SecretService)
			router := chi.NewRouter()
			router.Mount("/api/v2/orgs/{id}/secrets", h)

			b, err := json.Marshal(tt.args.secrets)
			if err != nil {
				t.Fatalf("failed to marshal secrets: %v", err)
			}

			buf := bytes.NewReader(b)
			u := fmt.Sprintf("http://any.url/api/v2/orgs/%s/secrets", tt.args.orgID)
			r := httptest.NewRequest("PATCH", u, buf)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, r)

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
				if string(body) != tt.wants.body {
					t.Errorf("%q. handlePatchSecrets() invalid body", tt.name)
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
			h := NewHandler(zaptest.NewLogger(t), "id", tt.fields.SecretService)
			router := chi.NewRouter()
			router.Mount("/api/v2/orgs/{id}/secrets", h)

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

			router.ServeHTTP(w, r)

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
				if string(body) != tt.wants.body {
					t.Errorf("%q. handleDeleteSecrets() invalid body", tt.name)
				}
			}

		})
	}
}
