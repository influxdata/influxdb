package http

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/models"

	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

var user1ID = influxtesting.MustIDBase16("020f755c3c082001")

// NewMockDeleteBackend returns a DeleteBackend with mock services.
func NewMockDeleteBackend(t *testing.T) *DeleteBackend {
	return &DeleteBackend{
		log: zaptest.NewLogger(t),

		DeleteService:       mock.NewDeleteService(),
		BucketService:       mock.NewBucketService(),
		OrganizationService: mock.NewOrganizationService(),
	}
}

func TestDelete(t *testing.T) {
	type fields struct {
		DeleteService       influxdb.DeleteService
		OrganizationService influxdb.OrganizationService
		BucketService       influxdb.BucketService
	}

	type args struct {
		queryParams map[string][]string
		body        []byte
		authorizer  influxdb.Authorizer
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
			name: "missing start time",
			args: args{
				queryParams: map[string][]string{},
				body:        []byte(`{}`),
				authorizer:  &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
					"code": "invalid",
					"message": "invalid request; error parsing request json: invalid RFC3339Nano for field start, please format your time with RFC3339Nano format, example: 2009-01-02T23:00:00Z"
				  }`,
			},
		},
		{
			name: "missing stop time",
			args: args{
				queryParams: map[string][]string{},
				body:        []byte(`{"start":"2009-01-01T23:00:00Z"}`),
				authorizer:  &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
					"code": "invalid",
					"message": "invalid request; error parsing request json: invalid RFC3339Nano for field stop, please format your time with RFC3339Nano format, example: 2009-01-01T23:00:00Z"
				  }`,
			},
		},
		{
			name: "start time too soon",
			args: args{
				queryParams: map[string][]string{},
				body:        []byte(fmt.Sprintf(`{"start":"%s"}`, time.Unix(0, models.MinNanoTime-1).UTC().Format(time.RFC3339Nano))),
				authorizer:  &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`{
					"code": "invalid",
					"message": "invalid request; error parsing request json: %s"
				  }`, msgStartTooSoon),
			},
		},
		{
			name: "stop time too late",
			args: args{
				queryParams: map[string][]string{},
				body:        []byte(fmt.Sprintf(`{"start":"2020-01-01T01:01:01Z", "stop":"%s"}`, time.Unix(0, models.MaxNanoTime+1).UTC().Format(time.RFC3339Nano))),
				authorizer:  &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(`{
					"code": "invalid",
					"message": "invalid request; error parsing request json: %s"
				  }`, msgStopTooLate),
			},
		},
		{
			name: "missing org",
			args: args{
				queryParams: map[string][]string{},
				body:        []byte(`{"start":"2009-01-01T23:00:00Z","stop":"2009-11-10T01:00:00Z"}`),
				authorizer:  &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "Please provide either orgID or org",
						}
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
					"code": "invalid",
					"message": "Please provide either orgID or org"
				  }`,
			},
		},
		{
			name: "missing bucket",
			args: args{
				queryParams: map[string][]string{
					"org": {"org1"},
				},
				body:       []byte(`{"start":"2009-01-01T23:00:00Z","stop":"2009-11-10T01:00:00Z"}`),
				authorizer: &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, f influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return nil, &errors.Error{
							Code: errors.EInvalid,
							Msg:  "Please provide either bucketID or bucket",
						}
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: platform.ID(1),
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body: `{
					"code": "invalid",
					"message": "Please provide either bucketID or bucket"
				  }`,
			},
		},
		{
			name: "insufficient permissions delete",
			args: args{
				queryParams: map[string][]string{
					"org":    {"org1"},
					"bucket": {"buck1"},
				},
				body:       []byte(`{"start":"2009-01-01T23:00:00Z","stop":"2019-11-10T01:00:00Z"}`),
				authorizer: &influxdb.Authorization{UserID: user1ID},
			},
			fields: fields{
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, f influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platform.ID(2),
							Name: "bucket1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID: platform.ID(1),
						}, nil
					},
				},
			},
			wants: wants{
				statusCode:  http.StatusForbidden,
				contentType: "application/json; charset=utf-8",
				body: `{
					"code": "forbidden",
					"message": "insufficient permissions to delete"
				  }`,
			},
		},
		{
			name: "no predicate delete",
			args: args{
				queryParams: map[string][]string{
					"org":    {"org1"},
					"bucket": {"buck1"},
				},
				body: []byte(`{"start":"2009-01-01T23:00:00Z","stop":"2019-11-10T01:00:00Z"}`),
				authorizer: &influxdb.Authorization{
					UserID: user1ID,
					Status: influxdb.Active,
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.WriteAction,
							Resource: influxdb.Resource{
								Type:  influxdb.BucketsResourceType,
								ID:    influxtesting.IDPtr(platform.ID(2)),
								OrgID: influxtesting.IDPtr(platform.ID(1)),
							},
						},
					},
				},
			},
			fields: fields{
				DeleteService: mock.NewDeleteService(),
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, f influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platform.ID(2),
							Name: "bucket1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platform.ID(1),
							Name: "org1",
						}, nil
					},
				},
			},
			wants: wants{
				statusCode: http.StatusNoContent,
				body:       ``,
			},
		},
		{
			name: "unsupported delete",
			args: args{
				queryParams: map[string][]string{
					"org":    {"org1"},
					"bucket": {"buck1"},
				},
				body: []byte(`{
					"start":"2009-01-01T23:00:00Z",
					"stop":"2019-11-10T01:00:00Z",
					"predicate": "tag1=\"v1\" and (tag2=\"v2\" or tag3=\"v3\")"
				}`),
				authorizer: &influxdb.Authorization{
					UserID: user1ID,
					Status: influxdb.Active,
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.WriteAction,
							Resource: influxdb.Resource{
								Type:  influxdb.BucketsResourceType,
								ID:    influxtesting.IDPtr(platform.ID(2)),
								OrgID: influxtesting.IDPtr(platform.ID(1)),
							},
						},
					},
				},
			},
			fields: fields{
				DeleteService: mock.NewDeleteService(),
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, f influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platform.ID(2),
							Name: "bucket1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platform.ID(1),
							Name: "org1",
						}, nil
					},
				},
			},
			wants: wants{
				statusCode: http.StatusBadRequest,
				body: `{
					"code": "invalid",
					"message": "invalid request; error parsing request json: the logical operator OR is not supported yet at position 25"
				  }`,
			},
		},
		{
			name: "complex delete",
			args: args{
				queryParams: map[string][]string{
					"org":    {"org1"},
					"bucket": {"buck1"},
				},
				body: []byte(`{
					"start":"2009-01-01T23:00:00Z",
					"stop":"2019-11-10T01:00:00Z",
					"predicate": "tag1=\"v1\" and (tag2=\"v2\" and tag3=\"v3\")"
				}`),
				authorizer: &influxdb.Authorization{
					UserID: user1ID,
					Status: influxdb.Active,
					Permissions: []influxdb.Permission{
						{
							Action: influxdb.WriteAction,
							Resource: influxdb.Resource{
								Type:  influxdb.BucketsResourceType,
								ID:    influxtesting.IDPtr(platform.ID(2)),
								OrgID: influxtesting.IDPtr(platform.ID(1)),
							},
						},
					},
				},
			},
			fields: fields{
				DeleteService: mock.NewDeleteService(),
				BucketService: &mock.BucketService{
					FindBucketFn: func(ctx context.Context, f influxdb.BucketFilter) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platform.ID(2),
							Name: "bucket1",
						}, nil
					},
				},
				OrganizationService: &mock.OrganizationService{
					FindOrganizationF: func(ctx context.Context, f influxdb.OrganizationFilter) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platform.ID(1),
							Name: "org1",
						}, nil
					},
				},
			},
			wants: wants{
				statusCode: http.StatusNoContent,
				body:       ``,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleteBackend := NewMockDeleteBackend(t)
			deleteBackend.HTTPErrorHandler = kithttp.NewErrorHandler(zaptest.NewLogger(t))
			deleteBackend.DeleteService = tt.fields.DeleteService
			deleteBackend.OrganizationService = tt.fields.OrganizationService
			deleteBackend.BucketService = tt.fields.BucketService
			h := NewDeleteHandler(zaptest.NewLogger(t), deleteBackend)

			r := httptest.NewRequest("POST", "http://any.tld", bytes.NewReader(tt.args.body))

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), tt.args.authorizer))
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleDelete(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDelete() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDelete() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDelete(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDelete() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}
