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

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	httpMock "github.com/influxdata/influxdb/v2/http/mock"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/tenant"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

const (
	targetOneIDString = "0000000000000111"
	targetTwoIDString = "0000000000000222"
)

var (
	targetOneID = platformtesting.MustIDBase16(targetOneIDString)
	targetTwoID = platformtesting.MustIDBase16(targetTwoIDString)
)

// NewMockScraperBackend returns a ScraperBackend with mock services.
func NewMockScraperBackend(t *testing.T) *ScraperBackend {
	return &ScraperBackend{
		log: zaptest.NewLogger(t),

		ScraperStorageService:      &mock.ScraperTargetStoreService{},
		BucketService:              mock.NewBucketService(),
		OrganizationService:        mock.NewOrganizationService(),
		UserService:                mock.NewUserService(),
		UserResourceMappingService: &mock.UserResourceMappingService{},
		LabelService:               mock.NewLabelService(),
	}
}

func TestService_handleGetScraperTargets(t *testing.T) {
	type fields struct {
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
		OrganizationService       influxdb.OrganizationService
		BucketService             influxdb.BucketService
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
			name: "get all scraper targets",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{
							{
								ID:       targetOneID,
								Name:     "target-1",
								Type:     influxdb.PrometheusScraperType,
								URL:      "www.one.url",
								OrgID:    platformtesting.MustIDBase16("0000000000000211"),
								BucketID: platformtesting.MustIDBase16("0000000000000212"),
							},
							{
								ID:       targetTwoID,
								Name:     "target-2",
								Type:     influxdb.PrometheusScraperType,
								URL:      "www.two.url",
								OrgID:    platformtesting.MustIDBase16("0000000000000211"),
								BucketID: platformtesting.MustIDBase16("0000000000000212"),
							},
						}, nil
					},
				},
			},
			args: args{},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(
					`
					{
					  "links": {
					    "self": "/api/v2/scrapers"
					  },
					  "configurations": [
					    {
					      "id": "%s",
						  "name": "target-1",
						  "bucket": "bucket1",
						  "bucketID": "0000000000000212",
						  "org": "org1",
						  "orgID": "0000000000000211",
						  "type": "prometheus",
						  "url": "www.one.url",
						  "links": {
						    "bucket": "/api/v2/buckets/0000000000000212",
						    "organization": "/api/v2/orgs/0000000000000211",
						    "self": "/api/v2/scrapers/0000000000000111",
						    "members": "/api/v2/scrapers/0000000000000111/members",
						    "owners": "/api/v2/scrapers/0000000000000111/owners"
						  }
						},
						{
						  "id": "%s",
						  "name": "target-2",
						  "bucket": "bucket1",
						  "bucketID": "0000000000000212",
						  "orgID": "0000000000000211",
						  "org": "org1",
						  "type": "prometheus",
						  "url": "www.two.url",
						  "links": {
						    "bucket": "/api/v2/buckets/0000000000000212",
						    "organization": "/api/v2/orgs/0000000000000211",
						    "self": "/api/v2/scrapers/0000000000000222",
						    "members": "/api/v2/scrapers/0000000000000222/members",
						    "owners": "/api/v2/scrapers/0000000000000222/owners"
						  }
                        }
					  ]
					}
					`,
					targetOneIDString,
					targetTwoIDString,
				),
			},
		},
		{
			name: "get all scraper targets when there are none",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					ListTargetsF: func(ctx context.Context, filter influxdb.ScraperTargetFilter) ([]influxdb.ScraperTarget, error) {
						return []influxdb.ScraperTarget{}, nil
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
                    "self": "/api/v2/scrapers"
                  },
                  "configurations": []
                }
                `,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraperBackend := NewMockScraperBackend(t)
			scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			scraperBackend.ScraperStorageService = tt.fields.ScraperTargetStoreService
			scraperBackend.OrganizationService = tt.fields.OrganizationService
			scraperBackend.BucketService = tt.fields.BucketService
			h := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)

			r := httptest.NewRequest("GET", "http://any.tld", nil)

			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()

			w := httptest.NewRecorder()

			h.handleGetScraperTargets(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetScraperTargets() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetScraperTargets() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetScraperTargets(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetScraperTargets() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleGetScraperTarget(t *testing.T) {
	type fields struct {
		OrganizationService       influxdb.OrganizationService
		BucketService             influxdb.BucketService
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
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
			name: "get a scraper target by id",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					GetTargetByIDF: func(ctx context.Context, id platform.ID) (*influxdb.ScraperTarget, error) {
						if id == targetOneID {
							return &influxdb.ScraperTarget{
								ID:       targetOneID,
								Name:     "target-1",
								Type:     influxdb.PrometheusScraperType,
								URL:      "www.some.url",
								OrgID:    platformtesting.MustIDBase16("0000000000000211"),
								BucketID: platformtesting.MustIDBase16("0000000000000212"),
							}, nil
						}
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "scraper target is not found",
						}
					},
				},
			},
			args: args{
				id: targetOneIDString,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(
					`
                    {
                      "id": "%s",
                      "name": "target-1",
                      "type": "prometheus",
					  "url": "www.some.url",
					  "bucket": "bucket1",
                      "bucketID": "0000000000000212",
					  "orgID": "0000000000000211",
					  "org": "org1",
                      "links": {
                        "bucket": "/api/v2/buckets/0000000000000212",
                        "organization": "/api/v2/orgs/0000000000000211",
                        "self": "/api/v2/scrapers/%s",
                        "members": "/api/v2/scrapers/%s/members",
                        "owners": "/api/v2/scrapers/%s/owners"
                      }
                    }
                    `,
					targetOneIDString, targetOneIDString, targetOneIDString, targetOneIDString,
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraperBackend := NewMockScraperBackend(t)
			scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			scraperBackend.ScraperStorageService = tt.fields.ScraperTargetStoreService
			scraperBackend.OrganizationService = tt.fields.OrganizationService
			scraperBackend.BucketService = tt.fields.BucketService
			h := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)

			r := httptest.NewRequest("GET", "http://any.tld", nil)

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

			h.handleGetScraperTarget(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetScraperTarget() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetScraperTarget() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetScraperTarget(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetScraperTarget() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleDeleteScraperTarget(t *testing.T) {
	type fields struct {
		Service influxdb.ScraperTargetStoreService
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
			name: "delete a scraper target by id",
			fields: fields{
				Service: &mock.ScraperTargetStoreService{
					RemoveTargetF: func(ctx context.Context, id platform.ID) error {
						if id == targetOneID {
							return nil
						}

						return fmt.Errorf("wrong id")
					},
				},
			},
			args: args{
				id: targetOneIDString,
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
		{
			name: "scraper target not found",
			fields: fields{
				Service: &mock.ScraperTargetStoreService{
					RemoveTargetF: func(ctx context.Context, id platform.ID) error {
						return &errors.Error{
							Code: errors.ENotFound,
							Msg:  influxdb.ErrScraperTargetNotFound,
						}
					},
				},
			},
			args: args{
				id: targetTwoIDString,
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraperBackend := NewMockScraperBackend(t)
			scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			scraperBackend.ScraperStorageService = tt.fields.Service
			h := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)

			r := httptest.NewRequest("GET", "http://any.tld", nil)

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

			h.handleDeleteScraperTarget(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteScraperTarget() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteScraperTarget() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteScraperTarget(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteScraperTarget() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostScraperTarget(t *testing.T) {
	type fields struct {
		OrganizationService       influxdb.OrganizationService
		BucketService             influxdb.BucketService
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}

	type args struct {
		target *influxdb.ScraperTarget
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
			name: "create a new scraper target",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					AddTargetF: func(ctx context.Context, st *influxdb.ScraperTarget, userID platform.ID) error {
						st.ID = targetOneID
						return nil
					},
				},
			},
			args: args{
				target: &influxdb.ScraperTarget{
					Name:     "hello",
					Type:     influxdb.PrometheusScraperType,
					BucketID: platformtesting.MustIDBase16("0000000000000212"),
					OrgID:    platformtesting.MustIDBase16("0000000000000211"),
					URL:      "www.some.url",
				},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(
					`
                    {
                      "id": "%s",
                      "name": "hello",
                      "type": "prometheus",
                      "url": "www.some.url",
					  "orgID": "0000000000000211",
					  "org": "org1",
					  "bucket": "bucket1",
                      "bucketID": "0000000000000212",
                      "links": {
                        "bucket": "/api/v2/buckets/0000000000000212",
                        "organization": "/api/v2/orgs/0000000000000211",
                        "self": "/api/v2/scrapers/%s",
                        "members": "/api/v2/scrapers/%s/members",
                        "owners": "/api/v2/scrapers/%s/owners"
                      }
                    }
                    `,
					targetOneIDString, targetOneIDString, targetOneIDString, targetOneIDString,
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraperBackend := NewMockScraperBackend(t)
			scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			scraperBackend.ScraperStorageService = tt.fields.ScraperTargetStoreService
			scraperBackend.OrganizationService = tt.fields.OrganizationService
			scraperBackend.BucketService = tt.fields.BucketService
			h := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)

			st, err := json.Marshal(tt.args.target)
			if err != nil {
				t.Fatalf("failed to unmarshal scraper target: %v", err)
			}

			r := httptest.NewRequest("GET", "http://any.tld", bytes.NewReader(st))
			r = r.WithContext(platcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{}))
			w := httptest.NewRecorder()

			h.handlePostScraperTarget(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostScraperTarget() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostScraperTarget() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostScraperTarget(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostScraperTarget() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePatchScraperTarget(t *testing.T) {
	type fields struct {
		BucketService             influxdb.BucketService
		OrganizationService       influxdb.OrganizationService
		ScraperTargetStoreService influxdb.ScraperTargetStoreService
	}

	type args struct {
		id     string
		update *influxdb.ScraperTarget
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
			name: "update a scraper target",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					UpdateTargetF: func(ctx context.Context, t *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						if t.ID == targetOneID {
							return t, nil
						}

						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  "scraper target is not found",
						}
					},
				},
			},
			args: args{
				id: targetOneIDString,
				update: &influxdb.ScraperTarget{
					ID:       targetOneID,
					Name:     "name",
					BucketID: platformtesting.MustIDBase16("0000000000000212"),
					Type:     influxdb.PrometheusScraperType,
					URL:      "www.example.url",
					OrgID:    platformtesting.MustIDBase16("0000000000000211"),
				},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: fmt.Sprintf(
					`{
		              "id":"%s",
		              "name":"name",
		              "type":"prometheus",
					  "url":"www.example.url",
					  "org": "org1",
					  "orgID":"0000000000000211",
					  "bucket": "bucket1",
					  "bucketID":"0000000000000212",
		              "links":{
		                "bucket": "/api/v2/buckets/0000000000000212",
		                "organization": "/api/v2/orgs/0000000000000211",
		                "self":"/api/v2/scrapers/%s",
		                "members":"/api/v2/scrapers/%s/members",
		                "owners":"/api/v2/scrapers/%s/owners"
		              }
		            }`,
					targetOneIDString, targetOneIDString, targetOneIDString, targetOneIDString,
				),
			},
		},
		{
			name: "scraper target not found",
			fields: fields{
				OrganizationService: &mock.OrganizationService{
					FindOrganizationByIDF: func(ctx context.Context, id platform.ID) (*influxdb.Organization, error) {
						return &influxdb.Organization{
							ID:   platformtesting.MustIDBase16("0000000000000211"),
							Name: "org1",
						}, nil
					},
				},
				BucketService: &mock.BucketService{
					FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
						return &influxdb.Bucket{
							ID:   platformtesting.MustIDBase16("0000000000000212"),
							Name: "bucket1",
						}, nil
					},
				},
				ScraperTargetStoreService: &mock.ScraperTargetStoreService{
					UpdateTargetF: func(ctx context.Context, upd *influxdb.ScraperTarget, userID platform.ID) (*influxdb.ScraperTarget, error) {
						return nil, &errors.Error{
							Code: errors.ENotFound,
							Msg:  influxdb.ErrScraperTargetNotFound,
						}
					},
				},
			},
			args: args{
				id: targetOneIDString,
				update: &influxdb.ScraperTarget{
					ID:       targetOneID,
					Name:     "name",
					BucketID: platformtesting.MustIDBase16("0000000000000212"),
					Type:     influxdb.PrometheusScraperType,
					URL:      "www.example.url",
					OrgID:    platformtesting.MustIDBase16("0000000000000211"),
				},
			},
			wants: wants{
				statusCode: http.StatusNotFound,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scraperBackend := NewMockScraperBackend(t)
			scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			scraperBackend.ScraperStorageService = tt.fields.ScraperTargetStoreService
			scraperBackend.OrganizationService = tt.fields.OrganizationService
			scraperBackend.BucketService = tt.fields.BucketService
			h := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)

			var err error
			st := make([]byte, 0)
			if tt.args.update != nil {
				st, err = json.Marshal(*tt.args.update)
				if err != nil {
					t.Fatalf("failed to unmarshal scraper target: %v", err)
				}
			}

			r := httptest.NewRequest("GET", "http://any.tld", bytes.NewReader(st))

			r = r.WithContext(context.WithValue(
				context.Background(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "id",
						Value: tt.args.id,
					},
				}))
			r = r.WithContext(platcontext.SetAuthorizer(r.Context(), &influxdb.Authorization{}))
			w := httptest.NewRecorder()

			h.handlePatchScraperTarget(w, r)

			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePatchScraperTarget() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePatchScraperTarget() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePatchScraperTarget(). error unmarshalling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePatchScraperTarget() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func initScraperService(f platformtesting.TargetFields, t *testing.T) (influxdb.ScraperTargetStoreService, string, func()) {
	t.Helper()

	store := platformtesting.NewTestInmemStore(t)
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)

	svc := kv.NewService(zaptest.NewLogger(t), store, tenantService)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	for _, target := range f.Targets {
		if err := svc.PutTarget(ctx, target); err != nil {
			t.Fatalf("failed to populate scraper targets")
		}
	}

	for _, o := range f.Organizations {
		mock.SetIDForFunc(&tenantStore.OrgIDGen, o.ID, func() {
			if err := tenantService.CreateOrganization(ctx, o); err != nil {
				t.Fatalf("failed to populate orgs")
			}
		})
	}

	scraperBackend := NewMockScraperBackend(t)
	scraperBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
	scraperBackend.ScraperStorageService = svc
	scraperBackend.OrganizationService = tenantService
	scraperBackend.BucketService = &mock.BucketService{
		FindBucketByIDFn: func(ctx context.Context, id platform.ID) (*influxdb.Bucket, error) {
			return &influxdb.Bucket{
				ID:   id,
				Name: "bucket1",
			}, nil
		},
	}

	handler := NewScraperHandler(zaptest.NewLogger(t), scraperBackend)
	server := httptest.NewServer(httpMock.NewAuthMiddlewareHandler(
		handler,
		&influxdb.Authorization{
			UserID: platformtesting.MustIDBase16("020f755c3c082002"),
			Token:  "tok",
		},
	))
	client := struct {
		influxdb.UserResourceMappingService
		influxdb.OrganizationService
		ScraperService
	}{
		UserResourceMappingService: tenantService,
		OrganizationService:        tenantService,
		ScraperService: ScraperService{
			Token: "tok",
			Addr:  server.URL,
		},
	}
	done := server.Close

	return &client, "", done
}

func TestScraperService(t *testing.T) {
	platformtesting.ScraperService(initScraperService, t)
}
