package http

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/mock"
	influxtesting "github.com/influxdata/influxdb/testing"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NewMockDocumentBackend returns a DocumentBackend with mock services.
func NewMockDocumentBackend() *DocumentBackend {
	return &DocumentBackend{
		Logger: zap.NewNop().With(zap.String("handler", "document")),

		DocumentService: mock.NewDocumentService(),
	}
}

func TestService_handleGetDocuments(t *testing.T) {
	type fields struct {
		DocumentService influxdb.DocumentService
	}
	type args struct {
		queryParams map[string][]string
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
			name: "get all documents without org or orgID",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{}, nil
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: influxtesting.MustIDBase16("020f755c3c082001")},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"Please provide either org or orgID"}`,
			},
		},
		{
			name: "get all documents with both org and orgID",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{}, nil
					},
				},
			},
			args: args{
				queryParams: map[string][]string{
					"orgID": []string{"020f755c3c082002"},
					"org":   []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: influxtesting.MustIDBase16("020f755c3c082001")},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"Please provide either org or orgID, not both"}`,
			},
		},
		{
			name: "get all documents with orgID",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							FindDocumentsFn: func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
								return []*influxdb.Document{
									{
										ID: influxtesting.MustIDBase16("020f755c3c082010"),
										Meta: influxdb.DocumentMeta{
											Name: "doc1",
										},
										Content: "content1",
										Labels: []*influxdb.Label{
											{
												Name: "l1",
											},
										},
									},
									{
										ID: influxtesting.MustIDBase16("020f755c3c082011"),
										Meta: influxdb.DocumentMeta{
											Name: "doc2",
										},
										Content: "content2",
									},
								}, nil
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string][]string{
					"orgID": []string{"020f755c3c082002"},
				},
				authorizer: &influxdb.Session{UserID: influxtesting.MustIDBase16("020f755c3c082001")},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"documents":[
						{
							"id": "020f755c3c082010",
							"links": {
								"self": "/api/v2/documents/template/020f755c3c082010"
							},
							"content": "content1",
							"labels": [
                   				{
                      				"name": "l1"
                    			}
                  			],
							"meta": {
								"name": "doc1"
							}
						},
						{
							"id": "020f755c3c082011",
							"links": {
								"self": "/api/v2/documents/template/020f755c3c082011"
							},
							"content": "content2", 
							"meta": {
								"name": "doc2"
							}
						}
					]
				}`,
			},
		},
		{
			name: "get all documents with org name",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							FindDocumentsFn: func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
								return []*influxdb.Document{
									{
										ID: influxtesting.MustIDBase16("020f755c3c082010"),
										Meta: influxdb.DocumentMeta{
											Name: "doc1",
										},
										Content: "content1",
										Labels: []*influxdb.Label{
											{
												Name: "l1",
											},
										},
									},
									{
										ID: influxtesting.MustIDBase16("020f755c3c082011"),
										Meta: influxdb.DocumentMeta{
											Name: "doc2",
										},
										Content: "content2",
									},
								}, nil
							},
						}, nil
					},
				},
			},
			args: args{
				queryParams: map[string][]string{
					"org": []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: influxtesting.MustIDBase16("020f755c3c082001")},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{
					"documents":[
						{
							"id": "020f755c3c082010",
							"links": {
								"self": "/api/v2/documents/template/020f755c3c082010"
							},
							"content": "content1",
							"labels": [
                   				{
                      				"name": "l1"
                    			}
                  			],
							"meta": {
								"name": "doc1"
							}
						},
						{
							"id": "020f755c3c082011",
							"links": {
								"self": "/api/v2/documents/template/020f755c3c082011"
							},
							"content": "content2", 
							"meta": {
								"name": "doc2"
							}
						}
					]
				}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend()
			documentBackend.DocumentService = tt.fields.DocumentService
			h := NewDocumentHandler(documentBackend)
			r := httptest.NewRequest("GET", "http://any.url", nil)
			qp := r.URL.Query()
			for k, vs := range tt.args.queryParams {
				for _, v := range vs {
					qp.Add(k, v)
				}
			}
			r.URL.RawQuery = qp.Encode()
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), tt.args.authorizer))
			r = r.WithContext(context.WithValue(r.Context(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "ns",
						Value: "template",
					}}))
			w := httptest.NewRecorder()
			h.handleGetDocuments(w, r)
			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetDocuments() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetDocuments() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if eq, diff, _ := jsonEqual(string(body), tt.wants.body); tt.wants.body != "" && !eq {
				t.Errorf("%q. handleGetDocuments() = ***%s***", tt.name, diff)
			}
		})
	}
}
