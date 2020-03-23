package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	kithttp "github.com/influxdata/influxdb/kit/transport/http"
	"github.com/influxdata/influxdb/mock"
	influxtesting "github.com/influxdata/influxdb/testing"
	"go.uber.org/zap/zaptest"
)

var (
	doc1ID   = influxtesting.MustIDBase16("020f755c3c082010")
	doc2ID   = influxtesting.MustIDBase16("020f755c3c082011")
	doc3ID   = influxtesting.MustIDBase16("020f755c3c082012")
	doc4ID   = influxtesting.MustIDBase16("020f755c3c082013")
	doc5ID   = influxtesting.MustIDBase16("020f755c3c082014")
	doc6ID   = influxtesting.MustIDBase16("020f755c3c082015")
	user1ID  = influxtesting.MustIDBase16("020f755c3c082001")
	label1ID = influxtesting.MustIDBase16("020f755c3c082300")
	label2ID = influxtesting.MustIDBase16("020f755c3c082301")
	label3ID = influxtesting.MustIDBase16("020f755c3c082302")
	label1   = influxdb.Label{
		ID:   label1ID,
		Name: "l1",
	}
	label2 = influxdb.Label{
		ID:   label2ID,
		Name: "l2",
	}
	label3 = influxdb.Label{
		ID:   label3ID,
		Name: "l3",
	}
	label3MappingJSON, _ = json.Marshal(influxdb.LabelMapping{
		LabelID: label3ID,
	})
	mockGen = mock.TimeGenerator{
		FakeValue: time.Date(2006, 5, 24, 1, 2, 3, 4, time.UTC),
	}
	doc1 = influxdb.Document{
		ID: doc1ID,
		Meta: influxdb.DocumentMeta{
			Name:        "doc1",
			Type:        "typ1",
			Description: "desc1",
		},
		Content: "content1",
		Labels: []*influxdb.Label{
			&label1,
		},
	}
	doc2 = influxdb.Document{
		ID: doc2ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc2",
		},
		Content: "content2",
		Labels:  []*influxdb.Label{},
	}
	doc3 = influxdb.Document{
		ID: doc3ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc3",
		},
		Content: "content3",
		Labels: []*influxdb.Label{
			&label2,
		},
	}
	doc4 = influxdb.Document{
		ID: doc4ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc4",
			Type: "typ4",
		},
		Content: "content4",
	}
	doc5 = influxdb.Document{
		ID: doc5ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc5",
		},
		Content: "content5",
	}
	doc6 = influxdb.Document{
		ID: doc6ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc6",
		},
		Content: "content6",
	}

	docs = []*influxdb.Document{
		&doc1,
		&doc2,
	}
	docsResp = `{
		"documents":[
			{
				"id": "020f755c3c082010",
				"links": {
					"self": "/api/v2/documents/template/020f755c3c082010"
				},
				"content": "content1",
				"labels": [
					   {
							"id": "020f755c3c082300",
							"name": "l1"
						}
				  ],
				"meta": {
					"name": "doc1",
					"type": "typ1",
					"createdAt": "0001-01-01T00:00:00Z",
					"updatedAt": "0001-01-01T00:00:00Z",
					"description": "desc1"
				}
			},
			{
				"id": "020f755c3c082011",
				"links": {
					"self": "/api/v2/documents/template/020f755c3c082011"
				},
				"content": "content2",
				"meta": {
					"name": "doc2",
					"createdAt": "0001-01-01T00:00:00Z",
					"updatedAt": "0001-01-01T00:00:00Z"	
				}
			}
		]
	}`
	findDocsServiceMock = &mock.DocumentService{
		FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
			return &mock.DocumentStore{
				FindDocumentsFn: func(ctx context.Context, opts ...influxdb.DocumentFindOptions) ([]*influxdb.Document, error) {
					return docs, nil
				},
			}, nil
		},
	}
	findDoc1ServiceMock = &mock.DocumentService{
		FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
			return &mock.DocumentStore{
				FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
					return &doc1, nil
				},
			}, nil
		},
	}
	findDoc2ServiceMock = &mock.DocumentService{
		FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
			return &mock.DocumentStore{
				FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
					return &doc2, nil
				},
			}, nil
		},
	}
	getOrgIDServiceMock = &mock.OrganizationService{
		FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
			return &influxdb.Organization{
				ID: 1,
			}, nil
		},
	}
)

// NewMockDocumentBackend returns a DocumentBackend with mock services.
func NewMockDocumentBackend(t *testing.T) *DocumentBackend {
	return &DocumentBackend{
		log: zaptest.NewLogger(t),

		DocumentService: mock.NewDocumentService(),
		LabelService:    mock.NewLabelService(),
	}
}

func TestService_handleDeleteDocumentLabel(t *testing.T) {
	type fields struct {
		DocumentService influxdb.DocumentService
		LabelService    influxdb.LabelService
	}
	type args struct {
		authorizer influxdb.Authorizer
		documentID influxdb.ID
		labelID    influxdb.ID
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
			name: "bad doc id",
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"url missing resource id"}`,
			},
		},
		{
			name: "bad label id",
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc1ID,
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"label id is missing"}`,
			},
		},
		{
			name: "label not found",
			fields: fields{
				DocumentService: findDoc2ServiceMock,
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(context.Context, influxdb.ID) (*influxdb.Label, error) {
						return nil, &influxdb.Error{
							Code: influxdb.ENotFound,
							Msg:  "label not found",
						}
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc2ID,
				labelID:    label1ID,
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"not found", "message":"label not found"}`,
			},
		},
		{
			name: "regular get labels",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
								return &doc3, nil
							},
							UpdateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
								return nil
							},
						}, nil
					},
				},
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(context.Context, influxdb.ID) (*influxdb.Label, error) {
						return &label2, nil
					},
					DeleteLabelMappingFn: func(context.Context, *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc3ID,
				labelID:    label2ID,
			},
			wants: wants{
				statusCode: http.StatusNoContent,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend(t)
			documentBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			documentBackend.DocumentService = tt.fields.DocumentService
			documentBackend.LabelService = tt.fields.LabelService
			h := NewDocumentHandler(documentBackend)
			r := httptest.NewRequest("DELETE", "http://any.url", nil)
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), tt.args.authorizer))
			r = r.WithContext(context.WithValue(r.Context(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "ns",
						Value: "template",
					},
					{
						Key:   "id",
						Value: tt.args.documentID.String(),
					},
					{
						Key:   "lid",
						Value: tt.args.labelID.String(),
					},
				}))
			w := httptest.NewRecorder()
			h.handleDeleteDocumentLabel(w, r)
			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleDeleteDocumentLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleDeleteDocumentLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleDeleteDocumentLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleDeleteDocumentLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostDocumentLabel(t *testing.T) {
	type fields struct {
		DocumentService influxdb.DocumentService
		LabelService    influxdb.LabelService
	}
	type args struct {
		body       *bytes.Buffer
		authorizer influxdb.Authorizer
		documentID influxdb.ID
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
			name: "bad doc id",
			args: args{
				body: new(bytes.Buffer),
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"url missing id"}`,
			},
		},
		{
			name: "doc not found",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
								return nil, &influxdb.Error{
									Code: influxdb.ENotFound,
									Msg:  "doc not found",
								}
							},
						}, nil
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc2ID,
				body:       new(bytes.Buffer),
			},
			wants: wants{
				statusCode:  http.StatusNotFound,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"not found", "message":"doc not found"}`,
			},
		},
		{
			name: "empty post a label",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
								return &doc3, nil
							},
							UpdateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
								return nil
							},
						}, nil
					},
				},
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(context.Context, influxdb.ID) (*influxdb.Label, error) {
						return &label2, nil
					},
					DeleteLabelMappingFn: func(context.Context, *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc3ID,
				body:       new(bytes.Buffer),
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"Invalid post label map request"}`,
			},
		},
		{
			name: "regular post a label",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							TimeGenerator: mockGen,
							FindDocumentFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Document, error) {
								return &doc4, nil
							},
							UpdateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
								return nil
							},
						}, nil
					},
				},
				LabelService: &mock.LabelService{
					CreateLabelMappingFn: func(context.Context, *influxdb.LabelMapping) error {
						return nil
					},
					FindLabelByIDFn: func(context.Context, influxdb.ID) (*influxdb.Label, error) {
						return &label3, nil
					},
					DeleteLabelMappingFn: func(context.Context, *influxdb.LabelMapping) error {
						return nil
					},
				},
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc3ID,
				body:       bytes.NewBuffer(label3MappingJSON),
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `{"label": {
					    "id": "020f755c3c082302",
					    "name": "l3"
					  },
					  "links": {"self": "/api/v2/labels/020f755c3c082302"}}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend(t)
			documentBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			documentBackend.DocumentService = tt.fields.DocumentService
			documentBackend.LabelService = tt.fields.LabelService
			h := NewDocumentHandler(documentBackend)
			r := httptest.NewRequest("POST", "http://any.url", tt.args.body)
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), tt.args.authorizer))
			r = r.WithContext(context.WithValue(r.Context(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "ns",
						Value: "template",
					},
					{
						Key:   "id",
						Value: tt.args.documentID.String(),
					},
				}))
			w := httptest.NewRecorder()
			h.handlePostDocumentLabel(w, r)
			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostDocumentLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostDocumentLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handlePostDocumentLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostDocumentLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleGetDocumentLabels(t *testing.T) {
	type fields struct {
		DocumentService influxdb.DocumentService
		LabelService    influxdb.LabelService
	}
	type args struct {
		queryParams map[string][]string
		authorizer  influxdb.Authorizer
		documentID  influxdb.ID
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
			name: "invalid document id",
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid", "message":"url missing id"}`,
			},
		},
		{
			name: "regular get labels",
			fields: fields{
				DocumentService: findDoc1ServiceMock,
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc1ID,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body: `{"labels": [{
			"id": "020f755c3c082300",
			"name": "l1"
		}],"links":{"self":"/api/v2/labels"}}`},
		},
		{
			name: "find no labels",
			fields: fields{
				DocumentService: findDoc2ServiceMock,
			},
			args: args{
				authorizer: &influxdb.Session{UserID: user1ID},
				documentID: doc1ID,
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        `{"labels": [],"links":{"self":"/api/v2/labels"}}`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend(t)
			documentBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			documentBackend.DocumentService = tt.fields.DocumentService
			documentBackend.LabelService = tt.fields.LabelService
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
					},
					{
						Key:   "id",
						Value: tt.args.documentID.String(),
					},
				}))
			w := httptest.NewRecorder()
			h.handleGetDocumentLabel(w, r)
			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handleGetDocumentLabel() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handleGetDocumentLabel() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetDocumentLabel(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetDocumentLabel() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handleGetDocuments(t *testing.T) {
	type fields struct {
		DocumentService     influxdb.DocumentService
		OrganizationService influxdb.OrganizationService
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
			name: "get all documents with both org and orgID",
			fields: fields{
				DocumentService: findDocsServiceMock,
			},
			args: args{
				queryParams: map[string][]string{
					"orgID": []string{"020f755c3c082002"},
					"org":   []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
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
				DocumentService: findDocsServiceMock,
			},
			args: args{
				queryParams: map[string][]string{
					"orgID": []string{"020f755c3c082002"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        docsResp,
			},
		},
		{
			name: "get all documents with org name",
			fields: fields{
				DocumentService:     findDocsServiceMock,
				OrganizationService: getOrgIDServiceMock,
			},
			args: args{
				queryParams: map[string][]string{
					"org": []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusOK,
				contentType: "application/json; charset=utf-8",
				body:        docsResp,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend(t)
			documentBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			documentBackend.DocumentService = tt.fields.DocumentService
			documentBackend.OrganizationService = tt.fields.OrganizationService
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
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(string(body), tt.wants.body); err != nil {
					t.Errorf("%q, handleGetDocuments(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handleGetDocuments() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}

func TestService_handlePostDocuments(t *testing.T) {
	type fields struct {
		DocumentService     influxdb.DocumentService
		LabelService        influxdb.LabelService
		OrganizationService influxdb.OrganizationService
	}
	type args struct {
		body        *bytes.Buffer
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
			name: "blank body",
			fields: fields{
				DocumentService: &mock.DocumentService{},
				LabelService:    &mock.LabelService{},
			},
			args: args{
				body: bytes.NewBuffer([]byte{}),
				queryParams: map[string][]string{
					"org": []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message": "document body error: EOF"}`,
			},
		},
		{
			name: "empty json",
			fields: fields{
				DocumentService: &mock.DocumentService{},
				LabelService:    &mock.LabelService{},
			},
			args: args{
				body: bytes.NewBuffer([]byte(`{}`)),
				queryParams: map[string][]string{
					"org": []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusBadRequest,
				contentType: "application/json; charset=utf-8",
				body:        `{"code":"invalid","message": "missing document body"}`,
			},
		},
		{
			name: "without label",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							TimeGenerator: mockGen,
							CreateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
								d.Meta.CreatedAt = mockGen.Now()
								return nil
							},
						}, nil
					},
				},
				LabelService:        &mock.LabelService{},
				OrganizationService: getOrgIDServiceMock,
			},
			args: args{
				body: func() *bytes.Buffer {
					req := postDocumentRequest{
						Document: &doc5,
						Org:      "org1",
					}
					m, _ := json.Marshal(req)
					return bytes.NewBuffer(m)
				}(),
				queryParams: map[string][]string{
					"org": []string{"org1"},
				},
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `{
					"content": "content5",
					"id": "020f755c3c082014",
					"links": {
						"self": "/api/v2/documents/template/020f755c3c082014"
					},
					"meta": {
						"name": "doc5",
						"createdAt": "2006-05-24T01:02:03.000000004Z",
						"updatedAt": "0001-01-01T00:00:00Z"
					}}`,
			},
		},
		{
			name: "with label",
			fields: fields{
				DocumentService: &mock.DocumentService{
					FindDocumentStoreFn: func(context.Context, string) (influxdb.DocumentStore, error) {
						return &mock.DocumentStore{
							TimeGenerator: mockGen,
							CreateDocumentFn: func(ctx context.Context, d *influxdb.Document) error {
								d.Labels = []*influxdb.Label{&label1, &label2}
								d.Meta.CreatedAt = mockGen.Now()
								return nil
							},
						}, nil
					},
				},
				LabelService: &mock.LabelService{
					FindLabelByIDFn: func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
						if id == label1ID {
							return &label1, nil
						}
						return &label2, nil
					},
				},
				OrganizationService: getOrgIDServiceMock,
			},
			args: args{
				body: func() *bytes.Buffer {
					req := postDocumentRequest{
						Document: &doc6,
						Org:      "org1",
					}
					m, _ := json.Marshal(req)
					return bytes.NewBuffer(m)
				}(),
				authorizer: &influxdb.Session{UserID: user1ID},
			},
			wants: wants{
				statusCode:  http.StatusCreated,
				contentType: "application/json; charset=utf-8",
				body: `{
					"content": "content6",
					"id": "020f755c3c082015",
					"links": {
						"self": "/api/v2/documents/template/020f755c3c082015"
					},
					"labels": [{
            			"id": "020f755c3c082300",
            			"name": "l1"
					},
					{
            			"id": "020f755c3c082301",
            			"name": "l2"
            		}],
					"meta": {
						"name": "doc6",
						"createdAt": "2006-05-24T01:02:03.000000004Z",
						"updatedAt": "0001-01-01T00:00:00Z"
					}}`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			documentBackend := NewMockDocumentBackend(t)
			documentBackend.HTTPErrorHandler = kithttp.ErrorHandler(0)
			documentBackend.DocumentService = tt.fields.DocumentService
			documentBackend.LabelService = tt.fields.LabelService
			documentBackend.OrganizationService = tt.fields.OrganizationService
			h := NewDocumentHandler(documentBackend)
			r := httptest.NewRequest("POST", "http://any.url", tt.args.body)
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), tt.args.authorizer))
			r = r.WithContext(context.WithValue(r.Context(),
				httprouter.ParamsKey,
				httprouter.Params{
					{
						Key:   "ns",
						Value: "template",
					},
				}))
			w := httptest.NewRecorder()
			h.handlePostDocument(w, r)
			res := w.Result()
			content := res.Header.Get("Content-Type")
			body, _ := ioutil.ReadAll(res.Body)

			if res.StatusCode != tt.wants.statusCode {
				t.Errorf("%q. handlePostDocument() = %v, want %v", tt.name, res.StatusCode, tt.wants.statusCode)
			}
			if tt.wants.contentType != "" && content != tt.wants.contentType {
				t.Errorf("%q. handlePostDocument() = %v, want %v", tt.name, content, tt.wants.contentType)
			}
			if tt.wants.body != "" {
				if eq, diff, err := jsonEqual(tt.wants.body, string(body)); err != nil {
					t.Errorf("%q, handlePostDocument(). error unmarshaling json %v", tt.name, err)
				} else if !eq {
					t.Errorf("%q. handlePostDocument() = ***%s***", tt.name, diff)
				}
			}
		})
	}
}
