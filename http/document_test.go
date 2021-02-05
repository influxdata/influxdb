package http

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	influxtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

var (
	doc1ID = influxtesting.MustIDBase16("020f755c3c082010")
	doc2ID = influxtesting.MustIDBase16("020f755c3c082011")
	doc1   = influxdb.Document{
		ID: doc1ID,
		Meta: influxdb.DocumentMeta{
			Name:        "doc1",
			Type:        "typ1",
			Description: "desc1",
		},
		Content: "content1",
	}
	doc2 = influxdb.Document{
		ID: doc2ID,
		Meta: influxdb.DocumentMeta{
			Name: "doc2",
		},
		Content: "content2",
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
				FindDocumentsFn: func(ctx context.Context, _ influxdb.ID) ([]*influxdb.Document, error) {
					return docs, nil
				},
			}, nil
		},
	}
)

// NewMockDocumentBackend returns a DocumentBackend with mock services.
func NewMockDocumentBackend(t *testing.T) *DocumentBackend {
	return &DocumentBackend{
		log: zaptest.NewLogger(t),

		DocumentService: mock.NewDocumentService(),
	}
}

func TestService_handleGetDocuments(t *testing.T) {
	type fields struct {
		DocumentService influxdb.DocumentService
	}
	type args struct {
		authorizer influxdb.Authorizer
		orgID      influxdb.ID
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
			name: "get all documents",
			fields: fields{
				DocumentService: findDocsServiceMock,
			},
			args: args{
				authorizer: mock.NewMockAuthorizer(true, nil),
				orgID:      influxdb.ID(2),
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
			h := NewDocumentHandler(documentBackend)

			r := httptest.NewRequest("GET", "http://any.url", nil)
			qp := r.URL.Query()
			qp.Add("orgID", tt.args.orgID.String())
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
