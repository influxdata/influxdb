package http

import (
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

const prefixDocuments = "/api/v2/documents"

// DocumentService is an interface HTTP-exposed portion of the document service.
type DocumentService interface {
	GetDocuments(ctx context.Context, namespace string, orgID influxdb.ID) ([]*influxdb.Document, error)
}

// DocumentBackend is all services and associated parameters required to construct
// the DocumentHandler.
type DocumentBackend struct {
	log *zap.Logger
	influxdb.HTTPErrorHandler

	DocumentService influxdb.DocumentService
}

// NewDocumentBackend returns a new instance of DocumentBackend.
func NewDocumentBackend(log *zap.Logger, b *APIBackend) *DocumentBackend {
	return &DocumentBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,
		DocumentService:  b.DocumentService,
	}
}

// DocumentHandler represents an HTTP API handler for documents.
type DocumentHandler struct {
	*httprouter.Router

	log *zap.Logger
	influxdb.HTTPErrorHandler

	DocumentService influxdb.DocumentService
	LabelService    influxdb.LabelService
}

const (
	documentsPath = "/api/v2/documents/:ns"
)

// NewDocumentHandler returns a new instance of DocumentHandler.
// TODO(desa): this should probably take a namespace
func NewDocumentHandler(b *DocumentBackend) *DocumentHandler {
	h := &DocumentHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log,

		DocumentService: b.DocumentService,
	}

	h.HandlerFunc("GET", documentsPath, h.handleGetDocuments)

	return h
}

type documentResponse struct {
	Links map[string]string `json:"links"`
	*influxdb.Document
}

func newDocumentResponse(ns string, d *influxdb.Document) *documentResponse {
	if d.Labels == nil {
		d.Labels = []*influxdb.Label{}
	}
	return &documentResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/documents/%s/%s", ns, d.ID),
		},
		Document: d,
	}
}

type documentsResponse struct {
	Documents []*documentResponse `json:"documents"`
}

func newDocumentsResponse(ns string, docs []*influxdb.Document) *documentsResponse {
	ds := make([]*documentResponse, 0, len(docs))
	for _, doc := range docs {
		ds = append(ds, newDocumentResponse(ns, doc))
	}

	return &documentsResponse{
		Documents: ds,
	}
}

// handleGetDocuments is the HTTP handler for the GET /api/v2/documents/:ns route.
func (h *DocumentHandler) handleGetDocuments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetDocumentsRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	s, err := h.DocumentService.FindDocumentStore(ctx, req.Namespace)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	ds, err := s.FindDocuments(ctx, req.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Documents retrieved", zap.String("documents", fmt.Sprint(ds)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDocumentsResponse(req.Namespace, ds)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getDocumentsRequest struct {
	Namespace string
	Org       string
	OrgID     influxdb.ID
}

func decodeGetDocumentsRequest(ctx context.Context, r *http.Request) (*getDocumentsRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	ns := params.ByName("ns")
	if ns == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing namespace",
		}
	}

	qp := r.URL.Query()
	req := &getDocumentsRequest{
		Namespace: ns,
		Org:       qp.Get("org"),
	}

	if oidStr := qp.Get("orgID"); oidStr != "" {
		oid, err := influxdb.IDFromString(oidStr)
		if err != nil {
			return nil, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "Invalid orgID",
			}
		}
		req.OrgID = *oid
	}
	return req, nil
}

type documentService struct {
	Client *httpc.Client
}

// NewDocumentService creates a client to connect to Influx via HTTP to manage documents.
func NewDocumentService(client *httpc.Client) DocumentService {
	return &documentService{
		Client: client,
	}
}

func buildDocumentsPath(namespace string) string {
	return path.Join(prefixDocuments, namespace)
}

// GetDocuments returns the documents for a `namespace` and an `orgID`.
// Returned documents do not  contain their content.
func (s *documentService) GetDocuments(ctx context.Context, namespace string, orgID influxdb.ID) ([]*influxdb.Document, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var resp documentsResponse
	r := s.Client.
		Get(buildDocumentsPath(namespace)).
		DecodeJSON(&resp)
	r = r.QueryParams([2]string{"orgID", orgID.String()})
	if err := r.Do(ctx); err != nil {
		return nil, err
	}
	docs := make([]*influxdb.Document, len(resp.Documents))
	for i := 0; i < len(docs); i++ {
		docs[i] = resp.Documents[i].Document
	}
	return docs, nil
}
