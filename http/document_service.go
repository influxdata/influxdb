package http

import (
	"context"
	"encoding/json"
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
	CreateDocument(ctx context.Context, namespace string, orgID influxdb.ID, d *influxdb.Document) error
	GetDocuments(ctx context.Context, namespace string, orgID influxdb.ID) ([]*influxdb.Document, error)
	GetDocument(ctx context.Context, namespace string, id influxdb.ID) (*influxdb.Document, error)
	UpdateDocument(ctx context.Context, namespace string, d *influxdb.Document) error
	DeleteDocument(ctx context.Context, namespace string, id influxdb.ID) error

	GetDocumentLabels(ctx context.Context, namespace string, id influxdb.ID) ([]*influxdb.Label, error)
	AddDocumentLabel(ctx context.Context, namespace string, did influxdb.ID, lid influxdb.ID) (*influxdb.Label, error)
	DeleteDocumentLabel(ctx context.Context, namespace string, did influxdb.ID, lid influxdb.ID) error
}

// DocumentBackend is all services and associated parameters required to construct
// the DocumentHandler.
type DocumentBackend struct {
	log *zap.Logger
	influxdb.HTTPErrorHandler

	DocumentService     influxdb.DocumentService
	LabelService        influxdb.LabelService
	OrganizationService influxdb.OrganizationService
}

// NewDocumentBackend returns a new instance of DocumentBackend.
func NewDocumentBackend(log *zap.Logger, b *APIBackend) *DocumentBackend {
	return &DocumentBackend{
		HTTPErrorHandler:    b.HTTPErrorHandler,
		log:                 log,
		DocumentService:     b.DocumentService,
		LabelService:        b.LabelService,
		OrganizationService: b.OrganizationService,
	}
}

// DocumentHandler represents an HTTP API handler for documents.
type DocumentHandler struct {
	*httprouter.Router

	log *zap.Logger
	influxdb.HTTPErrorHandler

	DocumentService     influxdb.DocumentService
	LabelService        influxdb.LabelService
	OrganizationService influxdb.OrganizationService
}

const (
	documentsPath        = "/api/v2/documents/:ns"
	documentPath         = "/api/v2/documents/:ns/:id"
	documentLabelsPath   = "/api/v2/documents/:ns/:id/labels"
	documentLabelsIDPath = "/api/v2/documents/:ns/:id/labels/:lid"
)

// NewDocumentHandler returns a new instance of DocumentHandler.
// TODO(desa): this should probably take a namespace
func NewDocumentHandler(b *DocumentBackend) *DocumentHandler {
	h := &DocumentHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log,

		DocumentService:     b.DocumentService,
		LabelService:        b.LabelService,
		OrganizationService: b.OrganizationService,
	}

	h.HandlerFunc("POST", documentsPath, h.handlePostDocument)
	h.HandlerFunc("GET", documentsPath, h.handleGetDocuments)
	h.HandlerFunc("GET", documentPath, h.handleGetDocument)
	h.HandlerFunc("PUT", documentPath, h.handlePutDocument)
	h.HandlerFunc("DELETE", documentPath, h.handleDeleteDocument)

	h.HandlerFunc("GET", documentLabelsPath, h.handleGetDocumentLabel)
	h.HandlerFunc("POST", documentLabelsPath, h.handlePostDocumentLabel)
	h.HandlerFunc("DELETE", documentLabelsIDPath, h.handleDeleteDocumentLabel)

	return h
}

func (h *DocumentHandler) getOrgID(ctx context.Context, org string, orgID influxdb.ID) (influxdb.ID, error) {
	if org != "" && orgID.Valid() {
		return 0, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "Please provide either org or orgID, not both",
		}
	}
	if orgID.Valid() {
		return orgID, nil
	}
	if org != "" {
		o, err := h.OrganizationService.FindOrganization(ctx, influxdb.OrganizationFilter{Name: &org})
		if err != nil {
			return 0, err
		}
		return o.ID, nil
	}
	return 0, &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "Please provide either org or orgID",
	}
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

// handlePostDocument is the HTTP handler for the POST /api/v2/documents/:ns route.
func (h *DocumentHandler) handlePostDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostDocumentRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	s, err := h.DocumentService.FindDocumentStore(ctx, req.Namespace)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	orgID, err := h.getOrgID(ctx, req.Org, req.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	req.Document.Organizations = make(map[influxdb.ID]influxdb.UserType)
	req.Document.Organizations[orgID] = influxdb.Owner

	d := req.Document
	for _, lid := range req.Labels {
		d.Labels = append(d.Labels, &influxdb.Label{ID: lid})
	}
	if err := s.CreateDocument(ctx, d); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Document created", zap.String("document", fmt.Sprint(req.Document)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newDocumentResponse(req.Namespace, req.Document)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postDocumentRequest struct {
	*influxdb.Document
	Namespace string      `json:"-"`
	Org       string      `json:"org"`
	OrgID     influxdb.ID `json:"orgID,omitempty"`
	// TODO(affo): Why not accepting fully qualified labels?
	//  this forces us to convert them back and forth.
	Labels []influxdb.ID `json:"labels"`
}

func decodePostDocumentRequest(ctx context.Context, r *http.Request) (*postDocumentRequest, error) {
	req := &postDocumentRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "document body error",
			Err:  err,
		}
	}

	if req.Document == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "missing document body",
		}
	}

	params := httprouter.ParamsFromContext(ctx)
	req.Namespace = params.ByName("ns")
	if req.Namespace == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing namespace",
		}
	}

	return req, nil
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

	orgID, err := h.getOrgID(ctx, req.Org, req.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	opts := []influxdb.DocumentFindOptions{influxdb.IncludeLabels, influxdb.WhereOrgID(orgID)}
	ds, err := s.FindDocuments(ctx, opts...)
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

func (h *DocumentHandler) handlePostDocumentLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_, _, err := h.getDocument(w, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	req, err := decodePostLabelMappingRequest(ctx, r, influxdb.DocumentsResourceType)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := req.Mapping.Validate(); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.LabelService.CreateLabelMapping(ctx, &req.Mapping); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	label, err := h.LabelService.FindLabelByID(ctx, req.Mapping.LabelID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Document label created", zap.String("label", fmt.Sprint(label)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newLabelResponse(label)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handleDeleteDocumentLabel will first remove the label from the document,
// then remove that label.
func (h *DocumentHandler) handleDeleteDocumentLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteLabelMappingRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	_, _, err = h.getDocument(w, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	_, err = h.LabelService.FindLabelByID(ctx, req.LabelID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	mapping := &influxdb.LabelMapping{
		LabelID:      req.LabelID,
		ResourceID:   req.ResourceID,
		ResourceType: influxdb.DocumentsResourceType,
	}

	// remove the label
	if err := h.LabelService.DeleteLabelMapping(ctx, mapping); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Document label deleted", zap.String("mapping", fmt.Sprint(mapping)))

	w.WriteHeader(http.StatusNoContent)
}

func (h *DocumentHandler) handleGetDocumentLabel(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	d, _, err := h.getDocument(w, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Document label retrieved", zap.String("labels", fmt.Sprint(d.Labels)))

	if err := encodeResponse(ctx, w, http.StatusOK, newLabelsResponse(d.Labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *DocumentHandler) getDocument(w http.ResponseWriter, r *http.Request) (*influxdb.Document, string, error) {
	ctx := r.Context()

	req, err := decodeGetDocumentRequest(ctx, r)
	if err != nil {
		return nil, "", err
	}
	s, err := h.DocumentService.FindDocumentStore(ctx, req.Namespace)
	if err != nil {
		return nil, "", err
	}
	ds, err := s.FindDocument(ctx, req.ID)
	if err != nil {
		return nil, "", err
	}
	return ds, req.Namespace, nil
}

// handleGetDocument is the HTTP handler for the GET /api/v2/documents/:ns/:id route.
func (h *DocumentHandler) handleGetDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	d, namespace, err := h.getDocument(w, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Document retrieved", zap.String("document", fmt.Sprint(d)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDocumentResponse(namespace, d)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getDocumentRequest struct {
	Namespace string
	ID        influxdb.ID
}

func decodeGetDocumentRequest(ctx context.Context, r *http.Request) (*getDocumentRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	ns := params.ByName("ns")
	if ns == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing namespace",
		}
	}

	i := params.ByName("id")
	if i == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var id influxdb.ID
	if err := id.DecodeFromString(i); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad id in url",
		}
	}

	return &getDocumentRequest{
		Namespace: ns,
		ID:        id,
	}, nil
}

// handleDeleteDocument is the HTTP handler for the DELETE /api/v2/documents/:ns/:id route.
func (h *DocumentHandler) handleDeleteDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteDocumentRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	s, err := h.DocumentService.FindDocumentStore(ctx, req.Namespace)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := s.DeleteDocument(ctx, req.ID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Document deleted", zap.String("documentID", fmt.Sprint(req.ID)))

	w.WriteHeader(http.StatusNoContent)
}

type deleteDocumentRequest struct {
	Namespace string
	ID        influxdb.ID
}

func decodeDeleteDocumentRequest(ctx context.Context, r *http.Request) (*deleteDocumentRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	ns := params.ByName("ns")
	if ns == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing namespace",
		}
	}

	i := params.ByName("id")
	if i == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var id influxdb.ID
	if err := id.DecodeFromString(i); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad id in url",
		}
	}

	return &deleteDocumentRequest{
		Namespace: ns,
		ID:        id,
	}, nil
}

// handlePutDocument is the HTTP handler for the PUT /api/v2/documents/:ns/:id route.
func (h *DocumentHandler) handlePutDocument(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePutDocumentRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	s, err := h.DocumentService.FindDocumentStore(ctx, req.Namespace)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := s.UpdateDocument(ctx, req.Document); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	d, err := s.FindDocument(ctx, req.Document.ID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Document updated", zap.String("document", fmt.Sprint(d)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDocumentResponse(req.Namespace, d)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type putDocumentRequest struct {
	*influxdb.Document
	Namespace string `json:"-"`
}

func decodePutDocumentRequest(ctx context.Context, r *http.Request) (*putDocumentRequest, error) {
	req := &putDocumentRequest{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, err
	}

	params := httprouter.ParamsFromContext(ctx)
	i := params.ByName("id")
	if i == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	if err := req.ID.DecodeFromString(i); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	req.Namespace = params.ByName("ns")
	if req.Namespace == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing namespace",
		}
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
func buildDocumentPath(namespace string, id influxdb.ID) string {
	return path.Join(prefixDocuments, namespace, id.String())
}
func buildDocumentLabelsPath(namespace string, id influxdb.ID) string {
	return path.Join(prefixDocuments, namespace, id.String(), "labels")
}
func buildDocumentLabelPath(namespace string, did influxdb.ID, lid influxdb.ID) string {
	return path.Join(prefixDocuments, namespace, did.String(), "labels", lid.String())
}

// CreateDocument creates a document in the specified namespace.
// Only the ids of the given labels will be used.
// After the call, if successful, the input document will contain the newly assigned ID.
func (s *documentService) CreateDocument(ctx context.Context, namespace string, orgID influxdb.ID, d *influxdb.Document) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	lids := make([]influxdb.ID, len(d.Labels))
	for i := 0; i < len(lids); i++ {
		lids[i] = d.Labels[i].ID
	}
	// Set a valid ID for proper marshaling.
	// It will be assigned by the backend in any case.
	d.ID = influxdb.ID(1)
	req := &postDocumentRequest{
		Document: d,
		OrgID:    orgID,
		Labels:   lids,
	}
	var resp documentResponse
	if err := s.Client.
		PostJSON(req, buildDocumentsPath(namespace)).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return err
	}
	d.ID = resp.ID
	return nil
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

// GetDocument returns the document with the specified id.
func (s *documentService) GetDocument(ctx context.Context, namespace string, id influxdb.ID) (*influxdb.Document, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var resp documentResponse
	if err := s.Client.
		Get(buildDocumentPath(namespace, id)).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return nil, err
	}
	return resp.Document, nil
}

// UpdateDocument updates the document with id `d.ID` and replaces the content of `d` with the patched value.
func (s *documentService) UpdateDocument(ctx context.Context, namespace string, d *influxdb.Document) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var resp documentResponse
	if err := s.Client.
		PutJSON(d, buildDocumentPath(namespace, d.ID)).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return err
	}
	*d = *resp.Document
	return nil
}

// DeleteDocument deletes the document with the given id.
func (s *documentService) DeleteDocument(ctx context.Context, namespace string, id influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.Client.
		Delete(buildDocumentPath(namespace, id)).
		Do(ctx); err != nil {
		return err
	}
	return nil
}

// GetDocumentLabels returns the labels associated to the document with the given id.
func (s *documentService) GetDocumentLabels(ctx context.Context, namespace string, id influxdb.ID) ([]*influxdb.Label, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var resp labelsResponse
	if err := s.Client.
		Get(buildDocumentLabelsPath(namespace, id)).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return nil, err
	}
	return resp.Labels, nil
}

// AddDocumentLabel adds the label with id `lid` to the document with id `did`.
func (s *documentService) AddDocumentLabel(ctx context.Context, namespace string, did influxdb.ID, lid influxdb.ID) (*influxdb.Label, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	mapping := &influxdb.LabelMapping{
		LabelID: lid,
	}
	var resp labelResponse
	if err := s.Client.
		PostJSON(mapping, buildDocumentLabelsPath(namespace, did)).
		DecodeJSON(&resp).
		Do(ctx); err != nil {
		return nil, err
	}
	return &resp.Label, nil
}

// DeleteDocumentLabel deletes the label with id `lid` from the document with id `did`.
func (s *documentService) DeleteDocumentLabel(ctx context.Context, namespace string, did influxdb.ID, lid influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := s.Client.
		Delete(buildDocumentLabelPath(namespace, did, lid)).
		Do(ctx); err != nil {
		return err
	}
	return nil
}
