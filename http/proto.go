package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	platform "github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// ProtoHandler is the handler for creating instances of
// prebuilt resources (dashboards, tasks, etcd).
type ProtoHandler struct {
	*httprouter.Router
	Logger       *zap.Logger
	ProtoService platform.ProtoService
	LabelService platform.LabelService
}

const (
	protosPath           = "/api/v2/protos"
	protosDashboardsPath = "/api/v2/protos/:protoID/dashboards"
)

func extractID(ctx context.Context, s string) (platform.ID, error) {
	params := httprouter.ParamsFromContext(ctx)
	idStr := params.ByName(s)
	if idStr == "" {
		return 0, platform.NewError(platform.WithErrorMsg("url missing protoID"), platform.WithErrorCode(platform.EInvalid))
	}

	var id platform.ID
	if err := id.DecodeFromString(idStr); err != nil {
		return 0, err
	}

	return id, nil
}

func extractProtoID(ctx context.Context) (platform.ID, error) {
	return extractID(ctx, "protoID")
}

// ProtoBackend is the backend for the proto handler.
type ProtoBackend struct {
	Logger       *zap.Logger
	ProtoService platform.ProtoService
	LabelService platform.LabelService
}

// NewProtoBackend creates an instance of the Protobackend from the APIBackend.
func NewProtoBackend(b *APIBackend) *ProtoBackend {
	return &ProtoBackend{
		Logger:       b.Logger.With(zap.String("handler", "proto")),
		ProtoService: b.ProtoService,
		LabelService: b.LabelService,
	}
}

// NewProtoHandler creates an instance of a proto handler.
func NewProtoHandler(b *ProtoBackend) *ProtoHandler {
	h := &ProtoHandler{
		Router:       NewRouter(),
		Logger:       b.Logger,
		ProtoService: b.ProtoService,
		LabelService: b.LabelService,
	}

	h.HandlerFunc("GET", protosPath, h.handleGetProtos)
	h.HandlerFunc("POST", protosDashboardsPath, h.handlePostProtosDashboards)

	return h
}

type protosResponse struct {
	Protos []*protoResponse `json:"protos"`
}

func newProtosResponse(ps []*platform.Proto) *protosResponse {
	rs := []*protoResponse{}
	for _, p := range ps {
		rs = append(rs, newProtoResponse(p))
	}

	return &protosResponse{
		Protos: rs,
	}
}

type protoResponse struct {
	Links map[string]string `json:"links"`
	*platform.Proto
}

func newProtoResponse(p *platform.Proto) *protoResponse {
	return &protoResponse{
		Links: map[string]string{
			"dashboards": fmt.Sprintf("/api/v2/protos/%s/dashboards", p.ID),
		},
		Proto: p,
	}
}

func (h *ProtoHandler) handleGetProtos(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	ps, err := h.ProtoService.FindProtos(ctx)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newProtosResponse(ps)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type createProtoResourcesRequest struct {
	ProtoID        platform.ID `json:"-"`
	OrganizationID platform.ID `json:"orgID"`
	opts           platform.FindOptions
}

// Decode turns an http request into a createProtoResourceRequest.
func (r *createProtoResourcesRequest) Decode(req *http.Request) error {
	ctx := req.Context()
	id, err := extractProtoID(ctx)
	if err != nil {
		return err
	}

	opts, err := decodeFindOptions(ctx, req)
	if err != nil {
		return err
	}
	r.opts = *opts

	r.ProtoID = id

	if err := json.NewDecoder(req.Body).Decode(r); err != nil {
		return err
	}

	return nil
}

func (h *ProtoHandler) handlePostProtosDashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req := &createProtoResourcesRequest{}
	if err := req.Decode(r); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	ds, err := h.ProtoService.CreateDashboardsFromProto(ctx, req.ProtoID, req.OrganizationID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	filter := platform.DashboardFilter{OrganizationID: &req.OrganizationID}
	if err := encodeResponse(ctx, w, http.StatusCreated, newGetDashboardsResponse(ctx, ds, filter, req.opts, h.LabelService)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}

}
