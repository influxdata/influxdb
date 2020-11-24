package dbrp

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const (
	PrefixDBRP = "/api/v2/dbrps"
)

var (
	ErrMissingOrgID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "invalid json structure: either 'orgID' or 'org' must be specified",
	}
	ErrMissingBucketID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "invalid json structure: 'bucketID' must be specified",
	}
	ErrInvalidOrgID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "invalid orgID",
	}
	ErrInvalidBucketID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "invalid bucketID",
	}
	ErrInvalidDbrpID = &influxdb.Error{
		Code: influxdb.EInvalid,
		Msg:  "invalid DBRP id",
	}
)

type Handler struct {
	chi.Router
	api     *kithttp.API
	log     *zap.Logger
	dbrpSvc influxdb.DBRPMappingServiceV2
	orgSvc  influxdb.OrganizationService
}

// NewHTTPHandler constructs a new http server.
func NewHTTPHandler(log *zap.Logger, dbrpSvc influxdb.DBRPMappingServiceV2, orgSvc influxdb.OrganizationService) *Handler {
	h := &Handler{
		api:     kithttp.NewAPI(kithttp.WithLog(log)),
		log:     log,
		dbrpSvc: dbrpSvc,
		orgSvc:  orgSvc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Route("/", func(r chi.Router) {
		r.Post("/", h.handlePostDBRP)
		r.Get("/", h.handleGetDBRPs)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.handleGetDBRP)
			r.Patch("/", h.handlePatchDBRP)
			r.Delete("/", h.handleDeleteDBRP)
		})
	})

	h.Router = r
	return h
}

type createDBRPRequest struct {
	Database        string `json:"database"`
	RetentionPolicy string `json:"retention_policy"`
	Default         bool   `json:"default"`
	Org             string `json:"org"`
	OrganizationID  string `json:"orgID"`
	BucketID        string `json:"bucketID"`
}

func (h *Handler) handlePostDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req createDBRPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid json structure",
			Err:  err,
		})
		return
	}

	// Make sure all required IDs were provided.
	//
	// N.B. we have special checks for these fields because a previous impl (incorrectly)
	// expected them to arrive under different keys in the JSON. When we fixed the impl,
	// existing users of the API started getting confusing 'invalid ID' errors because the
	// old keys were getting ignored, and the ID fields were populated by empty string.
	// The checks here allow us to provide more specific error messages.
	if req.OrganizationID == "" && req.Org == "" {
		h.api.Err(w, r, ErrMissingOrgID)
		return
	}
	if req.BucketID == "" {
		h.api.Err(w, r, ErrMissingBucketID)
		return
	}

	var orgID influxdb.ID
	if req.OrganizationID == "" {
		org, err := h.orgSvc.FindOrganization(r.Context(), influxdb.OrganizationFilter{
			Name: &req.Org,
		})
		if err != nil {
			h.api.Err(w, r, influxdb.ErrOrgNotFound)
			return
		}
		orgID = org.ID
	} else if err := orgID.DecodeFromString(req.OrganizationID); err != nil {
		h.api.Err(w, r, ErrInvalidOrgID)
		return
	}

	var bucketID influxdb.ID
	if err := bucketID.DecodeFromString(req.BucketID); err != nil {
		h.api.Err(w, r, ErrInvalidBucketID)
		return
	}

	dbrp := &influxdb.DBRPMappingV2{
		Database:        req.Database,
		RetentionPolicy: req.RetentionPolicy,
		Default:         req.Default,
		OrganizationID:  orgID,
		BucketID:        bucketID,
	}
	if err := h.dbrpSvc.Create(ctx, dbrp); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusCreated, dbrp)
}

type getDBRPsResponse struct {
	Content []*influxdb.DBRPMappingV2 `json:"content"`
}

func (h *Handler) handleGetDBRPs(w http.ResponseWriter, r *http.Request) {
	filter, err := h.getFilterFromHTTPRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	dbrps, _, err := h.dbrpSvc.FindMany(r.Context(), filter)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, getDBRPsResponse{
		Content: dbrps,
	})
}

type getDBRPResponse struct {
	Content *influxdb.DBRPMappingV2 `json:"content"`
}

func (h *Handler) handleGetDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	if id == "" {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	orgID, err := h.mustGetOrgIDFromHTTPRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	dbrp, err := h.dbrpSvc.FindByID(ctx, *orgID, i)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.api.Respond(w, r, http.StatusOK, getDBRPResponse{
		Content: dbrp,
	})
}

func (h *Handler) handlePatchDBRP(w http.ResponseWriter, r *http.Request) {
	bodyRequest := struct {
		Default         *bool   `json:"default"`
		RetentionPolicy *string `json:"retention_policy"`
	}{}

	ctx := r.Context()

	id := chi.URLParam(r, "id")
	if id == "" {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	orgID, err := h.mustGetOrgIDFromHTTPRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	dbrp, err := h.dbrpSvc.FindByID(ctx, *orgID, i)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := json.NewDecoder(r.Body).Decode(&bodyRequest); err != nil {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid json structure",
			Err:  err,
		})
		return
	}

	if bodyRequest.Default != nil && dbrp.Default != *bodyRequest.Default {
		dbrp.Default = *bodyRequest.Default
	}

	if bodyRequest.RetentionPolicy != nil && *bodyRequest.RetentionPolicy != dbrp.RetentionPolicy {
		dbrp.RetentionPolicy = *bodyRequest.RetentionPolicy
	}

	if err := h.dbrpSvc.Update(ctx, dbrp); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.api.Respond(w, r, http.StatusOK, struct {
		Content *influxdb.DBRPMappingV2 `json:"content"`
	}{
		Content: dbrp,
	})
}

func (h *Handler) handleDeleteDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	if id == "" {
		h.api.Err(w, r, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		h.api.Err(w, r, err)
		return
	}

	orgID, err := h.mustGetOrgIDFromHTTPRequest(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	if err := h.dbrpSvc.Delete(ctx, *orgID, i); err != nil {
		h.api.Err(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) getFilterFromHTTPRequest(r *http.Request) (f influxdb.DBRPMappingFilterV2, err error) {
	// Always provide OrgID.
	f.OrgID, err = h.mustGetOrgIDFromHTTPRequest(r)
	if err != nil {
		return f, err
	}
	f.ID, err = getDBRPIDFromHTTPRequest(r)
	if err != nil {
		return f, err
	}
	f.BucketID, err = getBucketIDFromHTTPRequest(r)
	if err != nil {
		return f, err
	}
	rawDB := r.URL.Query().Get("db")
	if rawDB != "" {
		f.Database = &rawDB
	}
	rawRP := r.URL.Query().Get("rp")
	if rawRP != "" {
		f.RetentionPolicy = &rawRP
	}
	rawDefault := r.URL.Query().Get("default")
	if rawDefault != "" {
		d, err := strconv.ParseBool(rawDefault)
		if err != nil {
			return f, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "invalid default parameter",
			}
		}
		f.Default = &d
	}
	return f, nil
}

func getIDFromHTTPRequest(r *http.Request, key string, retErr error) (*influxdb.ID, error) {
	var id influxdb.ID
	raw := r.URL.Query().Get(key)
	if raw != "" {
		if err := id.DecodeFromString(raw); err != nil {
			return nil, retErr
		}
	} else {
		return nil, nil
	}
	return &id, nil
}

// mustGetOrgIDFromHTTPRequest returns the org ID parameter from the request, falling
// back to looking up the org ID by org name if the ID parameter is not present.
func (h *Handler) mustGetOrgIDFromHTTPRequest(r *http.Request) (*influxdb.ID, error) {
	orgID, err := getIDFromHTTPRequest(r, "orgID", ErrInvalidOrgID)
	if err != nil {
		return nil, err
	}
	if orgID == nil {
		name := r.URL.Query().Get("org")
		if name == "" {
			return nil, influxdb.ErrOrgNotFound
		}
		org, err := h.orgSvc.FindOrganization(r.Context(), influxdb.OrganizationFilter{
			Name: &name,
		})
		if err != nil {
			return nil, influxdb.ErrOrgNotFound
		}
		orgID = &org.ID
	}
	return orgID, nil
}

func getDBRPIDFromHTTPRequest(r *http.Request) (*influxdb.ID, error) {
	return getIDFromHTTPRequest(r, "id", ErrInvalidDbrpID)
}

func getBucketIDFromHTTPRequest(r *http.Request) (*influxdb.ID, error) {
	return getIDFromHTTPRequest(r, "bucketID", ErrInvalidBucketID)
}
