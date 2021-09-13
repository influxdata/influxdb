package dbrp

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

const (
	PrefixDBRP = "/api/v2/dbrps"
)

type Handler struct {
	chi.Router
	api     *kithttp.API
	log     *zap.Logger
	dbrpSvc influxdb.DBRPMappingService
	orgSvc  influxdb.OrganizationService
}

// NewHTTPHandler constructs a new http server.
func NewHTTPHandler(log *zap.Logger, dbrpSvc influxdb.DBRPMappingService, orgSvc influxdb.OrganizationService) *Handler {
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
	// N.B. These are purposefully typed as string instead of
	// influxdb.ID so we can provide more specific error messages.
	// If they have the ID type, our JSON decoder will just return
	// a generic "invalid ID" error without stating which ID is
	// the problem.
	//
	// Ideally we'd fix the decoder so we could get more useful
	// errors everywhere, but I'm worried about the impact of a
	// system-wide change to our "invalid ID" error format.
	OrganizationID string `json:"orgID"`
	BucketID       string `json:"bucketID"`
}

func (h *Handler) handlePostDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req createDBRPRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "invalid json structure",
			Err:  err,
		})
		return
	}

	var orgID platform.ID
	var bucketID platform.ID

	if req.OrganizationID == "" {
		if req.Org == "" {
			h.api.Err(w, r, ErrNoOrgProvided)
			return
		}
		org, err := h.orgSvc.FindOrganization(r.Context(), influxdb.OrganizationFilter{
			Name: &req.Org,
		})
		if err != nil {
			h.api.Err(w, r, ErrOrgNotFound(req.Org))
			return
		}
		orgID = org.ID
	} else if err := orgID.DecodeFromString(req.OrganizationID); err != nil {
		h.api.Err(w, r, ErrInvalidOrgID(req.OrganizationID, err))
		return
	}

	if err := bucketID.DecodeFromString(req.BucketID); err != nil {
		h.api.Err(w, r, ErrInvalidBucketID(req.BucketID, err))
		return
	}

	dbrp := &influxdb.DBRPMapping{
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
	Content []*influxdb.DBRPMapping `json:"content"`
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
	Content *influxdb.DBRPMapping `json:"content"`
}

func (h *Handler) handleGetDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	if id == "" {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i platform.ID
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
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i platform.ID
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
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
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
		Content *influxdb.DBRPMapping `json:"content"`
	}{
		Content: dbrp,
	})
}

func (h *Handler) handleDeleteDBRP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := chi.URLParam(r, "id")
	if id == "" {
		h.api.Err(w, r, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		})
		return
	}

	var i platform.ID
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

func (h *Handler) getFilterFromHTTPRequest(r *http.Request) (f influxdb.DBRPMappingFilter, err error) {
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
			return f, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "invalid default parameter",
			}
		}
		f.Default = &d
	}
	return f, nil
}

func getIDFromHTTPRequest(r *http.Request, key string, onErr func(string, error) error) (*platform.ID, error) {
	var id platform.ID
	raw := r.URL.Query().Get(key)
	if raw != "" {
		if err := id.DecodeFromString(raw); err != nil {
			return nil, onErr(raw, err)
		}
	} else {
		return nil, nil
	}
	return &id, nil
}

// mustGetOrgIDFromHTTPRequest returns the org ID parameter from the request, falling
// back to looking up the org ID by org name if the ID parameter is not present.
func (h *Handler) mustGetOrgIDFromHTTPRequest(r *http.Request) (*platform.ID, error) {
	orgID, err := getIDFromHTTPRequest(r, "orgID", ErrInvalidOrgID)
	if err != nil {
		return nil, err
	}
	if orgID == nil {
		name := r.URL.Query().Get("org")
		if name == "" {
			return nil, ErrNoOrgProvided
		}
		org, err := h.orgSvc.FindOrganization(r.Context(), influxdb.OrganizationFilter{
			Name: &name,
		})
		if err != nil {
			return nil, ErrOrgNotFound(name)
		}
		orgID = &org.ID
	}
	return orgID, nil
}

func getDBRPIDFromHTTPRequest(r *http.Request) (*platform.ID, error) {
	return getIDFromHTTPRequest(r, "id", ErrInvalidDBRPID)
}

func getBucketIDFromHTTPRequest(r *http.Request) (*platform.ID, error) {
	return getIDFromHTTPRequest(r, "bucketID", ErrInvalidBucketID)
}
