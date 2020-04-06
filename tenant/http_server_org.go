package tenant

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb/v2"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// OrgHandler represents an HTTP API handler for organizations.
type OrgHandler struct {
	chi.Router
	api    *kithttp.API
	log    *zap.Logger
	orgSvc influxdb.OrganizationService
}

const (
	prefixOrganizations = "/api/v2/orgs"
)

func (h *OrgHandler) Prefix() string {
	return prefixOrganizations
}

// NewHTTPOrgHandler constructs a new http server.
func NewHTTPOrgHandler(log *zap.Logger, orgService influxdb.OrganizationService, urm http.Handler, labelHandler http.Handler, secretHandler http.Handler) *OrgHandler {
	svr := &OrgHandler{
		api:    kithttp.NewAPI(kithttp.WithLog(log)),
		log:    log,
		orgSvc: orgService,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)

	r.Route("/", func(r chi.Router) {
		r.Post("/", svr.handlePostOrg)
		r.Get("/", svr.handleGetOrgs)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", svr.handleGetOrg)
			r.Patch("/", svr.handlePatchOrg)
			r.Delete("/", svr.handleDeleteOrg)

			// mount embedded resources
			r.Mount("/members", urm)
			r.Mount("/owners", urm)
			r.Mount("/labels", labelHandler)
			r.Mount("/secrets", secretHandler)
		})
	})

	svr.Router = r
	return svr
}

type orgResponse struct {
	influxdb.Organization
}

func newOrgResponse(o influxdb.Organization) orgResponse {
	return orgResponse{
		Organization: o,
	}
}

type orgsResponse struct {
	Organizations []orgResponse `json:"orgs"`
}

func newOrgsResponse(orgs []*influxdb.Organization) *orgsResponse {
	res := orgsResponse{
		Organizations: []orgResponse{},
	}
	for _, org := range orgs {
		res.Organizations = append(res.Organizations, newOrgResponse(*org))
	}
	return &res
}

// handlePostOrg is the HTTP handler for the POST /api/v2/orgs route.
func (h *OrgHandler) handlePostOrg(w http.ResponseWriter, r *http.Request) {
	var org influxdb.Organization
	if err := h.api.DecodeJSON(r.Body, &org); err != nil {
		h.api.Err(w, err)
		return
	}

	if err := h.orgSvc.CreateOrganization(r.Context(), &org); err != nil {
		h.api.Err(w, err)
		return
	}

	h.log.Debug("Org created", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, http.StatusCreated, newOrgResponse(org))
}

// handleGetOrg is the HTTP handler for the GET /api/v2/orgs/:id route.
func (h *OrgHandler) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, err)
		return
	}

	org, err := h.orgSvc.FindOrganizationByID(r.Context(), *id)
	if err != nil {
		h.api.Err(w, err)
		return
	}
	h.log.Debug("Org retrieved", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, http.StatusOK, newOrgResponse(*org))
}

// handleGetOrgs is the HTTP handler for the GET /api/v2/orgs route.
func (h *OrgHandler) handleGetOrgs(w http.ResponseWriter, r *http.Request) {
	var filter influxdb.OrganizationFilter
	qp := r.URL.Query()
	if name := qp.Get("org"); name != "" {
		filter.Name = &name
	}

	if id := qp.Get("orgID"); id != "" {
		i, err := influxdb.IDFromString(id)
		if err == nil {
			filter.ID = i
		}
	}

	orgs, _, err := h.orgSvc.FindOrganizations(r.Context(), filter)
	if err != nil {
		h.api.Err(w, err)
		return
	}
	h.log.Debug("Orgs retrieved", zap.String("org", fmt.Sprint(orgs)))

	h.api.Respond(w, http.StatusOK, newOrgsResponse(orgs))
}

// handlePatchOrg is the HTTP handler for the PATH /api/v2/orgs route.
func (h *OrgHandler) handlePatchOrg(w http.ResponseWriter, r *http.Request) {
	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, err)
		return
	}

	var upd influxdb.OrganizationUpdate
	if err := h.api.DecodeJSON(r.Body, &upd); err != nil {
		h.api.Err(w, err)
		return
	}

	org, err := h.orgSvc.UpdateOrganization(r.Context(), *id, upd)
	if err != nil {
		h.api.Err(w, err)
		return
	}
	h.log.Debug("Org updated", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, http.StatusOK, newOrgResponse(*org))
}

// handleDeleteOrganization is the HTTP handler for the DELETE /api/v2/orgs/:id route.
func (h *OrgHandler) handleDeleteOrg(w http.ResponseWriter, r *http.Request) {
	id, err := influxdb.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, err)
		return
	}

	ctx := r.Context()
	if err := h.orgSvc.DeleteOrganization(ctx, *id); err != nil {
		h.api.Err(w, err)
		return
	}
	h.log.Debug("Org deleted", zap.String("orgID", fmt.Sprint(id)))

	h.api.Respond(w, http.StatusNoContent, nil)
}
