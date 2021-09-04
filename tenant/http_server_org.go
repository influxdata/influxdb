package tenant

import (
	"context"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb/v2/kit/platform"

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
func NewHTTPOrgHandler(log *zap.Logger, orgService influxdb.OrganizationService, urm http.Handler, secretHandler http.Handler) *OrgHandler {
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
			mountableRouter := r.With(kithttp.ValidResource(svr.api, svr.lookupOrgByID))
			mountableRouter.Mount("/members", urm)
			mountableRouter.Mount("/owners", urm)
			mountableRouter.Mount("/secrets", secretHandler)
		})
	})
	svr.Router = r
	return svr
}

type orgResponse struct {
	Links map[string]string `json:"links"`
	influxdb.Organization
}

func newOrgResponse(o influxdb.Organization) orgResponse {
	return orgResponse{
		Links: map[string]string{
			"self":       fmt.Sprintf("/api/v2/orgs/%s", o.ID),
			"logs":       fmt.Sprintf("/api/v2/orgs/%s/logs", o.ID),
			"members":    fmt.Sprintf("/api/v2/orgs/%s/members", o.ID),
			"owners":     fmt.Sprintf("/api/v2/orgs/%s/owners", o.ID),
			"secrets":    fmt.Sprintf("/api/v2/orgs/%s/secrets", o.ID),
			"labels":     fmt.Sprintf("/api/v2/orgs/%s/labels", o.ID),
			"buckets":    fmt.Sprintf("/api/v2/buckets?org=%s", o.Name),
			"tasks":      fmt.Sprintf("/api/v2/tasks?org=%s", o.Name),
			"dashboards": fmt.Sprintf("/api/v2/dashboards?org=%s", o.Name),
		},
		Organization: o,
	}
}

type orgsResponse struct {
	Links         *influxdb.PagingLinks `json:"links"`
	Organizations []orgResponse         `json:"orgs"`
}

func newOrgsResponse(orgs []*influxdb.Organization, opts influxdb.FindOptions, f influxdb.OrganizationFilter) *orgsResponse {
	res := orgsResponse{
		Links:         influxdb.NewPagingLinks(prefixOrganizations, opts, f, len(orgs)),
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
		h.api.Err(w, r, err)
		return
	}

	if err := h.orgSvc.CreateOrganization(r.Context(), &org); err != nil {
		h.api.Err(w, r, err)
		return
	}

	h.log.Debug("Org created", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, r, http.StatusCreated, newOrgResponse(org))
}

// handleGetOrg is the HTTP handler for the GET /api/v2/orgs/:id route.
func (h *OrgHandler) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	org, err := h.orgSvc.FindOrganizationByID(r.Context(), *id)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Org retrieved", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, r, http.StatusOK, newOrgResponse(*org))
}

// handleGetOrgs is the HTTP handler for the GET /api/v2/orgs route.
func (h *OrgHandler) handleGetOrgs(w http.ResponseWriter, r *http.Request) {
	qp := r.URL.Query()

	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	var filter influxdb.OrganizationFilter
	if name := qp.Get("org"); name != "" {
		filter.Name = &name
	}

	if id := qp.Get("orgID"); id != "" {
		i, err := platform.IDFromString(id)
		if err == nil {
			filter.ID = i
		}
	}

	if id := qp.Get("userID"); id != "" {
		i, err := platform.IDFromString(id)
		if err == nil {
			filter.UserID = i
		}
	}

	orgs, _, err := h.orgSvc.FindOrganizations(r.Context(), filter, *opts)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Orgs retrieved", zap.String("org", fmt.Sprint(orgs)))

	h.api.Respond(w, r, http.StatusOK, newOrgsResponse(orgs, *opts, filter))
}

// handlePatchOrg is the HTTP handler for the PATH /api/v2/orgs route.
func (h *OrgHandler) handlePatchOrg(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	var upd influxdb.OrganizationUpdate
	if err := h.api.DecodeJSON(r.Body, &upd); err != nil {
		h.api.Err(w, r, err)
		return
	}

	org, err := h.orgSvc.UpdateOrganization(r.Context(), *id, upd)
	if err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Org updated", zap.String("org", fmt.Sprint(org)))

	h.api.Respond(w, r, http.StatusOK, newOrgResponse(*org))
}

// handleDeleteOrganization is the HTTP handler for the DELETE /api/v2/orgs/:id route.
func (h *OrgHandler) handleDeleteOrg(w http.ResponseWriter, r *http.Request) {
	id, err := platform.IDFromString(chi.URLParam(r, "id"))
	if err != nil {
		h.api.Err(w, r, err)
		return
	}

	ctx := r.Context()
	if err := h.orgSvc.DeleteOrganization(ctx, *id); err != nil {
		h.api.Err(w, r, err)
		return
	}
	h.log.Debug("Org deleted", zap.String("orgID", fmt.Sprint(id)))

	h.api.Respond(w, r, http.StatusNoContent, nil)
}

func (h *OrgHandler) lookupOrgByID(ctx context.Context, id platform.ID) (platform.ID, error) {
	_, err := h.orgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		return 0, err
	}

	return id, nil
}
