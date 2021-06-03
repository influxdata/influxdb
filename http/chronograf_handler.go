package http

import (
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2/chronograf/server"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

const prefixChronograf = "/chronograf"

// ChronografHandler is an http handler for serving chronograf chronografs.
type ChronografHandler struct {
	*httprouter.Router
	Service *server.Service
}

// NewChronografHandler is the constructor an chronograf handler.
func NewChronografHandler(s *server.Service, he errors.HTTPErrorHandler) *ChronografHandler {
	h := &ChronografHandler{
		Router:  NewRouter(he),
		Service: s,
	}
	/* API */
	// Organizations
	h.HandlerFunc("GET", "/chronograf/v1/organizations", h.Service.Organizations)
	h.HandlerFunc("POST", "/chronograf/v1/organizations", h.Service.NewOrganization)

	h.HandlerFunc("GET", "/chronograf/v1/organizations/:oid", h.Service.OrganizationID)
	h.HandlerFunc("PATCH", "/chronograf/v1/organizations/:oid", h.Service.UpdateOrganization)
	h.HandlerFunc("DELETE", "/chronograf/v1/organizations/:oid", h.Service.RemoveOrganization)

	// Mappings
	h.HandlerFunc("GET", "/chronograf/v1/mappings", h.Service.Mappings)
	h.HandlerFunc("POST", "/chronograf/v1/mappings", h.Service.NewMapping)

	h.HandlerFunc("PUT", "/chronograf/v1/mappings/:id", h.Service.UpdateMapping)
	h.HandlerFunc("DELETE", "/chronograf/v1/mappings/:id", h.Service.RemoveMapping)

	// Layouts
	h.HandlerFunc("GET", "/chronograf/v1/layouts", h.Service.Layouts)
	h.HandlerFunc("GET", "/chronograf/v1/layouts/:id", h.Service.LayoutsID)

	// Users associated with Chronograf
	h.HandlerFunc("GET", "/chronograf/v1/me", h.Service.Me)

	// TODO(desa): what to do here?
	// Set current chronograf organization the user is logged into
	//h.HandlerFunc("PUT", "/chronograf/v1/me", h.Service.UpdateMe(opts.Auth))

	// TODO(desa): what to do about admin's being able to set superadmin
	h.HandlerFunc("GET", "/chronograf/v1/organizations/:oid/users", h.Service.Users)
	h.HandlerFunc("POST", "/chronograf/v1/organizations/:oid/users", h.Service.NewUser)

	h.HandlerFunc("GET", "/chronograf/v1/organizations/:oid/users/:id", h.Service.UserID)
	h.HandlerFunc("DELETE", "/chronograf/v1/organizations/:oid/users/:id", h.Service.RemoveUser)
	h.HandlerFunc("PATCH", "/chronograf/v1/organizations/:oid/users/:id", h.Service.UpdateUser)

	h.HandlerFunc("GET", "/chronograf/v1/users", h.Service.Users)
	h.HandlerFunc("POST", "/chronograf/v1/users", h.Service.NewUser)

	h.HandlerFunc("GET", "/chronograf/v1/users/:id", h.Service.UserID)
	h.HandlerFunc("DELETE", "/chronograf/v1/users/:id", h.Service.RemoveUser)
	h.HandlerFunc("PATCH", "/chronograf/v1/users/:id", h.Service.UpdateUser)

	// Dashboards
	h.HandlerFunc("GET", "/chronograf/v1/dashboards", h.Service.Dashboards)
	h.HandlerFunc("POST", "/chronograf/v1/dashboards", h.Service.NewDashboard)

	h.HandlerFunc("GET", "/chronograf/v1/dashboards/:id", h.Service.DashboardID)
	h.HandlerFunc("DELETE", "/chronograf/v1/dashboards/:id", h.Service.RemoveDashboard)
	h.HandlerFunc("PUT", "/chronograf/v1/dashboards/:id", h.Service.ReplaceDashboard)
	h.HandlerFunc("PATCH", "/chronograf/v1/dashboards/:id", h.Service.UpdateDashboard)
	// Dashboard Cells
	h.HandlerFunc("GET", "/chronograf/v1/dashboards/:id/cells", h.Service.DashboardCells)
	h.HandlerFunc("POST", "/chronograf/v1/dashboards/:id/cells", h.Service.NewDashboardCell)

	h.HandlerFunc("GET", "/chronograf/v1/dashboards/:id/cells/:cid", h.Service.DashboardCellID)
	h.HandlerFunc("DELETE", "/chronograf/v1/dashboards/:id/cells/:cid", h.Service.RemoveDashboardCell)
	h.HandlerFunc("PUT", "/chronograf/v1/dashboards/:id/cells/:cid", h.Service.ReplaceDashboardCell)
	// Dashboard Templates
	h.HandlerFunc("GET", "/chronograf/v1/dashboards/:id/templates", h.Service.Templates)
	h.HandlerFunc("POST", "/chronograf/v1/dashboards/:id/templates", h.Service.NewTemplate)

	h.HandlerFunc("GET", "/chronograf/v1/dashboards/:id/templates/:tid", h.Service.TemplateID)
	h.HandlerFunc("DELETE", "/chronograf/v1/dashboards/:id/templates/:tid", h.Service.RemoveTemplate)
	h.HandlerFunc("PUT", "/chronograf/v1/dashboards/:id/templates/:tid", h.Service.ReplaceTemplate)

	// Global application config for Chronograf
	h.HandlerFunc("GET", "/chronograf/v1/config", h.Service.Config)
	h.HandlerFunc("GET", "/chronograf/v1/config/auth", h.Service.AuthConfig)
	h.HandlerFunc("PUT", "/chronograf/v1/config/auth", h.Service.ReplaceAuthConfig)

	// Organization config settings for Chronograf
	h.HandlerFunc("GET", "/chronograf/v1/org_config", h.Service.OrganizationConfig)
	h.HandlerFunc("GET", "/chronograf/v1/org_config/logviewer", h.Service.OrganizationLogViewerConfig)
	h.HandlerFunc("PUT", "/chronograf/v1/org_config/logviewer", h.Service.ReplaceOrganizationLogViewerConfig)

	h.HandlerFunc("GET", "/chronograf/v1/env", h.Service.Environment)

	allRoutes := &server.AllRoutes{
		// TODO(desa): what to do here
		//logger:      opts.logger,
		//CustomLinks: opts.CustomLinks,
		StatusFeed: "https://www.influxdata.com/feed/json",
	}

	h.Handler("GET", "/chronograf/v1/", allRoutes)

	return h
}
