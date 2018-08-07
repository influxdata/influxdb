package http

import (
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/influxdata/platform/chronograf/server"
	"github.com/julienschmidt/httprouter"
)

// ChronografHandler is an http handler for serving chronograf chronografs.
type ChronografHandler struct {
	*httprouter.Router
	Service *server.Service
}

// NewChronografHandler is the constructor an chronograf handler.
func NewChronografHandler(s *server.Service) *ChronografHandler {
	h := &ChronografHandler{
		Router:  httprouter.New(),
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

	// Sources
	h.HandlerFunc("GET", "/chronograf/v1/sources", h.Service.Sources)
	h.HandlerFunc("POST", "/chronograf/v1/sources", h.Service.NewSource)

	h.HandlerFunc("GET", "/chronograf/v1/sources/:id", h.Service.SourcesID)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id", h.Service.UpdateSource)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id", h.Service.RemoveSource)
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/health", h.Service.SourceHealth)

	// Source Proxy to Influx; Has gzip compression around the handler
	influx := gziphandler.GzipHandler(http.HandlerFunc(h.Service.Influx))
	h.Handler("POST", "/chronograf/v1/sources/:id/proxy", influx)

	// Write proxies line protocol write requests to InfluxDB
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/write", h.Service.Write)

	// Queries is used to analyze a specific queries and does not create any
	// resources. It's a POST because Queries are POSTed to InfluxDB, but this
	// only modifies InfluxDB resources with certain metaqueries, e.g. DROP DATABASE.
	//
	// Admins should ensure that the InfluxDB source as the proper permissions
	// intended for Chronograf Users with the Viewer Role type.
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/queries", h.Service.Queries)

	// Annotations are user-defined events associated with this source
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/annotations", h.Service.Annotations)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/annotations", h.Service.NewAnnotation)
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/annotations/:aid", h.Service.Annotation)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/annotations/:aid", h.Service.RemoveAnnotation)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id/annotations/:aid", h.Service.UpdateAnnotation)

	// All possible permissions for users in this source
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/permissions", h.Service.Permissions)

	// Users associated with the data source
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/users", h.Service.SourceUsers)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/users", h.Service.NewSourceUser)

	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/users/:uid", h.Service.SourceUserID)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/users/:uid", h.Service.RemoveSourceUser)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id/users/:uid", h.Service.UpdateSourceUser)

	// Roles associated with the data source
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/roles", h.Service.SourceRoles)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/roles", h.Service.NewSourceRole)

	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/roles/:rid", h.Service.SourceRoleID)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/roles/:rid", h.Service.RemoveSourceRole)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id/roles/:rid", h.Service.UpdateSourceRole)

	// h.Services are resources that chronograf proxies to
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/services", h.Service.Services)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/service", h.Service.NewService)
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/services/:kid", h.Service.ServiceID)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id/services/:kid", h.Service.UpdateService)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/services/:kid", h.Service.RemoveService)

	// h.Service Proxy
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/h.Services/:kid/proxy", h.Service.ProxyGet)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/h.Services/:kid/proxy", h.Service.ProxyPost)
	h.HandlerFunc("PATCH", "/chronograf/v1/sources/:id/h.Services/:kid/proxy", h.Service.ProxyPatch)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/h.Services/:kid/proxy", h.Service.ProxyDelete)

	// Kapacitor
	//h.HandlerFunc("GET","/chronograf/v1/sources/:id/kapacitors", h.Service.Kapacitors))
	//h.HandlerFunc("POST","/chronograf/v1/sources/:id/kapacitors", h.Service.NewKapacitor))

	//h.HandlerFunc("GET","/chronograf/v1/sources/:id/kapacitors/:kid", h.Service.KapacitorsID))
	//h.HandlerFunc("PATCH","/chronograf/v1/sources/:id/kapacitors/:kid", h.Service.UpdateKapacitor))
	//h.HandlerFunc("DELETE","/chronograf/v1/sources/:id/kapacitors/:kid", h.Service.RemoveKapacitor))

	//// Kapacitor rules
	//h.HandlerFunc("GET","/chronograf/v1/sources/:id/kapacitors/:kid/rules", h.Service.KapacitorRulesGet))
	//h.HandlerFunc("POST","/chronograf/v1/sources/:id/kapacitors/:kid/rules", h.Service.KapacitorRulesPost))

	//h.HandlerFunc("GET","/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", h.Service.KapacitorRulesID))
	//h.HandlerFunc("PUT","/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", h.Service.KapacitorRulesPut))
	//h.HandlerFunc("PATCH","/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", h.Service.KapacitorRulesStatus))
	//h.HandlerFunc("DELETE","/chronograf/v1/sources/:id/kapacitors/:kid/rules/:tid", h.Service.KapacitorRulesDelete))

	//// Kapacitor Proxy
	//h.HandlerFunc("GET","/chronograf/v1/sources/:id/kapacitors/:kid/proxy", h.Service.ProxyGet))
	//h.HandlerFunc("POST","/chronograf/v1/sources/:id/kapacitors/:kid/proxy", h.Service.ProxyPost))
	//h.HandlerFunc("PATCH","/chronograf/v1/sources/:id/kapacitors/:kid/proxy", h.Service.ProxyPatch))
	//h.HandlerFunc("DELETE","/chronograf/v1/sources/:id/kapacitors/:kid/proxy", h.Service.ProxyDelete))

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

	// Databases
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/dbs", h.Service.GetDatabases)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/dbs", h.Service.NewDatabase)

	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/dbs/:db", h.Service.DropDatabase)

	// Retention Policies
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/dbs/:db/rps", h.Service.RetentionPolicies)
	h.HandlerFunc("POST", "/chronograf/v1/sources/:id/dbs/:db/rps", h.Service.NewRetentionPolicy)

	h.HandlerFunc("PUT", "/chronograf/v1/sources/:id/dbs/:db/rps/:rp", h.Service.UpdateRetentionPolicy)
	h.HandlerFunc("DELETE", "/chronograf/v1/sources/:id/dbs/:db/rps/:rp", h.Service.DropRetentionPolicy)

	// Measurements
	h.HandlerFunc("GET", "/chronograf/v1/sources/:id/dbs/:db/measurements", h.Service.Measurements)

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
		//Logger:      opts.Logger,
		//CustomLinks: opts.CustomLinks,
		StatusFeed: "https://www.influxdata.com/feed/json",
	}

	h.Handler("GET", "/chronograf/v1/", allRoutes)

	return h
}
