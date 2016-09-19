package restapi

import (
	"crypto/tls"
	"log"
	"net/http"
	"strings"

	errors "github.com/go-openapi/errors"
	runtime "github.com/go-openapi/runtime"
	middleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/swag"
	"golang.org/x/net/context"

	"github.com/influxdata/mrfusion"
	"github.com/influxdata/mrfusion/dist"
	"github.com/influxdata/mrfusion/mock"
	"github.com/influxdata/mrfusion/restapi/operations"
)

// This file is safe to edit. Once it exists it will not be overwritten

//go:generate swagger generate server --target .. --name  --spec ../swagger.yaml --with-context

var devFlags = struct {
	Develop bool `short:"d" long:"develop" description:"Run server in develop mode."`
}{}

func configureFlags(api *operations.MrFusionAPI) {
	api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{
		swag.CommandLineOptionsGroup{
			ShortDescription: "Develop Mode server",
			LongDescription:  "Server will use the ui/build directory directly.",
			Options:          &devFlags,
		},
	}
}

func assets() mrfusion.Assets {
	if devFlags.Develop {
		return &dist.DebugAssets{
			Dir: "ui/build",
		}
	}
	return &dist.BindataAssets{
		Prefix: "ui/build",
	}
}

func configureAPI(api *operations.MrFusionAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	// s.api.Logger = log.Printf

	api.JSONConsumer = runtime.JSONConsumer()

	api.JSONProducer = runtime.JSONProducer()

	mockHandler := mock.NewHandler()

	api.DeleteDashboardsIDHandler = operations.DeleteDashboardsIDHandlerFunc(func(ctx context.Context, params operations.DeleteDashboardsIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .DeleteDashboardsID has not yet been implemented")
	})
	api.DeleteSourcesIDHandler = operations.DeleteSourcesIDHandlerFunc(func(ctx context.Context, params operations.DeleteSourcesIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .DeleteSourcesID has not yet been implemented")
	})
	api.DeleteSourcesIDRolesRoleIDHandler = operations.DeleteSourcesIDRolesRoleIDHandlerFunc(func(ctx context.Context, params operations.DeleteSourcesIDRolesRoleIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .DeleteSourcesIDRolesRoleID has not yet been implemented")
	})
	api.DeleteSourcesIDUsersUserIDExplorationsExplorationIDHandler = operations.DeleteSourcesIDUsersUserIDExplorationsExplorationIDHandlerFunc(func(ctx context.Context, params operations.DeleteSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .DeleteSourcesIDUsersUserIDExplorationsExplorationID has not yet been implemented")
	})
	api.DeleteSourcesIDUsersUserIDHandler = operations.DeleteSourcesIDUsersUserIDHandlerFunc(func(ctx context.Context, params operations.DeleteSourcesIDUsersUserIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .DeleteSourcesIDUsersUserID has not yet been implemented")
	})
	api.GetHandler = operations.GetHandlerFunc(func(ctx context.Context, params operations.GetParams) middleware.Responder {
		return middleware.NotImplemented("operation .Get has not yet been implemented")
	})
	api.GetDashboardsHandler = operations.GetDashboardsHandlerFunc(func(ctx context.Context, params operations.GetDashboardsParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetDashboards has not yet been implemented")
	})
	api.GetDashboardsIDHandler = operations.GetDashboardsIDHandlerFunc(func(ctx context.Context, params operations.GetDashboardsIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetDashboardsID has not yet been implemented")
	})
	api.GetSourcesHandler = operations.GetSourcesHandlerFunc(func(ctx context.Context, params operations.GetSourcesParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSources has not yet been implemented")
	})
	api.GetSourcesIDHandler = operations.GetSourcesIDHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesID has not yet been implemented")
	})
	api.GetSourcesIDPermissionsHandler = operations.GetSourcesIDPermissionsHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDPermissionsParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesIDPermissions has not yet been implemented")
	})
	api.GetSourcesIDRolesHandler = operations.GetSourcesIDRolesHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDRolesParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesIDRoles has not yet been implemented")
	})
	api.GetSourcesIDRolesRoleIDHandler = operations.GetSourcesIDRolesRoleIDHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDRolesRoleIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesIDRolesRoleID has not yet been implemented")
	})

	api.GetSourcesIDUsersUserIDExplorationsExplorationIDHandler = operations.GetSourcesIDUsersUserIDExplorationsExplorationIDHandlerFunc(mockHandler.Exploration)

	api.GetSourcesIDUsersHandler = operations.GetSourcesIDUsersHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDUsersParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesIDUsers has not yet been implemented")
	})
	api.GetSourcesIDUsersUserIDHandler = operations.GetSourcesIDUsersUserIDHandlerFunc(func(ctx context.Context, params operations.GetSourcesIDUsersUserIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .GetSourcesIDUsersUserID has not yet been implemented")
	})

	api.GetSourcesIDUsersUserIDExplorationsHandler = operations.GetSourcesIDUsersUserIDExplorationsHandlerFunc(mockHandler.Explorations)
	api.PatchSourcesIDHandler = operations.PatchSourcesIDHandlerFunc(func(ctx context.Context, params operations.PatchSourcesIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .PatchSourcesID has not yet been implemented")
	})
	api.PatchSourcesIDRolesRoleIDHandler = operations.PatchSourcesIDRolesRoleIDHandlerFunc(func(ctx context.Context, params operations.PatchSourcesIDRolesRoleIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .PatchSourcesIDRolesRoleID has not yet been implemented")
	})
	api.PatchSourcesIDUsersUserIDExplorationsExplorationIDHandler = operations.PatchSourcesIDUsersUserIDExplorationsExplorationIDHandlerFunc(func(ctx context.Context, params operations.PatchSourcesIDUsersUserIDExplorationsExplorationIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .PatchSourcesIDUsersUserIDExplorationsExplorationID has not yet been implemented")
	})
	api.PatchSourcesIDUsersUserIDHandler = operations.PatchSourcesIDUsersUserIDHandlerFunc(func(ctx context.Context, params operations.PatchSourcesIDUsersUserIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .PatchSourcesIDUsersUserID has not yet been implemented")
	})
	api.PostDashboardsHandler = operations.PostDashboardsHandlerFunc(func(ctx context.Context, params operations.PostDashboardsParams) middleware.Responder {
		return middleware.NotImplemented("operation .PostDashboards has not yet been implemented")
	})
	api.PostSourcesHandler = operations.PostSourcesHandlerFunc(func(ctx context.Context, params operations.PostSourcesParams) middleware.Responder {
		return middleware.NotImplemented("operation .PostSources has not yet been implemented")
	})

	api.PostSourcesIDProxyHandler = operations.PostSourcesIDProxyHandlerFunc(mockHandler.Proxy)

	api.PostSourcesIDRolesHandler = operations.PostSourcesIDRolesHandlerFunc(func(ctx context.Context, params operations.PostSourcesIDRolesParams) middleware.Responder {
		return middleware.NotImplemented("operation .PostSourcesIDRoles has not yet been implemented")
	})
	api.PostSourcesIDUsersHandler = operations.PostSourcesIDUsersHandlerFunc(func(ctx context.Context, params operations.PostSourcesIDUsersParams) middleware.Responder {
		return middleware.NotImplemented("operation .PostSourcesIDUsers has not yet been implemented")
	})
	api.PostSourcesIDUsersUserIDExplorationsHandler = operations.PostSourcesIDUsersUserIDExplorationsHandlerFunc(func(ctx context.Context, params operations.PostSourcesIDUsersUserIDExplorationsParams) middleware.Responder {
		return middleware.NotImplemented("operation .PostSourcesIDUsersUserIDExplorations has not yet been implemented")
	})
	api.PutDashboardsIDHandler = operations.PutDashboardsIDHandlerFunc(func(ctx context.Context, params operations.PutDashboardsIDParams) middleware.Responder {
		return middleware.NotImplemented("operation .PutDashboardsID has not yet been implemented")
	})

	api.GetSourcesIDMonitoredHandler = operations.GetSourcesIDMonitoredHandlerFunc(mockHandler.MonitoredServices)

	api.ServerShutdown = func() {}

	handler := setupGlobalMiddleware(api.Serve(setupMiddlewares))
	return handler
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s\n", r.RemoteAddr, r.Method, r.URL)
		if strings.Contains(r.URL.Path, "/chronograf/v1") {
			handler.ServeHTTP(w, r)
			return
		} else if r.URL.Path == "//" {
			http.Redirect(w, r, "/index.html", http.StatusFound)
		} else {
			assets().Handler().ServeHTTP(w, r)
			return
		}
	})
}
