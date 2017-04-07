package server

import (
	"net/http"

	"github.com/influxdata/chronograf"
)

// AuthRoute are the routes for each type of OAuth2 provider
type AuthRoute struct {
	Name     string `json:"name"`     // Name uniquely identifies the provider
	Label    string `json:"label"`    // Label is a user-facing string to present in the UI
	Login    string `json:"login"`    // Login is the route to the login redirect path
	Logout   string `json:"logout"`   // Logout is the route to the logout redirect path
	Callback string `json:"callback"` // Callback is the route the provider calls to exchange the code/state
}

// AuthRoutes contains all OAuth2 provider routes.
type AuthRoutes []AuthRoute

// Lookup searches all the routes for a specific provider
func (r *AuthRoutes) Lookup(provider string) (AuthRoute, bool) {
	for _, route := range *r {
		if route.Name == provider {
			return route, true
		}
	}
	return AuthRoute{}, false
}

type getRoutesResponse struct {
	Layouts    string      `json:"layouts"`    // Location of the layouts endpoint
	Mappings   string      `json:"mappings"`   // Location of the application mappings endpoint
	Sources    string      `json:"sources"`    // Location of the sources endpoint
	Me         string      `json:"me"`         // Location of the me endpoint
	Dashboards string      `json:"dashboards"` // Location of the dashboards endpoint
	Auth       []AuthRoute `json:"auth"`       // Location of all auth routes.
}

// AllRoutes returns all top level routes within chronograf
func AllRoutes(authRoutes []AuthRoute, logger chronograf.Logger) http.HandlerFunc {
	routes := getRoutesResponse{
		Sources:    "/chronograf/v1/sources",
		Layouts:    "/chronograf/v1/layouts",
		Me:         "/chronograf/v1/me",
		Mappings:   "/chronograf/v1/mappings",
		Dashboards: "/chronograf/v1/dashboards",
		Auth:       make([]AuthRoute, len(authRoutes)),
	}

	for i, route := range authRoutes {
		routes.Auth[i] = route
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encodeJSON(w, http.StatusOK, routes, logger)
		return
	})
}
