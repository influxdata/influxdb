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
	Layouts       string                   `json:"layouts"`          // Location of the layouts endpoint
	Mappings      string                   `json:"mappings"`         // Location of the application mappings endpoint
	Sources       string                   `json:"sources"`          // Location of the sources endpoint
	Me            string                   `json:"me"`               // Location of the me endpoint
	Dashboards    string                   `json:"dashboards"`       // Location of the dashboards endpoint
	Auth          []AuthRoute              `json:"auth"`             // Location of all auth routes.
	Logout        *string                  `json:"logout,omitempty"` // Location of the logout route for all auth routes
	ExternalLinks getExternalLinksResponse `json:"external"`         // All external links for the client to use
}

type getExternalLinksResponse struct {
	StatusFeed *string `json:"statusFeed,omitempty"` // Location of the a JSON Feed for client's Status page News Feed
}

// AllRoutes is a handler that returns all links to resources in Chronograf server, as well as
// external links for the client to know about, such as for JSON feeds or custom side nav buttons.
// Optionally, routes for authentication can be returned.
type AllRoutes struct {
	AuthRoutes    []AuthRoute              // Location of all auth routes. If no auth, this can be empty.
	LogoutLink    string                   // Location of the logout route for all auth routes. If no auth, this can be empty.
	ExternalLinks getExternalLinksResponse // All external links for the client to use
	Logger        chronograf.Logger
}

// ServeHTTP returns all top level routes within chronograf
func (a *AllRoutes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	routes := getRoutesResponse{
		Sources:       "/chronograf/v1/sources",
		Layouts:       "/chronograf/v1/layouts",
		Me:            "/chronograf/v1/me",
		Mappings:      "/chronograf/v1/mappings",
		Dashboards:    "/chronograf/v1/dashboards",
		Auth:          make([]AuthRoute, len(a.AuthRoutes)), // We want to return at least an empty array, rather than null
		ExternalLinks: a.ExternalLinks,
	}

	// The JSON response will have no field present for the LogoutLink if there is no logout link.
	if a.LogoutLink != "" {
		routes.Logout = &a.LogoutLink
	}

	for i, route := range a.AuthRoutes {
		routes.Auth[i] = route
	}

	encodeJSON(w, http.StatusOK, routes, a.Logger)
}
