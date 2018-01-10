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
	Users         string                   `json:"users"`            // Location of the users endpoint
	RawUsers      string                   `json:"rawUsers"`         // Location of the raw users endpoint
	Organizations string                   `json:"organizations"`    // Location of the organizations endpoint
	Mappings      string                   `json:"mappings"`         // Location of the application mappings endpoint
	Sources       string                   `json:"sources"`          // Location of the sources endpoint
	Me            string                   `json:"me"`               // Location of the me endpoint
	Environment   string                   `json:"environment"`      // Location of the environement endpoint
	Dashboards    string                   `json:"dashboards"`       // Location of the dashboards endpoint
	Config        getConfigLinksResponse   `json:"config"`           // Location of the config endpoint and its various sections
	Auth          []AuthRoute              `json:"auth"`             // Location of all auth routes.
	Logout        *string                  `json:"logout,omitempty"` // Location of the logout route for all auth routes
	ExternalLinks getExternalLinksResponse `json:"external"`         // All external links for the client to use
}

// AllRoutes is a handler that returns all links to resources in Chronograf server, as well as
// external links for the client to know about, such as for JSON feeds or custom side nav buttons.
// Optionally, routes for authentication can be returned.
type AllRoutes struct {
	AuthRoutes  []AuthRoute       // Location of all auth routes. If no auth, this can be empty.
	LogoutLink  string            // Location of the logout route for all auth routes. If no auth, this can be empty.
	StatusFeed  string            // External link to the JSON Feed for the News Feed on the client's Status Page
	CustomLinks map[string]string // Custom external links for client's User menu, as passed in via CLI/ENV
	Logger      chronograf.Logger
}

// ServeHTTP returns all top level routes and external links within chronograf
func (a *AllRoutes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	customLinks, err := NewCustomLinks(a.CustomLinks)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), a.Logger)
		return
	}

	routes := getRoutesResponse{
		Sources:       "/chronograf/v1/sources",
		Layouts:       "/chronograf/v1/layouts",
		Users:         "/chronograf/v1/users",
		RawUsers:      "/chronograf/v1/users?raw=true",
		Organizations: "/chronograf/v1/organizations",
		Me:            "/chronograf/v1/me",
		Environment:   "/chronograf/v1/env",
		Mappings:      "/chronograf/v1/mappings",
		Dashboards:    "/chronograf/v1/dashboards",
		Config: getConfigLinksResponse{
			Self: "/chronograf/v1/config",
			Auth: "/chronograf/v1/config/auth",
		},
		Auth: make([]AuthRoute, len(a.AuthRoutes)), // We want to return at least an empty array, rather than null
		ExternalLinks: getExternalLinksResponse{
			StatusFeed:  &a.StatusFeed,
			CustomLinks: customLinks,
		},
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
