package server

import (
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/ifql"
)

// SuggestionsResponse provides a list of available IFQL functions
type SuggestionsResponse struct {
	Functions []string `json:"funcs"`
}

// SuggestionResponse provides the parameters available for a given IFQL function
type SuggestionResponse struct {
	Params map[string]string `json:"params"`
}

type ifqlLinks struct {
	Self        string `json:"self"`        // Self link mapping to this resource
	Suggestions string `json:"suggestions"` // URL for ifql builder function suggestions
}

type ifqlResponse struct {
	Links ifqlLinks `json:"links"`
}

// IFQL returns a list of links for the IFQL API
func (s *Service) IFQL(w http.ResponseWriter, r *http.Request) {
	httpAPIIFQL := "/chronograf/v1/ifql"
	res := ifqlResponse{
		Links: ifqlLinks{
			Self:        fmt.Sprintf("%s", httpAPIIFQL),
			Suggestions: fmt.Sprintf("%s/suggestions", httpAPIIFQL),
		},
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// IFQLSuggestions returns a list of available IFQL functions for the IFQL Builder
func (s *Service) IFQLSuggestions(w http.ResponseWriter, r *http.Request) {
	completer := ifql.DefaultCompleter()
	names := completer.FunctionNames()
	res := SuggestionsResponse{Functions: names}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// IFQLSuggestion returns the function parameters for the requested function
func (s *Service) IFQLSuggestion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := httprouter.GetParamFromContext(ctx, "name")
	completer := ifql.DefaultCompleter()

	res, err := completer.FunctionSuggestion(name)
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
	}

	encodeJSON(w, http.StatusOK, SuggestionResponse(res), s.Logger)
}
