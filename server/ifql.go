package server

import (
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
