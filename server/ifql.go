package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/ifql"
	"github.com/influxdata/ifql/parser"
)

type Params map[string]string

// SuggestionsResponse provides a list of available IFQL functions
type SuggestionsResponse struct {
	Functions []SuggestionResponse `json:"funcs"`
}

// SuggestionResponse provides the parameters available for a given IFQL function
type SuggestionResponse struct {
	Name   string `json:"name"`
	Params Params `json:"params"`
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
	var functions []SuggestionResponse
	for _, name := range names {
		suggestion, err := completer.FunctionSuggestion(name)
		if err != nil {
			Error(w, http.StatusNotFound, err.Error(), s.Logger)
			return
		}

		filteredParams := make(Params)
		for key, value := range suggestion.Params {
			if key == "table" {
				continue
			}

			filteredParams[key] = value
		}

		functions = append(functions, SuggestionResponse{
			Name:   name,
			Params: filteredParams,
		})
	}
	res := SuggestionsResponse{Functions: functions}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// IFQLSuggestion returns the function parameters for the requested function
func (s *Service) IFQLSuggestion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := httprouter.GetParamFromContext(ctx, "name")
	completer := ifql.DefaultCompleter()

	suggestion, err := completer.FunctionSuggestion(name)
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
	}

	encodeJSON(w, http.StatusOK, SuggestionResponse{Name: name, Params: suggestion.Params}, s.Logger)
}

type ASTRequest struct {
	Body string `json:"body"`
}

func (s *Service) IFQLAST(w http.ResponseWriter, r *http.Request) {
	var request ASTRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		invalidJSON(w, s.Logger)
	}

	ast, err := parser.NewAST(request.Body)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), s.Logger)
	}

	encodeJSON(w, http.StatusOK, ast, s.Logger)
}
