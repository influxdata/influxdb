package http

import (
	"encoding/json"
	"net/http"

	"github.com/influxdata/platform/query/complete"
	"github.com/influxdata/platform/query/parser"
	"github.com/julienschmidt/httprouter"
)

// FluxLangHandler represents an HTTP API handler for buckets.
type FluxLangHandler struct {
	*httprouter.Router
}

type astRequest struct {
	Body string `json:"body"`
}

// NewFluxLangHandler returns a new instance of FluxLangHandler.
func NewFluxLangHandler() *FluxLangHandler {
	h := &FluxLangHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("GET", "/v2/flux", h.getFlux)
	h.HandlerFunc("POST", "/v2/flux/ast", h.postFluxAST)
	h.HandlerFunc("GET", "/v2/flux/suggestions", h.getFluxSuggestions)
	h.HandlerFunc("GET", "/v2/flux/suggestions/:name", h.getFluxSuggestion)
	return h
}

// fluxParams contain flux funciton parameters as defined by the semantic graph
type fluxParams map[string]string

// suggestionsResponse provides a list of available Flux functions
type suggestionsResponse struct {
	Functions []suggestionResponse `json:"funcs"`
}

// suggestionResponse provides the parameters available for a given Flux function
type suggestionResponse struct {
	Name   string     `json:"name"`
	Params fluxParams `json:"params"`
}

type fluxLinks struct {
	Self        string `json:"self"`        // Self link mapping to this resource
	Suggestions string `json:"suggestions"` // URL for flux builder function suggestions
	AST         string `json:"ast"`         // URL for flux ast
}

type fluxResponse struct {
	Links fluxLinks `json:"links"`
}

var getFluxResponse = fluxResponse{
	Links: fluxLinks{
		Self:        "/v2/flux",
		AST:         "/v2/flux/ast",
		Suggestions: "/v2/flux/suggestions",
	},
}

// getFlux returns a list of links for the Flux API
func (h *FluxLangHandler) getFlux(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := encodeResponse(ctx, w, http.StatusOK, getFluxResponse); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// postFluxAST returns a flux AST for provided flux string
func (h *FluxLangHandler) postFluxAST(w http.ResponseWriter, r *http.Request) {
	var request astRequest
	ctx := r.Context()

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	ast, err := parser.NewAST(request.Body)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, ast); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// getFluxSuggestions returns a list of available Flux functions for the Flux Builder
func (h *FluxLangHandler) getFluxSuggestions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	completer := complete.DefaultCompleter()
	names := completer.FunctionNames()
	var functions []suggestionResponse
	for _, name := range names {
		suggestion, err := completer.FunctionSuggestion(name)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		filteredParams := make(fluxParams)
		for key, value := range suggestion.Params {
			if key == "table" {
				continue
			}

			filteredParams[key] = value
		}

		functions = append(functions, suggestionResponse{
			Name:   name,
			Params: filteredParams,
		})
	}
	res := suggestionsResponse{Functions: functions}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

// getFluxSuggestion returns the function parameters for the requested function
func (h *FluxLangHandler) getFluxSuggestion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := httprouter.ParamsFromContext(ctx).ByName("name")
	completer := complete.DefaultCompleter()

	suggestion, err := completer.FunctionSuggestion(name)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	res := suggestionResponse{Name: name, Params: suggestion.Params}
	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}
