package http

import (
	"errors"
	"net/http"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
	"github.com/julienschmidt/httprouter"
)

type TranspilerQueryHandler struct {
	*httprouter.Router

	QueryService query.QueryService
}

// NewQueryHandler returns a new instance of QueryHandler.
func NewTranspilerQueryHandler() *TranspilerQueryHandler {
	h := &TranspilerQueryHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v1/transpiler/query", h.handlePostQuery)
	h.HandlerFunc("POST", "/query", h.handlePostInfluxQL)
	return h
}

// handlePostQuery is an HTTP handler that transpiles a query for the specified language.
func (h *TranspilerQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queryStr := r.FormValue("q")
	if queryStr == "" {
		kerrors.EncodeHTTP(ctx, errors.New("must pass query string in q parameter"), w)
		return
	}

	// TODO(nathanielc): Create routes that mimic the API of the source languages instead of explicitly defining the language.
	lang := r.FormValue("lang")
	if lang == "" {
		kerrors.EncodeHTTP(ctx, errors.New("must pass language in lang parameter"), w)
		return
	}

	var orgID platform.ID
	err := orgID.DecodeFromString(r.FormValue("orgID"))
	if err != nil {
		kerrors.EncodeHTTP(ctx, errors.New("must pass organization ID as string in orgID parameter"), w)
		return
	}

	var ce crossExecute
	switch lang {
	case "influxql":
		fallthrough
	default:
		ce = influxqlCE
	}

	results, err := query.QueryWithTranspile(ctx, orgID, queryStr, h.QueryService, ce.transpiler)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	err = encodeResult(w, results, ce.contentType, ce.encoder)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}
}

// handlePostInfluxQL handles query requests mirroring the 1.x influxdb API.
func (h *TranspilerQueryHandler) handlePostInfluxQL(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queryStr := r.FormValue("q")
	if queryStr == "" {
		kerrors.EncodeHTTP(ctx, errors.New("must pass query string in q parameter"), w)
		return
	}

	var orgID platform.ID
	err := orgID.DecodeFromString(r.FormValue("orgID"))
	if err != nil {
		kerrors.EncodeHTTP(ctx, errors.New("must pass organization ID as string in orgID parameter"), w)
		return
	}

	//TODO(nathanielc): Get database and rp information if needed.

	ce := influxqlCE

	results, err := query.QueryWithTranspile(ctx, orgID, queryStr, h.QueryService, ce.transpiler)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	err = encodeResult(w, results, ce.contentType, ce.encoder)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}
}

// crossExecute contains the components needed to execute a transpiled query and encode results
type crossExecute struct {
	transpiler  query.Transpiler
	encoder     query.MultiResultEncoder
	contentType string
}

var influxqlCE = crossExecute{
	transpiler:  influxql.NewTranspiler(),
	encoder:     influxql.NewMultiResultEncoder(),
	contentType: "application/json",
}

func encodeResult(w http.ResponseWriter, results query.ResultIterator, contentType string, encoder query.MultiResultEncoder) error {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	return encoder.Encode(w, results)
}
