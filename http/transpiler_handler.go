package http

import (
	"errors"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

type TranspilerQueryHandler struct {
	*httprouter.Router

	Logger       *zap.Logger
	QueryService query.QueryService
	OrgID        platform.ID
}

// NewQueryHandler returns a new instance of QueryHandler.
func NewTranspilerQueryHandler(orgID platform.ID) *TranspilerQueryHandler {
	h := &TranspilerQueryHandler{
		OrgID:  orgID,
		Router: httprouter.New(),
		Logger: zap.NewNop(),
	}

	h.HandlerFunc("GET", "/ping", h.servePing)
	h.HandlerFunc("POST", "/query", h.handlePostQuery)
	return h
}

// servePing returns a simple response to let the client know the server is running.
// This handler is only available for 1.x compatibility and is not part of the 2.0 platform.
func (h *TranspilerQueryHandler) servePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNoContent)
}

// handlePostInfluxQL handles query requests mirroring the 1.x influxdb API.
func (h *TranspilerQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queryStr := r.FormValue("q")
	if queryStr == "" {
		EncodeError(ctx, errors.New("must pass query string in q parameter"), w)
		return
	}

	// Create a new transpiler from the http request.
	ce := influxqlCE
	transpiler := ce.transpiler(r)

	// Run the transpiler against the query service.
	results, err := query.QueryWithTranspile(ctx, h.OrgID, queryStr, h.QueryService, transpiler)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	// Once we have reached this stage, it is the encoder's job to encode any
	// errors in the encoded format.
	if err := encodeResult(w, results, ce.contentType, ce.encoder); err != nil {
		h.Logger.Info("Unable to encode result", zap.Error(err))
		return
	}
}

// crossExecute contains the components needed to execute a transpiled query and encode results.
type crossExecute struct {
	transpiler  func(req *http.Request) query.Transpiler
	encoder     query.MultiResultEncoder
	contentType string
}

var influxqlCE = crossExecute{
	transpiler: func(req *http.Request) query.Transpiler {
		config := influxql.Config{
			DefaultDatabase:        req.FormValue("db"),
			DefaultRetentionPolicy: req.FormValue("rp"),
		}
		return influxql.NewTranspilerWithConfig(config)
	},
	encoder:     influxql.NewMultiResultEncoder(),
	contentType: "application/json",
}

func encodeResult(w http.ResponseWriter, results query.ResultIterator, contentType string, encoder query.MultiResultEncoder) error {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	_, err := encoder.Encode(w, results)
	return err
}
