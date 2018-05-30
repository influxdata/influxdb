package http

import (
	"errors"
	"net/http"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
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

	h.HandlerFunc("POST", "/query", h.handlePostQuery)
	return h
}

// handlePostInfluxQL handles query requests mirroring the 1.x influxdb API.
func (h *TranspilerQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queryStr := r.FormValue("q")
	if queryStr == "" {
		kerrors.EncodeHTTP(ctx, errors.New("must pass query string in q parameter"), w)
		return
	}

	//TODO(nathanielc): Get database and rp information if needed.

	ce := influxqlCE

	results, err := query.QueryWithTranspile(ctx, h.OrgID, queryStr, h.QueryService, ce.transpiler)
	if err != nil {
		kerrors.EncodeHTTP(ctx, err, w)
		return
	}

	// Once we have reached this stage, it is the encoder's job to encode any
	// errors in the encoded format.
	if err := encodeResult(w, results, ce.contentType, ce.encoder); err != nil {
		h.Logger.Info("Unable to encode result", zap.Error(err))
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
