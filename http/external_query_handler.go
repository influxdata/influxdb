package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"unicode/utf8"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/csv"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// ExternalQueryHandler implements the /query API endpoint defined in the swagger doc.
// This only implements the POST method and only supports Spec or Flux queries.
type ExternalQueryHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	ProxyQueryService   query.ProxyQueryService
	OrganizationService platform.OrganizationService
}

// NewExternalQueryHandler returns a new instance of QueryHandler.
func NewExternalQueryHandler() *ExternalQueryHandler {
	h := &ExternalQueryHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/query", h.handlePostQuery)
	return h
}

func decodeQueryRequest(r *http.Request, req *query.ProxyRequest, orgSvc platform.OrganizationService) error {
	orgName := r.FormValue("organization")
	if orgName == "" {
		return errors.New(`missing the "organization" parameter`)
	}
	o, err := orgSvc.FindOrganization(r.Context(), platform.OrganizationFilter{Name: &orgName})
	if err != nil {
		return err
	}
	req.Request.OrganizationID = o.ID
	request := struct {
		Spec    *query.Spec `json:"spec"`
		Dialect struct {
			Header         bool     `json:"header"`
			Delimiter      string   `json:"delimiter"`
			CommentPrefix  string   `json:"commentPrefix"`
			DateTimeFormat string   `json:"dateTimeFormat"`
			Annotations    []string `json:"annotations"`
		}
	}{}
	// Set defaults
	request.Dialect.Header = true
	request.Dialect.Delimiter = ","
	// TODO(nathanielc): Set commentPrefix and dateTimeFormat defaults
	// once they are supported.

	switch r.Header.Get("Content-Type") {
	case "application/json":
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			return err
		}
		req.Request.Compiler = query.SpecCompiler{
			Spec: request.Spec,
		}
	default:
		q := r.FormValue("query")
		if q == "" {
			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				return err
			}
			q = string(data)
		}
		req.Request.Compiler = query.FluxCompiler{
			Query: q,
		}
	}

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		var delimiter rune
		dialect := request.Dialect
		if dialect.Delimiter != "" {
			delimiter, _ = utf8.DecodeRuneInString(dialect.Delimiter)
		}
		if dialect.CommentPrefix != "" {
			return errors.New("commentPrefix is not yet supported")
		}
		if dialect.DateTimeFormat != "" {
			return errors.New("dateTimeFormat is not yet supported")
		}
		config := csv.ResultEncoderConfig{
			NoHeader:    !dialect.Header,
			Delimiter:   delimiter,
			Annotations: dialect.Annotations,
		}
		req.Dialect = csv.Dialect{
			ResultEncoderConfig: config,
		}
	}
	return nil
}

func (h *ExternalQueryHandler) handlePostQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req query.ProxyRequest
	if err := decodeQueryRequest(r, &req, h.OrganizationService); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		EncodeError(ctx, fmt.Errorf("unsupported dialect over HTTP %T", req.Dialect), w)
		return
	}
	hd.SetHeaders(w)

	n, err := h.ProxyQueryService.Query(ctx, w, &req)
	if err != nil {
		if n == 0 {
			// Only record the error headers IFF nothing has been written to w.
			EncodeError(ctx, err, w)
			return
		}
		h.Logger.Info("Error writing response to client",
			zap.String("handler", "transpilerde"),
			zap.Error(err),
		)
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (h *ExternalQueryHandler) PrometheusCollectors() []prometheus.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}
