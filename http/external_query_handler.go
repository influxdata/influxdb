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
	request := struct {
		Spec    *query.Spec `json:"spec"`
		Query   string      `json:"query"`
		Type    string      `json:"type"`
		Dialect struct {
			Header         *bool    `json:"header"`
			Delimiter      string   `json:"delimiter"`
			CommentPrefix  string   `json:"commentPrefix"`
			DateTimeFormat string   `json:"dateTimeFormat"`
			Annotations    []string `json:"annotations"`
		}
	}{}

	switch r.Header.Get("Content-Type") {
	case "application/json":
		orgName := r.URL.Query().Get("organization")
		orgID := r.URL.Query().Get("organizationID")
		filter := platform.OrganizationFilter{}
		if orgID != "" {
			var id platform.ID
			err := id.DecodeFromString(orgID)
			if err != nil {
				return err
			}
			filter.ID = &id
		}
		if orgName != "" {
			filter.Name = &orgName
		}

		o, err := orgSvc.FindOrganization(r.Context(), filter)
		if err != nil {
			return err
		}

		req.Request.OrganizationID = o.ID
		err = json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			return err
		}

		// Set defaults
		if request.Type == "" {
			request.Type = "flux"
		}

		if request.Dialect.Header == nil {
			header := true
			request.Dialect.Header = &header
		}
		if request.Dialect.Delimiter == "" {
			request.Dialect.Delimiter = ","
		}
		if request.Dialect.DateTimeFormat == "" {
			request.Dialect.DateTimeFormat = "RFC3339"
		}

		if request.Type != "flux" {
			return fmt.Errorf(`unknown query type: %s`, request.Type)
		}
		if len(request.Dialect.CommentPrefix) > 1 {
			return fmt.Errorf("invalid dialect comment prefix: must be length 0 or 1")
		}
		if len(request.Dialect.Delimiter) != 1 {
			return fmt.Errorf("invalid dialect delimeter: must be length  1")
		}
		for _, a := range request.Dialect.Annotations {
			switch a {
			case "group", "datatype", "default":
			default:
				return fmt.Errorf(`unknown dialect annotation type: %s`, a)
			}
		}

		switch request.Dialect.DateTimeFormat {
		case "RFC3339", "RFC3339Nano":
		default:
			return fmt.Errorf(`unknown dialect date time format: %s`, request.Dialect.DateTimeFormat)
		}

		if request.Query != "" {
			req.Request.Compiler = query.FluxCompiler{Query: request.Query}
		} else if request.Spec != nil {
			req.Request.Compiler = query.SpecCompiler{
				Spec: request.Spec,
			}
		} else {
			return errors.New(`request body requires either spec or query`)
		}
	default:
		orgName := r.FormValue("organization")
		if orgName == "" {
			return errors.New(`missing the "organization" parameter`)
		}
		o, err := orgSvc.FindOrganization(r.Context(), platform.OrganizationFilter{Name: &orgName})
		if err != nil {
			return err
		}
		req.Request.OrganizationID = o.ID
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
		noHeader := false
		if dialect.Header != nil {
			noHeader = !*dialect.Header
		}
		// TODO(nathanielc): Use commentPrefix and dateTimeFormat
		// once they are supported.
		config := csv.ResultEncoderConfig{
			NoHeader:    noHeader,
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
