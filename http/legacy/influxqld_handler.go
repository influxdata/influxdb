package legacy

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	traceIDHeader = "Trace-Id"
)

func (h *InfluxqlHandler) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		h.Metrics.Requests,
		h.Metrics.RequestsLatency,
	}
}

// HandleQuery mimics the influxdb 1.0 /query
func (h *InfluxqlHandler) handleInfluxqldQuery(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "handleInfluxqldQuery")
	defer span.Finish()

	if id, _, found := tracing.InfoFromSpan(span); found {
		w.Header().Set(traceIDHeader, id)
	}

	ctx := r.Context()
	defer r.Body.Close()

	auth, err := getAuthorization(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if !auth.IsActive() {
		h.HandleHTTPError(ctx, &errors.Error{
			Code: errors.EForbidden,
			Msg:  "insufficient permissions",
		}, w)
		return
	}

	o, err := h.OrganizationService.FindOrganization(ctx, influxdb.OrganizationFilter{
		ID: &auth.OrgID,
	})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	var query string
	// Attempt to read the form value from the "q" form value.
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		query = qp
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			d, err := os.ReadFile(fhs[0].Filename)
			if err != nil {
				h.HandleHTTPError(ctx, err, w)
				return
			}
			query = string(d)
		}
	} else {
		ct := r.Header.Get("Content-Type")
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil {
			h.HandleHTTPError(ctx, &errors.Error{
				Code: errors.EInvalid,
				Err:  err,
			}, w)
			return
		}

		if mt == "application/vnd.influxql" {
			if d, err := io.ReadAll(r.Body); err != nil {
				h.HandleHTTPError(ctx, err, w)
				return
			} else {
				query = string(d)
			}
		}
	}

	// parse the parameters
	rawParams := r.FormValue("params")
	var params map[string]interface{}
	if rawParams != "" {
		decoder := json.NewDecoder(strings.NewReader(rawParams))
		decoder.UseNumber()
		if err := decoder.Decode(&params); err != nil {
			h.HandleHTTPError(ctx, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "error parsing query parameters",
				Err:  err,
			}, w)
			return
		}

		// Convert json.Number into int64 and float64 values
		for k, v := range params {
			if v, ok := v.(json.Number); ok {
				var err error
				if strings.Contains(string(v), ".") {
					params[k], err = v.Float64()
				} else {
					params[k], err = v.Int64()
				}

				if err != nil {
					h.HandleHTTPError(ctx, &errors.Error{
						Code: errors.EInvalid,
						Msg:  "error parsing json value",
						Err:  err,
					}, w)
					return
				}
			}
		}
	}

	// Parse chunk size. Use default if not provided or cannot be parsed
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	formatString := r.Header.Get("Accept")
	encodingFormat := influxql.EncodingFormatFromMimeType(formatString)
	w.Header().Set("Content-Type", encodingFormat.ContentType())

	req := &influxql.QueryRequest{
		DB:             r.FormValue("db"),
		RP:             r.FormValue("rp"),
		Epoch:          r.FormValue("epoch"),
		EncodingFormat: encodingFormat,
		OrganizationID: o.ID,
		Query:          query,
		Params:         params,
		Source:         r.Header.Get("User-Agent"),
		Authorization:  auth,
		Chunked:        chunked,
		ChunkSize:      chunkSize,
	}

	var respSize int64
	cw := iocounter.Writer{Writer: w}
	_, err = h.InfluxqldQueryService.Query(ctx, &cw, req)
	respSize = cw.Count()

	if err != nil {
		if respSize == 0 {
			// Only record the error headers IFF nothing has been written to w.
			h.HandleHTTPError(ctx, err, w)
			return
		}
		h.Logger.Info("error writing response to client",
			zap.String("org", o.Name),
			zap.String("handler", "influxql"),
			zap.Error(err),
		)
	}
}
