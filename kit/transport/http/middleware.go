package http

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/kit/platform"
	"github.com/influxdata/influxdb/kit/platform/errors"
	"github.com/influxdata/influxdb/kit/tracing"
	ua "github.com/mileusna/useragent"
	"github.com/prometheus/client_golang/prometheus"
)

// Middleware constructor.
type Middleware func(http.Handler) http.Handler

func SetCORS(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			// Access-Control-Allow-Origin must be present in every response
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		if r.Method == http.MethodOptions {
			// allow and stop processing in pre-flight requests
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, PATCH")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, User-Agent")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func Metrics(name string, reqMetric *prometheus.CounterVec, durMetric *prometheus.HistogramVec) Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			statusW := NewStatusResponseWriter(w)

			defer func(start time.Time) {
				label := prometheus.Labels{
					"handler":       name,
					"method":        r.Method,
					"path":          normalizePath(r.URL.Path),
					"status":        statusW.StatusCodeClass(),
					"response_code": fmt.Sprintf("%d", statusW.Code()),
					"user_agent":    UserAgent(r),
				}
				durMetric.With(label).Observe(time.Since(start).Seconds())
				reqMetric.With(label).Inc()
			}(time.Now())

			next.ServeHTTP(statusW, r)
		}
		return http.HandlerFunc(fn)
	}
}

func SkipOptions(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Preflight CORS requests from the browser will send an options request,
		// so we need to make sure we satisfy them
		if origin := r.Header.Get("Origin"); origin == "" && r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func Trace(name string) Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			span, r := tracing.ExtractFromHTTPRequest(r, name)
			defer span.Finish()

			span.LogKV("user_agent", UserAgent(r))
			for k, v := range r.Header {
				if len(v) == 0 {
					continue
				}

				if k == "Authorization" || k == "User-Agent" {
					continue
				}

				// If header has multiple values, only the first value will be logged on the trace.
				span.LogKV(k, v[0])
			}

			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}

func UserAgent(r *http.Request) string {
	header := r.Header.Get("User-Agent")
	if header == "" {
		return "unknown"
	}

	return ua.Parse(header).Name
}

func normalizePath(p string) string {
	var parts []string
	for head, tail := shiftPath(p); ; head, tail = shiftPath(tail) {
		piece := head
		if len(piece) == platform.IDLength {
			if _, err := platform.IDFromString(head); err == nil {
				piece = ":id"
			}
		}
		parts = append(parts, piece)
		if tail == "/" {
			break
		}
	}
	return "/" + path.Join(parts...)
}

func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}

type OrgContext string

const CtxOrgKey OrgContext = "orgID"

// ValidResource make sure a resource exists when a sub system needs to be mounted to an api
func ValidResource(api *API, lookupOrgByResourceID func(context.Context, platform.ID) (platform.ID, error)) Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			statusW := NewStatusResponseWriter(w)
			id, err := platform.IDFromString(chi.URLParam(r, "id"))
			if err != nil {
				api.Err(w, r, platform.ErrCorruptID(err))
				return
			}

			ctx := r.Context()

			orgID, err := lookupOrgByResourceID(ctx, *id)
			if err != nil {
				// if this function returns an error we will squash the error message and replace it with a not found error
				api.Err(w, r, &errors.Error{
					Code: errors.ENotFound,
					Msg:  "404 page not found",
				})
				return
			}

			// embed OrgID into context
			next.ServeHTTP(statusW, r.WithContext(context.WithValue(ctx, CtxOrgKey, orgID)))
		}
		return http.HandlerFunc(fn)
	}
}

// OrgIDFromContext ....
func OrgIDFromContext(ctx context.Context) *platform.ID {
	v := ctx.Value(CtxOrgKey)
	if v == nil {
		return nil
	}
	id := v.(platform.ID)
	return &id
}
