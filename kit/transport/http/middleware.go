package http

import (
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/tracing"
	ua "github.com/mileusna/useragent"
	"github.com/prometheus/client_golang/prometheus"
)

// Middleware constructor.
type Middleware func(http.Handler) http.Handler

func SetCORS(next http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, User-Agent")
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
					"handler":    name,
					"method":     r.Method,
					"path":       normalizePath(r.URL.Path),
					"status":     statusW.StatusCodeClass(),
					"user_agent": UserAgent(r),
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
		if len(piece) == influxdb.IDLength {
			if _, err := influxdb.IDFromString(head); err == nil {
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
