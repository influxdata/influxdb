package http

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
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
				statusCode := statusW.Code()
				// only log metrics for 2XX or 5XX requests
				if !reportFromCode(statusCode) {
					return
				}

				label := prometheus.Labels{
					"handler":       name,
					"method":        r.Method,
					"path":          normalizePath(r.URL.Path),
					"status":        statusW.StatusCodeClass(),
					"response_code": fmt.Sprintf("%d", statusCode),
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

// Constants used for normalizing paths
const (
	fileSlug  = ":file_name"
	shardSlug = ":shard_id"
)

func normalizePath(p string) string {
	// Normalize any paths used during backup or restore processes
	p = normalizeBackupAndRestore(p)

	// Go through each part of the path and normalize IDs and UI assets
	var parts []string
	for head, tail := shiftPath(p); ; head, tail = shiftPath(tail) {
		piece := head

		// Normalize any ID's in the path as the ":id" slug
		if len(piece) == platform.IDLength {
			if _, err := platform.IDFromString(head); err == nil {
				piece = ":id"
			}
		}
		parts = append(parts, piece)

		if tail == "/" {
			// Normalize UI asset file names. The UI asset file is the last part of the path.
			parts[len(parts)-1] = normalizeAssetFile(parts[len(parts)-1])
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

// Normalize the file name for a UI asset
// For example: 838442d56d.svg will return as :file_id.svg
// Files names that do not have one of the listed extensions will be returned unchanged
func normalizeAssetFile(f string) string {
	exts := []string{
		".js",
		".svg",
		".woff2",
		".wasm",
		".map",
		".LICENSE",
		".ttf",
		".woff",
		".eot",
	}

	for _, ext := range exts {
		if strings.HasSuffix(f, ext) {
			return fileSlug + ext
		}
	}

	return f
}

// Normalize paths used during the backup and restore process.
// Paths not matching any of the patterns will be returned unchanged.
func normalizeBackupAndRestore(pth string) string {
	patterns := map[string]string{
		`restore/shards/\d+`: path.Join("restore/shards", shardSlug),
		`backup/shards/\d+`:  path.Join("backup/shards", shardSlug),
	}

	for p, s := range patterns {
		re := regexp.MustCompile(p)
		if re.MatchString(pth) {
			return re.ReplaceAllString(pth, s)
		}
	}

	return pth
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

// reportFromCode is a helper function to determine if telemetry data should be
// reported for this response.
func reportFromCode(c int) bool {
	return (c >= 200 && c <= 299) || (c >= 500 && c <= 599)
}
