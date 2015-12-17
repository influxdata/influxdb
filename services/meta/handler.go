package meta

import (
	"compress/gzip"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdb/influxdb/uuid"
)

//type store interface {
//	AfterIndex(index int) <-chan struct{}
//	Database(name string) (*DatabaseInfo, error)
//}

// handler represents an HTTP handler for the meta service.
type handler struct {
	config  *Config
	Version string

	logger         *log.Logger
	loggingEnabled bool // Log every HTTP access.
	pprofEnabled   bool
	store          interface {
		AfterIndex(index int) <-chan struct{}
		Snapshot() ([]byte, error)
		SetCache(b []byte)
	}
}

// newHandler returns a new instance of handler with routes.
func newHandler(c *Config) *handler {
	h := &handler{
		config:         c,
		logger:         log.New(os.Stderr, "[meta-http] ", log.LstdFlags),
		loggingEnabled: c.LoggingEnabled,
	}

	return h
}

// SetRoutes sets the provided routes on the handler.
func (h *handler) WrapHandler(name string, hf http.HandlerFunc) http.Handler {
	var handler http.Handler
	handler = http.HandlerFunc(hf)
	handler = gzipFilter(handler)
	handler = versionHeader(handler, h)
	handler = cors(handler)
	handler = requestID(handler)
	if h.loggingEnabled {
		handler = logging(handler, name, h.logger)
	}
	handler = recovery(handler, name, h.logger) // make sure recovery is always last

	return handler
}

// ServeHTTP responds to HTTP request to the handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "HEAD":
		h.WrapHandler("ping", h.servePing).ServeHTTP(w, r)
	case "GET":
		h.WrapHandler("snapshot", h.serveSnapshot).ServeHTTP(w, r)
	case "POST":
		h.WrapHandler("execute", h.serveExec).ServeHTTP(w, r)
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
	}
}

// serveExec executes the requested command.
func (h *handler) serveExec(w http.ResponseWriter, r *http.Request) {
}

// serveSnapshot is a long polling http connection to server cache updates
func (h *handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	// get the current index that client has
	index, _ := strconv.Atoi(r.URL.Query().Get("index"))

	select {
	case <-h.store.AfterIndex(index):
		// Send updated snapshot to client.
		ss, err := h.store.Snapshot()
		if err != nil {
			h.logger.Println(err)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}
		w.Write(ss)
	case <-w.(http.CloseNotifier).CloseNotify():
		// Client closed the connection so we're done.
		return
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *handler) servePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ACK"))
}

// serveExpvar serves registered expvar information over HTTP.
func serveExpvar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintf(w, "{\n")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w gzipResponseWriter) Flush() {
	w.Writer.(*gzip.Writer).Flush()
}

func (w gzipResponseWriter) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

// determines if the client can accept compressed responses, and encodes accordingly
func gzipFilter(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			inner.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		inner.ServeHTTP(gzw, r)
	})
}

// versionHeader takes a HTTP handler and returns a HTTP handler
// and adds the X-INFLUXBD-VERSION header to outgoing responses.
func versionHeader(inner http.Handler, h *handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("X-InfluxDB-Version", h.Version)
		inner.ServeHTTP(w, r)
	})
}

// cors responds to incoming requests and adds the appropriate cors headers
// TODO: corylanou: add the ability to configure this in our config
func cors(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))

			w.Header().Set(`Access-Control-Expose-Headers`, strings.Join([]string{
				`Date`,
				`X-Influxdb-Version`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func requestID(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		uid := uuid.TimeUUID()
		r.Header.Set("Request-Id", uid.String())
		w.Header().Set("Request-Id", r.Header.Get("Request-Id"))

		inner.ServeHTTP(w, r)
	})
}

func logging(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}
		inner.ServeHTTP(l, r)
		logLine := buildLogLine(l, r, start)
		weblog.Println(logLine)
	})
}

func recovery(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		l := &responseLogger{w: w}

		defer func() {
			if err := recover(); err != nil {
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf(`%s [panic:%s]`, logLine, err)
				weblog.Println(logLine)
			}
		}()

		inner.ServeHTTP(l, r)
	})
}
