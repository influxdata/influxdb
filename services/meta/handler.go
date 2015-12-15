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

// Handler represents an HTTP handler for the InfluxDB server.
type Handler struct {
	config  *Config
	Version string

	Logger         *log.Logger
	loggingEnabled bool // Log every HTTP access.
	pprofEnabled   bool
	store          *store
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(c *Config, s *store) *Handler {
	h := &Handler{
		config:         c,
		Logger:         log.New(os.Stderr, "[meta-http] ", log.LstdFlags),
		loggingEnabled: c.LoggingEnabled,
		store:          s,
	}

	return h
}

// SetRoutes sets the provided routes on the handler.
func (h *Handler) WrapHandler(name string, hf http.HandlerFunc) http.Handler {
	var handler http.Handler
	handler = http.HandlerFunc(hf)
	handler = gzipFilter(handler)
	handler = versionHeader(handler, h)
	handler = cors(handler)
	handler = requestID(handler)
	if h.loggingEnabled {
		handler = logging(handler, name, h.Logger)
	}
	handler = recovery(handler, name, h.Logger) // make sure recovery is always last

	return handler
}

// ServeHTTP responds to HTTP request to the handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "HEAD":
		h.WrapHandler("ping", h.servePing).ServeHTTP(w, r)
	case "GET":
		h.WrapHandler("cache", h.serveCache).ServeHTTP(w, r)
	case "POST":
		h.WrapHandler("execute", h.serveExec).ServeHTTP(w, r)
	default:
		http.Error(w, "bad request", http.StatusBadRequest)
	}
}

// serveExec executes the requested command.
func (h *Handler) serveExec(w http.ResponseWriter, r *http.Request) {
}

// serveCache is a long polling http connection to server cache updates
func (h *Handler) serveCache(w http.ResponseWriter, r *http.Request) {
	// get the current index that client has
	clientIndex, _ := strconv.Atoi(r.URL.Query().Get("index"))
	index, c := h.store.Snapshot()

	// If the client has an older index, send the updated cache
	if index > clientIndex {
		w.Header().Add("influxdb-meta-index", strconv.Itoa(index))
		w.Write(c)
		return
	}

	// Make sure if the client disconnects we signal the query to abort
	closing := make(chan struct{})
	if notifier, ok := w.(http.CloseNotifier); ok {
		notify := notifier.CloseNotify()
		go func() {
			<-notify
			close(closing)
		}()
	}

	// block until we get a data change, or an error which signals
	// scenarios like leader change, store closing, etc.
	h.store.WaitForDataChanged(closing)

	index, c = h.store.Snapshot()
	w.Header().Add("influxdb-meta-index", strconv.Itoa(index))
	w.Write(c)
	return
}

// servePing returns a simple response to let the client know the server is running.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
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
func versionHeader(inner http.Handler, h *Handler) http.Handler {
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
