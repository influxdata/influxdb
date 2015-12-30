package meta

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/influxdb/influxdb/services/meta/internal"
	"github.com/influxdb/influxdb/uuid"
)

// handler represents an HTTP handler for the meta service.
type handler struct {
	config  *Config
	Version string

	logger         *log.Logger
	loggingEnabled bool // Log every HTTP access.
	pprofEnabled   bool
	store          interface {
		afterIndex(index uint64) <-chan struct{}
		index() uint64
		leader() string
		leaderHTTP() string
		snapshot() (*Data, error)
		apply(b []byte) error
		join(n *NodeInfo) error
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
		http.Error(w, "", http.StatusBadRequest)
	}
}

// serveExec executes the requested command.
func (h *handler) serveExec(w http.ResponseWriter, r *http.Request) {
	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	fmt.Println("serveExec: ", r.Host)
	if r.URL.Path == "/join" {
		n := &NodeInfo{}
		if err := json.Unmarshal(body, n); err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		err := h.store.join(n)
		if err == raft.ErrNotLeader {
			l := h.store.leaderHTTP()
			fmt.Println("redirecting to leader:", l)
			if l == "" {
				// No cluster leader. Client will have to try again later.
				h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
				return
			}
			scheme := "http://"
			if h.config.HTTPSEnabled {
				scheme = "https://"
			}

			l = scheme + l + "/join"
			fmt.Println("FOO: ", l)
			http.Redirect(w, r, l, http.StatusTemporaryRedirect)
			return
		}

		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}

		return
	}

	// Make sure it's a valid command.
	if err := validateCommand(body); err != nil {
		h.httpError(err, w, http.StatusBadRequest)
		return
	}

	// Apply the command to the store.
	var resp *internal.Response
	if err := h.store.apply(body); err != nil {
		// If we aren't the leader, redirect client to the leader.
		if err == raft.ErrNotLeader {
			l := h.store.leaderHTTP()
			if l == "" {
				// No cluster leader. Client will have to try again later.
				h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
				return
			}
			scheme := "http://"
			if h.config.HTTPSEnabled {
				scheme = "https://"
			}

			l = scheme + l + "/execute"
			fmt.Println("FOO: ", l)
			http.Redirect(w, r, l, http.StatusTemporaryRedirect)
			return
		}

		// Error wasn't a leadership error so pass it back to client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Error: proto.String(err.Error()),
		}
	} else {
		// Apply was successful. Return the new store index to the client.
		resp = &internal.Response{
			OK:    proto.Bool(false),
			Index: proto.Uint64(h.store.index()),
		}
	}

	// Marshal the response.
	b, err := proto.Marshal(resp)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	// Send response to client.
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Write(b)
}

func validateCommand(b []byte) error {
	// Ensure command can be deserialized before applying.
	if err := proto.Unmarshal(b, &internal.Command{}); err != nil {
		return fmt.Errorf("unable to unmarshal command: %s", err)
	}

	return nil
}

// serveSnapshot is a long polling http connection to server cache updates
func (h *handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	// get the current index that client has
	index, err := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
	if err != nil {
		http.Error(w, "error parsing index", http.StatusBadRequest)
	}

	select {
	case <-h.store.afterIndex(index):
		// Send updated snapshot to client.
		ss, err := h.store.snapshot()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		b, err := ss.MarshalBinary()
		if err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}
		w.Write(b)
		return
	case <-w.(http.CloseNotifier).CloseNotify():
		// Client closed the connection so we're done.
		return
	}
}

// servePing returns a simple response to let the client know the server is running.
func (h *handler) servePing(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ACK"))
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

func (h *handler) httpError(err error, w http.ResponseWriter, status int) {
	if h.loggingEnabled {
		h.logger.Println(err)
	}
	http.Error(w, "", status)
}
