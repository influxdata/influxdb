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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/raft"
	"github.com/influxdata/influxdb/services/meta/internal"
	"github.com/influxdata/influxdb/uuid"
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
		otherMetaServersHTTP() []string
		peers() []string
	}
	s *Service

	mu      sync.RWMutex
	closing chan struct{}
	leases  *Leases
}

// newHandler returns a new instance of handler with routes.
func newHandler(c *Config, s *Service) *handler {
	h := &handler{
		s:              s,
		config:         c,
		logger:         log.New(os.Stderr, "[meta-http] ", log.LstdFlags),
		loggingEnabled: c.LoggingEnabled,
		closing:        make(chan struct{}),
		leases:         NewLeases(time.Duration(c.LeaseDuration)),
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
	case "GET":
		switch r.URL.Path {
		case "/ping":
			h.WrapHandler("ping", h.servePing).ServeHTTP(w, r)
		case "/lease":
			h.WrapHandler("lease", h.serveLease).ServeHTTP(w, r)
		case "/peers":
			h.WrapHandler("peers", h.servePeers).ServeHTTP(w, r)
		default:
			h.WrapHandler("snapshot", h.serveSnapshot).ServeHTTP(w, r)
		}
	case "POST":
		h.WrapHandler("execute", h.serveExec).ServeHTTP(w, r)
	default:
		http.Error(w, "", http.StatusBadRequest)
	}
}

func (h *handler) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-h.closing:
		// do nothing here
	default:
		close(h.closing)
	}
	return nil
}

func (h *handler) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	select {
	case <-h.closing:
		return true
	default:
		return false
	}
}

// serveExec executes the requested command.
func (h *handler) serveExec(w http.ResponseWriter, r *http.Request) {
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusServiceUnavailable)
		return
	}

	// Read the command from the request body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
		return
	}

	if r.URL.Path == "/join" {
		n := &NodeInfo{}
		if err := json.Unmarshal(body, n); err != nil {
			h.httpError(err, w, http.StatusInternalServerError)
			return
		}

		err := h.store.join(n)
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

			l = scheme + l + "/join"
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
	if h.isClosed() {
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
		return
	}

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
	case <-h.closing:
		h.httpError(fmt.Errorf("server closed"), w, http.StatusInternalServerError)
		return
	}
}

// servePing will return if the server is up, or if specified will check the status
// of the other metaservers as well
func (h *handler) servePing(w http.ResponseWriter, r *http.Request) {
	// if they're not asking to check all servers, just return who we think
	// the leader is
	if r.URL.Query().Get("all") == "" {
		w.Write([]byte(h.store.leader()))
		return
	}

	leader := h.store.leader()
	healthy := true
	for _, n := range h.store.otherMetaServersHTTP() {
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}
		url := scheme + n + "/ping"

		resp, err := http.Get(url)
		if err != nil {
			healthy = false
			break
		}

		defer resp.Body.Close()
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			healthy = false
			break
		}

		if leader != string(b) {
			healthy = false
			break
		}
	}

	if healthy {
		w.Write([]byte(h.store.leader()))
		return
	}

	h.httpError(fmt.Errorf("one or more metaservers not up"), w, http.StatusInternalServerError)
}

func (h *handler) servePeers(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	if err := enc.Encode(h.store.peers()); err != nil {
		h.httpError(err, w, http.StatusInternalServerError)
	}
}

// serveLease
func (h *handler) serveLease(w http.ResponseWriter, r *http.Request) {
	var name, nodeIDStr string
	q := r.URL.Query()

	// Get the requested lease name.
	name = q.Get("name")
	if name == "" {
		http.Error(w, "lease name required", http.StatusBadRequest)
		return
	}

	// Get the ID of the requesting node.
	nodeIDStr = q.Get("nodeid")
	if nodeIDStr == "" {
		http.Error(w, "node ID required", http.StatusBadRequest)
		return
	}

	// Redirect to leader if necessary.
	leader := h.store.leaderHTTP()
	if leader != h.s.httpAddr {
		if leader == "" {
			// No cluster leader. Client will have to try again later.
			h.httpError(errors.New("no leader"), w, http.StatusServiceUnavailable)
			return
		}
		scheme := "http://"
		if h.config.HTTPSEnabled {
			scheme = "https://"
		}

		leader = scheme + leader + "/lease?" + q.Encode()
		http.Redirect(w, r, leader, http.StatusTemporaryRedirect)
		return
	}

	// Convert node ID to an int.
	nodeID, err := strconv.ParseUint(nodeIDStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid node ID", http.StatusBadRequest)
		return
	}

	// Try to acquire the requested lease.
	// Always returns a lease. err determins if we own it.
	l, err := h.leases.Acquire(name, nodeID)
	// Marshal the lease to JSON.
	b, e := json.Marshal(l)
	if e != nil {
		h.httpError(e, w, http.StatusInternalServerError)
		return
	}
	// Write HTTP status.
	if err != nil {
		// Another node owns the lease.
		w.WriteHeader(http.StatusConflict)
	} else {
		// Lease successfully acquired.
		w.WriteHeader(http.StatusOK)
	}
	// Write the lease data.
	w.Header().Add("content-type", "application/json")
	w.Write(b)
	return
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
				b := make([]byte, 1024)
				runtime.Stack(b, false)
				logLine := buildLogLine(l, r, start)
				logLine = fmt.Sprintf("%s [panic:%s]\n%s", logLine, err, string(b))
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

type Lease struct {
	Name       string    `json:"name"`
	Expiration time.Time `json:"expiration"`
	Owner      uint64    `json:"owner"`
}

type Leases struct {
	mu sync.Mutex
	m  map[string]*Lease
	d  time.Duration
}

func NewLeases(d time.Duration) *Leases {
	return &Leases{
		m: make(map[string]*Lease),
		d: d,
	}
}

func (leases *Leases) Acquire(name string, nodeID uint64) (*Lease, error) {
	leases.mu.Lock()
	defer leases.mu.Unlock()

	l, ok := leases.m[name]
	if ok {
		if time.Now().After(l.Expiration) || l.Owner == nodeID {
			l.Expiration = time.Now().Add(leases.d)
			l.Owner = nodeID
			return l, nil
		}
		return l, errors.New("another node has the lease")
	}

	l = &Lease{
		Name:       name,
		Expiration: time.Now().Add(leases.d),
		Owner:      nodeID,
	}

	leases.m[name] = l

	return l, nil
}
