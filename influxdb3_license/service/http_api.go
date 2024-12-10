package main

import (
	"net/http"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/config"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/logger"
	"go.uber.org/zap"
)

type LicenseMaker interface {
	MakeLicense(email, instanceID, hostID string) (string, error)
}

type LicenseSigner interface {
	SignLicense(license string) (string, error)
}

type Mailer interface {
	SendEmail(to, subject, body string) error
}

type Storer interface {
	PutLicense(license string) error
	GetLicense(email, instanceID string) (string, error)
}

// HTTPHandler is the HTTP API handler for the license service.
type HTTPHandler struct {
	cfg    *config.Config
	logger *zap.Logger
	store  Storer
	mail   Mailer
	licmkr LicenseMaker
	licsgr LicenseSigner
}

// NewHTTPHandler creates a new Handler with the given configuration.
func NewHTTPHandler(cfg *config.Config, logger *zap.Logger, store Storer, mail Mailer, licmkr LicenseMaker, licsgr LicenseSigner) *HTTPHandler {
	return &HTTPHandler{
		cfg:    cfg,
		logger: logger,
		store:  store,
		mail:   mail,
		licmkr: licmkr,
		licsgr: licsgr,
	}
}

// ServeHTTP implements the http.Handler interface.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log, logEnd := logger.NewOperation(h.logger, "HTTP request recieved", "ServeHTTP")
	defer logEnd()

	// Log the request
	log.Info("Request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("query", r.URL.RawQuery),
		zap.String("remote_addr", r.RemoteAddr),
	)

	switch r.URL.Path {
	case "/licenses":
		h.handleLicenses(w, r, log)
	default:
		log.Error("Not Found", zap.String("path", r.URL.Path))
		http.NotFound(w, r)
	}
}

// handleLicenses handles requests to the /licenses endpoint.
func (h *HTTPHandler) handleLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	switch r.Method {
	case http.MethodPost:
		h.handlePostLicenses(w, r, log)
	case http.MethodGet:
		h.handleGetLicenses(w, r, log)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

// handlePostLicenses handles POST requests to the /licenses endpoint.
// This handler requires the following parameters:
// - email: the email address of the user
// - instance-id: the instance ID of the InfluxDB instance
// - host-id: the host ID of the InfluxDB instance
func (h *HTTPHandler) handlePostLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	email := r.FormValue("email")
	instanceID := r.FormValue("instance-id")
	hostID := r.FormValue("host-id")

	if email == "" || instanceID == "" || hostID == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// TODO: Implement email verification

	w.WriteHeader(http.StatusCreated)
}

// handleGetLicenses handles GET requests to the /licenses endpoint.
// This handler requires the following parameters:
// - email: the email address of the user
// - instance-id: the instance ID of the InfluxDB instance
func (h *HTTPHandler) handleGetLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	log.Info("Getting license")

	email := r.FormValue("email")
	instanceID := r.FormValue("instance-id")

	if email == "" || instanceID == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// TODO: Implement license retrieval
	lic := "InfluxDB Pro 30 Day Trial License"

	_, _ = w.Write([]byte(lic))
}
