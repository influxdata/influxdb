package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/email"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/license"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/config"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/logger"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"go.uber.org/zap"
)

var (
	// ErrInvalidIP indicates an invalid IP address.
	ErrInvalidIP = errors.New("invalid IP address")
	// ErrLicenseAlreadyExists indicates that a license already exists.
	ErrLicenseAlreadyExists = errors.New("license already exists")
	// ErrInstanceIDCollision indicates that an instance ID already exists.
	ErrInstanceIDCollision = errors.New("instance ID collision")
)

type contextKey int

const (
	realIPKey contextKey = iota
	nodeIDKey
	instanceIDKey
)

// HTTPHandler is the HTTP API handler for the license service.
type HTTPHandler struct {
	cfg        *config.Config
	logger     *zap.Logger
	store      store.Store
	emailSvc   *email.Service
	lic        *license.Creator
	logLimiter *rate.Sometimes
}

// NewHTTPHandler creates a new Handler with the given configuration.
func NewHTTPHandler(cfg *config.Config, logger *zap.Logger, store store.Store, emailSvc *email.Service, lic *license.Creator) *HTTPHandler {
	return &HTTPHandler{
		cfg:        cfg,
		logger:     logger,
		store:      store,
		emailSvc:   emailSvc,
		lic:        lic,
		logLimiter: &rate.Sometimes{First: 10, Interval: 10 * time.Second},
	}
}

// ServeHTTP implements the http.Handler interface.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log, logEnd := logger.NewOperation(h.logger, "HTTP request recieved", "ServeHTTP")
	defer logEnd()

	// Get the IP of the last hop this request went through before our proxies
	realIP, err := getRealIP(r, h.cfg.GetTrustedProxies())
	if err != nil {
		log.Error(err.Error())
		http.Error(w, "error processing request", http.StatusInternalServerError)
	}

	// Save the realIP in the request context so later methods don't have to re-parse
	r = r.WithContext(context.WithValue(r.Context(), realIPKey, realIP))

	w = newLoggingResponseWriter(w, h.logger, r.Method, r.URL.Redacted())
	// Log the request
	h.logHTTPRequest("ServeHTTP (start)", r, zapcore.InfoLevel, log)
	defer log.Info("ServeHTTP (end)")

	// TODO: implement API rate limit and better IP blocking

	blocked, err := h.isIPBlocked(r)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	} else if blocked {
		h.logLimiter.Do(func() {
			log.Warn("ignoring blocked IP address")
		})
		http.Error(w, "blocked", http.StatusForbidden)
	}

	switch r.URL.Path {
	case "/licenses":
		h.handleLicenses(w, r, log)
	case "/verify":
		h.handleVerify(w, r, log)
	default:
		log.Error("not Found", zap.String("path", r.URL.Path))
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
// - node-id: the node ID of the InfluxDB instance
func (h *HTTPHandler) handlePostLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	to := r.FormValue("email")
	instanceID := r.FormValue("instance-id")
	nodeID := r.FormValue("node-id")

	if nodeID == "" {
		// Try writer-id for backwards compatibility
		nodeID = r.FormValue("writer-id")
	}

	if to == "" || instanceID == "" || nodeID == "" {
		h.logHTTPRequest("missing required parameters", r, zapcore.InfoLevel, log)
		http.Error(w, "missing required parameters", http.StatusBadRequest)
		return
	}

	r = r.WithContext(context.WithValue(r.Context(), nodeIDKey, nodeID))
	r = r.WithContext(context.WithValue(r.Context(), instanceIDKey, instanceID))

	// Get user from database
	user, err := h.getOrCreateUser(r.Context(), to)
	if err != nil {
		// Something went wrong internally :-(
		log.Error("error getting user from database", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// User already exists. See if they're blocked
	blocked, err := h.isUserBlocked(r, user)
	if err != nil {
		log.Error(err.Error())
		http.Error(w, "error processing request", http.StatusInternalServerError)
	}
	if blocked {
		h.logLimiter.Do(func() {
			log.Warn("ignoring blocked user", zap.String("email", to))
		})
		http.Error(w, "blocked", http.StatusForbidden)
		return
	}

	// User isn't blocked so create a license for them.
	h.createLicenseForUser(w, r, log, user)
}

func (h *HTTPHandler) isIPBlocked(r *http.Request) (bool, error) {
	ipAddr := r.Context().Value(realIPKey).(net.IP)

	// Check if the IP is blocked
	blocked, err := h.store.IsIPBlocked(r.Context(), ipAddr)
	if err != nil {
		return true, fmt.Errorf("isBlocked: %w", err)
	}

	return blocked, nil
}

func (h *HTTPHandler) isUserBlocked(r *http.Request, user *store.User) (bool, error) {
	tx, err := h.store.BeginTx(r.Context())
	if err != nil {
		return true, fmt.Errorf("isUserBlocked: %w", err)
	}

	// Check if the user ID has any blocked IPs associated with it
	userIPs, err := h.store.GetUserIPsByUserID(r.Context(), tx, user.ID)
	if err != nil {
		return true, fmt.Errorf("isBlocked: %w", err)
	}

	for _, ip := range userIPs {
		if ip.Blocked {
			return true, nil
		}
	}

	return false, nil
}

// handleGetLicenses handles GET requests to the /licenses endpoint.
// This handler requires the following parameters:
// - email: the email address of the user
// - instance-id: the instance ID of the InfluxDB instance
func (h *HTTPHandler) handleGetLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	log.Info("Getting license")

	email := r.URL.Query().Get("email")
	instanceID := r.URL.Query().Get("instance-id")

	if email == "" || instanceID == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// Get the license from the database
	lic, err := h.store.GetLicenseByInstanceID(r.Context(), nil, instanceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "Not Found", http.StatusNotFound)
		} else {
			log.Error("error getting license by instance ID", zap.Error(err))
			http.Error(w, "error processing request", http.StatusInternalServerError)
		}
		return
	} else if lic == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Check if the email matches the license
	if lic.Email != email {
		log.Error("email does not match license",
			zap.String("email", email),
			zap.String("license-email", lic.Email),
			zap.String("instance-id", instanceID),
		)
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	} else if lic.State != store.LicenseStateActive {
		log.Info("license is not active", zap.String("instance-id", instanceID))
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	} else if lic.ValidUntil.Before(time.Now()) {
		log.Info("license has expired", zap.String("instance-id", instanceID))
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Write the license to the response
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(lic.LicenseKey)); err != nil {
		log.Error("error writing license to response", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	log.Info("License sent", zap.String("email", email), zap.String("instance-id", instanceID))
}

func (h *HTTPHandler) getOrCreateUser(ctx context.Context, to string) (*store.User, error) {
	tx, err := h.store.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			h.logger.Error("error rolling back transaction", zap.Error(err))
		}
	}()

	user, err := h.store.GetUserByEmailTx(ctx, tx, to)
	if err == nil {
		return user, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("error getting user from database: %w", err)
	}

	err = h.store.CreateUser(ctx, tx, &store.User{Email: to})
	if err != nil {
		return nil, fmt.Errorf("error creating user: %w", err)
	}

	user, err = h.store.GetUserByEmailTx(ctx, tx, to)
	if err != nil {
		return nil, fmt.Errorf("error getting user from database after successful creation: %w", err)
	}

	// Create the user's IP address entry in the database
	userIP := &store.UserIP{
		IPAddr: ctx.Value(realIPKey).(net.IP),
		UserID: user.ID,
	}

	err = h.store.CreateUserIP(ctx, tx, userIP)
	if err != nil {
		return nil, fmt.Errorf("error creating user ip: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("error commiting transaction: %w", err)
	}

	return user, nil
}

func (h *HTTPHandler) createLicenseForUser(w http.ResponseWriter, r *http.Request, log *zap.Logger, user *store.User) {
	tx, err := h.store.BeginTx(r.Context())
	if err != nil {
		log.Error("error starting transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("error rolling back transaction", zap.Error(err))
		}
	}()

	// Create a license for the user
	license, err := h.createLicense(r.Context(), tx, user)
	if err != nil {
		if errors.Is(err, ErrLicenseAlreadyExists) {
			log.Info("license already exists for user", zap.String("email", user.Email), zap.Error(err))
			// If the license already exists, we can just return the existing license
			w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(user.Email), r.Context().Value(instanceIDKey).(string)))
			w.WriteHeader(http.StatusAccepted)
			return
		} else if errors.Is(err, ErrInstanceIDCollision) {
			log.Error("instance ID already exists for another user", zap.String("email", user.Email), zap.Error(err))
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		log.Error("error creating license", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	if user.VerifiedAt != nil {
		log.Info("setting license state", zap.Error(err))
		err = h.store.SetLicenseState(r.Context(), tx, license.ID, store.LicenseStateRequested)
		if err != nil {
			log.Error("setting license state", zap.Error(err))
			http.Error(w, "error processing request", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(user.Email), license.InstanceID))
		w.WriteHeader(http.StatusCreated)
		return
	}

	switch license.State {
	case store.LicenseStateInactive:
		// "inactive" probably shouldn't be possible since it's never set anywhere,
		// but handle it here anyway
		log.Error("error creating license", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	case store.LicenseStateRequested:
		// Queue a verification email to be sent

		// NOTE: the logic around when we send the email verification is incomplete
		// at the time of writing this comment since there is no reliable way to
		// know whether we've already sent an email verification for a given user
		// without making some assumptions about the number of licenses (ie if
		// there is more than one license, assume a verification email has gone
		// out) OR without changing the LicenseState data model on the backend with
		// new data migration
		//
		// this means that whenever a license is created before verification the
		// user will receive a new verification request.
		//
		// this could result in redundant un-verified tokens that stick around in
		// the database even after one of them has been verified
		err := h.sendEmailVerification(r.Context(), tx, user, license.ID)
		if err != nil {
			log.Error("error sending email verification", zap.Error(err))
			http.Error(w, "error processing request", http.StatusInternalServerError)
			return
		}
	default:
		log.Error("unhandled license state during creation", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return

	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Error("error committing transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Send a response to the client with Location header to download
	// the license
	w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(user.Email), license.InstanceID))
	w.WriteHeader(http.StatusCreated)
}

func (h *HTTPHandler) sendEmailVerification(ctx context.Context, tx store.Tx, user *store.User, licenseID int64) error {
	// Queue an email to be sent to the user to verify their email address
	token := uuid.NewString()
	ipAddr := ctx.Value(realIPKey).(net.IP)

	// Create the verification URL
	verificationURL := h.cfg.EmailVerificationURL + "/verify?token=" + token

	msg := &store.Email{
		UserID:            user.ID,
		UserIP:            ipAddr,
		VerificationToken: token,
		VerificationURL:   verificationURL,
		LicenseID:         licenseID,
		TemplateName:      h.cfg.EmailTemplateName,
		From:              "noreply@influxdata.com",
		To:                user.Email,
		Subject:           "InfluxDB 3 Enterprise Trial License Verification",
	}

	if err := h.emailSvc.QueueEmail(ctx, tx, msg); err != nil {
		return err
	}

	return nil
}

// handleVerify handles requests to the /verify endpoint, which is called when
// a new user clicks on the verification link in their email.
func (h *HTTPHandler) handleVerify(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the verification token from the request query parameters
	token := r.FormValue("token")
	if token == "" {
		http.Error(w, "missing required token parameter", http.StatusBadRequest)
		return
	}

	// Start a transaction for the database operations
	tx, err := h.store.BeginTx(r.Context())
	if err != nil {
		log.Error("error starting transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("error rolling back transaction", zap.Error(err))
		}
	}()

	// Mark the email as verified
	status, err := h.store.MarkEmailAsVerified(r.Context(), tx, token)
	if err != nil {
		log.Error("error marking email as verified", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	if status == store.EmailNotFound {
		log.Info("email not found", zap.String("token", token))
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	} else if status == store.EmailAlreadyVerified {
		log.Info("email already verified", zap.String("token", token))
		http.Error(w, "email already verified", http.StatusBadRequest)
		return
	}

	// Get the email record from the database
	email, err := h.store.GetEmailByVerificationToken(r.Context(), tx, token)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Info("email not found", zap.String("token", token))
			http.Error(w, "Not Found", http.StatusNotFound)
			return
		}

		log.Error("error getting email by token", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Mark user as verified
	if err := h.store.MarkUserAsVerified(r.Context(), tx, email.UserID); err != nil {
		log.Error("error marking user as verified", zap.Int64("user-id", email.UserID), zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Mark the license as active
	if err := h.store.SetLicenseState(r.Context(), tx, email.LicenseID, store.LicenseStateActive); err != nil {
		log.Error("error setting license state", zap.Int64("license-id", email.LicenseID), zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Fetch the license from the database
	lic, err := h.store.GetLicenseByID(r.Context(), tx, email.LicenseID)
	if err != nil {
		log.Error("error getting license by ID", zap.Int64("license-id", email.LicenseID), zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Error("error committing transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Convert license duration to days
	days := int(lic.ValidUntil.Sub(lic.ValidFrom).Hours() / 24)

	// URL encode the email
	urlSafeEmail := url.QueryEscape(lic.Email)

	// Return a success page
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `
		<html>
			<head>
				<title>Email Verified</title>
				<style>
					body { font-family: system-ui; max-width: 600px; margin: 40px auto; padding: 0 20px; }
					.success { color: #15803d; }
				</style>
			</head>
			<body>
				<h1 class="success">✓ Email Verified Successfully</h1>
				<p>Your %d day InfluxDB 3 Enterprise trial license is now active.</p>
				<p>If you verified your email while InfluxDB was waiting, it should have saved the license in the object store and should now be running and ready to use. If InfluxDB is not running, simply run it again and enter the same email address when prompted. It should fetch the license and startup immediately.</p>
				<p>You can also download the trial license file directly from <a href="/licenses?email=%s&instance-id=%s">here</a> and manually save it to the object store.</p>
			</body>
		</html>
	`, days, urlSafeEmail, lic.InstanceID)
}

func (h *HTTPHandler) createLicense(ctx context.Context, tx store.Tx, user *store.User) (*store.License, error) {
	nodeID := ctx.Value(nodeIDKey).(string)
	instanceID := ctx.Value(instanceIDKey).(string)

	// Check if the user already has a license for this instance ID
	license, err := h.store.GetLicenseByInstanceID(ctx, tx, instanceID)
	if license != nil {
		return nil, ErrLicenseAlreadyExists
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("createLicense: error getting license by instance ID: %w", err)
	}

	// no license exists, so create a new one

	// create a signed license token
	now, dur := h.trialLicenseDuration()

	token, err := h.lic.Create(user.Email, nodeID, instanceID, dur)
	if err != nil {
		return nil, fmt.Errorf("createLicense: error creating signed license token: %w", err)
	}

	// Save the license in the database
	lic := &store.License{
		UserID:     user.ID,
		Email:      user.Email,
		NodeID:     nodeID,
		InstanceID: instanceID,
		LicenseKey: token,
		ValidFrom:  now,
		ValidUntil: now.Add(dur),
		State:      store.LicenseStateRequested,
	}

	err = h.store.CreateLicense(ctx, tx, lic)
	if err != nil {
		return nil, fmt.Errorf("createLicense: error creating license in database: %w", err)
	}

	return lic, nil
}

func (h *HTTPHandler) trialLicenseDuration() (now time.Time, dur time.Duration) {
	now = time.Now()

	if now.After(h.cfg.TrialEndDate) {
		return now, h.cfg.TrialDuration
	}

	return now, h.cfg.TrialEndDate.Sub(now)
}

func (h *HTTPHandler) logHTTPRequest(msg string, r *http.Request, lvl zapcore.Level, logger *zap.Logger) {
	if logger == nil {
		return
	}

	var ipAddrStr string
	if ipAddr := r.Context().Value(realIPKey); ipAddr != nil {
		ipAddrStr = ipAddr.(net.IP).String()
	} else {
		ipAddrStr = r.RemoteAddr
	}

	fields := []zap.Field{
		zap.String("method", r.Method),
		zap.String("url", r.URL.String()),
		zap.String("remote_addr", ipAddrStr),
		zap.String("user_agent", r.UserAgent()),
	}

	switch lvl {
	case zapcore.DebugLevel:
		logger.Debug(msg, fields...)
	case zapcore.InfoLevel:
		logger.Info(msg, fields...)
	case zapcore.WarnLevel:
		logger.Warn(msg, fields...)
	case zapcore.ErrorLevel:
		logger.Error(msg, fields...)
	case zapcore.DPanicLevel:
		logger.DPanic(msg, fields...)
	case zapcore.PanicLevel:
		logger.Panic(msg, fields...)
	case zapcore.FatalLevel:
		logger.Fatal(msg, fields...)
	default:
		logger.Info(msg, fields...) // Default to Info level if unspecified
	}
}

func getRealIP(r *http.Request, trustedProxies []net.IPNet) (net.IP, error) {
	// First get the immediate client IP
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteIP = r.RemoteAddr // Handle case where port isn't present
	}

	clientIP := net.ParseIP(remoteIP)
	if clientIP == nil {
		return nil, fmt.Errorf("%w: invalid remote IP: %s", ErrInvalidIP, remoteIP)
	}

	// Only process headers if the request came from a trusted proxy
	isTrustedProxy := false
	for _, proxy := range trustedProxies {
		if proxy.Contains(clientIP) {
			isTrustedProxy = true
			break
		}
	}

	if !isTrustedProxy {
		return clientIP, nil // Return immediate client IP if not from trusted proxy
	}

	// Process XFF from trusted proxy
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		ips := strings.Split(xff, ",")
		// Take first non-private IP from the left
		// This assumes your proxies append IPs to the right
		for _, ip := range ips {
			ip = strings.TrimSpace(ip)
			parsed := net.ParseIP(ip)
			if parsed == nil {
				continue
			}
			if !parsed.IsPrivate() && !parsed.IsLoopback() {
				return parsed, nil
			}
		}
	}

	return clientIP, nil
}

// loggingResponseWriter implments http.ResponseWriter in such a way that
// if either Write or WriteHeader are called the response gets logged. If a
// response handler doesn't call either of these methods then nothing is
// logged.
//
// Since it's possible that both Write and WriteHeader are called in a given
// response, we also keep track of whether or not anything at all has been
// logged in order to avoid duplicate logs.
type loggingResponseWriter struct {
	w      http.ResponseWriter
	logger *zap.Logger

	statusCode int
	method     string
	url        string

	logged bool
}

func newLoggingResponseWriter(w http.ResponseWriter, logger *zap.Logger, method, url string) http.ResponseWriter {
	return &loggingResponseWriter{
		w:          w,
		logger:     logger,
		statusCode: http.StatusOK,
		method:     method,
		url:        url,
		logged:     false,
	}
}

// Header implements http.ResponseWriter
func (w *loggingResponseWriter) Header() http.Header {
	return w.w.Header()
}

// Write implements http.ResponseWriter
func (w *loggingResponseWriter) Write(bs []byte) (int, error) {
	w.log()
	return w.w.Write(bs)
}

// WriteHeader implements http.ResponseWriter
func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.log()
	w.w.WriteHeader(statusCode)
}

func (w *loggingResponseWriter) log() {
	if w.logger != nil && !w.logged {
		fields := []zap.Field{
			zap.String("method", w.method),
			zap.String("url", w.url),
			zap.Int("status_code", w.statusCode),
		}
		if w.logger.Level() == zapcore.DebugLevel {
			for k, v := range w.w.Header() {
				fields = append(fields, zap.Strings(k, v))
			}
		}
		w.logger.Info("Writing HTTP response", fields...)
		w.logged = true
	}
}
