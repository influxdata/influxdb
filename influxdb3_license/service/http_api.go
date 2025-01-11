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
	ErrInvalidIP            = errors.New("invalid IP address")
	ErrLicenseAlreadyExists = errors.New("license already exists")
	ErrInstanceIDCollision  = errors.New("instance ID collision")
)

type contextKey int

const (
	realIPKey contextKey = iota
	hostIDKey
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
// - host-id: the host ID of the InfluxDB instance
func (h *HTTPHandler) handlePostLicenses(w http.ResponseWriter, r *http.Request, log *zap.Logger) {
	to := r.FormValue("email")
	instanceID := r.FormValue("instance-id")
	hostID := r.FormValue("host-id")

	if to == "" || instanceID == "" || hostID == "" {
		h.logHTTPRequest("missing required parameters", r, zapcore.InfoLevel, log)
		http.Error(w, "missing required parameters", http.StatusBadRequest)
		return
	}

	r = r.WithContext(context.WithValue(r.Context(), hostIDKey, hostID))
	r = r.WithContext(context.WithValue(r.Context(), instanceIDKey, instanceID))

	// Get user from database
	user, err := h.store.GetUserByEmail(r.Context(), to)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// This user doesn't exist in the database yet so handle that flow
			h.handleNewUser(w, r, log, to)
			return
		}

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

	// Ensure that user is verified
	if user.VerifiedAt == nil || user.VerifiedAt.IsZero() {
		log.Info("user is not verified", zap.String("email", to))
		http.Error(w, "bad request - please verify your email address", http.StatusBadRequest)
		return
	}

	// User isn't blocked so create a license for them.
	h.handleExistingUserNewLicense(w, r, log, user)
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
			return
		} else {
			log.Error("error getting license by instance ID", zap.Error(err))
			http.Error(w, "error processing request", http.StatusInternalServerError)
			return
		}
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

func (h *HTTPHandler) handleNewUser(w http.ResponseWriter, r *http.Request, log *zap.Logger, to string) {
	// Create a transaction for the database operations to keep them atomic
	tx, err := h.store.BeginTx(r.Context())
	if err != nil {
		log.Error("error starting transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}
	defer func(tx store.Tx) {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			log.Error("error rolling back transaction", zap.Error(err))
		}
	}(tx)

	// Create a new user record in the database
	user := &store.User{Email: to}
	if err := h.store.CreateUser(r.Context(), tx, user); err != nil {
		log.Error("error creating user", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Create the user's IP address entry in the database
	user_ip := &store.UserIP{
		IPAddr: r.Context().Value(realIPKey).(net.IP),
		UserID: user.ID,
	}

	if err := h.store.CreateUserIP(r.Context(), tx, user_ip); err != nil {
		log.Error("error creating user ip", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Create a license for the user
	licenseID, err := h.createLicense(r.Context(), tx, user)
	if err != nil {
		if errors.Is(err, ErrLicenseAlreadyExists) {
			log.Info("license already exists for user", zap.String("email", user.Email), zap.Error(err))
			// If the license already exists, we can just return the existing license
			w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(to), r.Context().Value(instanceIDKey).(string)))
			w.WriteHeader(http.StatusCreated)
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

	// Queue a verification email to be sent
	if err := h.sendEmailVerification(r.Context(), tx, user, licenseID); err != nil {
		log.Error("error sending email verification", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Error("error committing transaction", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Get the host ID and instance ID from the request context
	instanceID := r.Context().Value(instanceIDKey).(string)

	// Send a response to the client with Location header to download
	// the license
	w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", url.QueryEscape(to), instanceID))
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

func (h *HTTPHandler) handleExistingUserNewLicense(w http.ResponseWriter, r *http.Request, log *zap.Logger, user *store.User) {
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

	// Create a record for the user's IP address if it doesn't already exist
	userIP := &store.UserIP{
		UserID: user.ID,
		IPAddr: r.Context().Value(realIPKey).(net.IP),
	}
	if err := h.store.CreateUserIP(r.Context(), tx, userIP); err != nil {
		log.Error("error creating user ip", zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Create another license for his user in the database
	licID, err := h.createLicense(r.Context(), tx, user)
	if err != nil {
		if errors.Is(err, ErrLicenseAlreadyExists) {
			// If the license already exists, we can just return the existing license
			w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", user.Email, r.Context().Value(instanceIDKey).(string)))
			w.WriteHeader(http.StatusCreated)
			return
		} else if errors.Is(err, ErrInstanceIDCollision) {
			log.Error("instance ID already exists for another user", zap.String("email", user.Email), zap.Error(err))
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		log.Error("error creating license for existing user", zap.Int64("user-id", user.ID), zap.String("email", user.Email), zap.Error(err))
		http.Error(w, "error processing request", http.StatusInternalServerError)
		return
	}

	// Since this is an existing user, we don't need to send a verification
	// email and we need to mark the license as active
	if err := h.store.SetLicenseState(r.Context(), tx, licID, store.LicenseStateActive); err != nil {
		log.Error("error setting license state", zap.Int64("license-id", licID), zap.Error(err))
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
	instanceID := r.Context().Value(instanceIDKey).(string)
	w.Header().Set("Location", fmt.Sprintf("/licenses?email=%s&instance-id=%s", user.Email, instanceID))
	w.WriteHeader(http.StatusCreated)
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

func (h *HTTPHandler) createLicense(ctx context.Context, tx store.Tx, user *store.User) (licenseID int64, err error) {
	hostID := ctx.Value(hostIDKey).(string)
	instanceID := ctx.Value(instanceIDKey).(string)

	// Check if the user already has a license for this host ID
	lic, err := h.store.GetLicenseByEmailAndHostID(ctx, tx, user.Email, hostID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return -1, fmt.Errorf("createLicense: error getting license by host ID: %w", err)
	} else if lic != nil {
		return -1, fmt.Errorf("createLicense: %w: host ID: %s", ErrLicenseAlreadyExists, hostID)
	}

	// Check if the user already has a license for this instance ID
	lic, err = h.store.GetLicenseByInstanceID(ctx, tx, instanceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return -1, fmt.Errorf("createLicense: error getting license by instance ID: %w", err)
	} else if lic != nil {
		if lic.Email != user.Email {
			return -1, fmt.Errorf("createLicense: %w: instance ID: %s", ErrInstanceIDCollision, instanceID)
		}
		return -1, fmt.Errorf("createLicense: %w: instance ID: %s", ErrLicenseAlreadyExists, instanceID)
	}

	// create a signed license token
	now, dur := h.trialLicenseDuration()

	token, err := h.lic.Create(user.Email, hostID, instanceID, dur)
	if err != nil {
		return -1, fmt.Errorf("createLicense: error creating signed license token: %w", err)
	}

	// Save the license in the database
	lic = &store.License{
		UserID:     user.ID,
		Email:      user.Email,
		HostID:     hostID,
		InstanceID: instanceID,
		LicenseKey: token,
		ValidFrom:  now,
		ValidUntil: now.Add(dur),
		State:      store.LicenseStateRequested,
	}

	if err := h.store.CreateLicense(ctx, tx, lic); err != nil {
		return -1, fmt.Errorf("createLicense: error creating license in database: %w", err)
	}

	return lic.ID, nil
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
