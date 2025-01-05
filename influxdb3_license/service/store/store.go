package store

import (
	"context"
	"net"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// User represents a user record from the database
type User struct {
	ID              int64
	Email           string
	EmailsSentCount int
	VerifiedAt      *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	DeletedAt       *time.Time
}

// UserIP represents a record in the user_ips table
type UserIP struct {
	IPAddr  net.IP
	UserID  int64
	Blocked bool
}

// EmailState describes the possible states an email can be in
type EmailState string

const (
	EmailStateScheduled EmailState = "scheduled"
	EmailStateSent      EmailState = "sent"
	EmailStateFailed    EmailState = "failed"
)

func (e EmailState) String() string {
	switch e {
	case EmailStateScheduled:
		return "scheduled"
	case EmailStateSent:
		return "sent"
	case EmailStateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Email represents an email in emails_pending or emails_sent
type Email struct {
	ID                int64
	UserID            int64
	UserIP            net.IP
	VerificationToken string
	VerifiedAt        time.Time
	VerificationURL   string
	LicenseID         int64
	TemplateName      string
	From              string
	To                string
	Subject           string
	Body              string
	State             EmailState
	ScheduledAt       time.Time
	SentAt            time.Time // Date/time of last successful send
	SendCnt           int       // Count of successful sends
	SendFailCnt       int       // Count of failed send attempts
	LastErrMsg        string    // Error message from last failed send
	DeliverySrvcResp  string    // Response from delivery service (eg, Mailgun)
	DeliverSrvcID     string    // ID from delivery service for this email
}

func LogEmail(msg string, e *Email, level zapcore.Level, logger *zap.Logger) {
	if logger == nil {
		return
	}

	fields := []zap.Field{
		zap.Int64("id", e.ID),
		zap.Int64("user_id", e.UserID),
		zap.String("user_ip", e.UserIP.String()),
		zap.String("verification_token", e.VerificationToken),
		zap.String("verification_url", e.VerificationURL),
		zap.Int64("license_id", e.LicenseID),
		zap.String("to_email", e.To),
		zap.String("state", string(e.State)),
		zap.Time("scheduled_at", e.ScheduledAt),
		zap.Int("send_cnt", e.SendCnt),
		zap.Int("send_fail_cnt", e.SendFailCnt),
	}

	if level == zapcore.ErrorLevel || level == zapcore.DebugLevel {
		fields = append(fields,
			zap.String("email_template_name", e.TemplateName),
			zap.String("subject", e.Subject),
			zap.String("body", e.Body),
		)
	}

	// Optional fields
	if e.TemplateName != "" {
		fields = append(fields, zap.String("template_name", e.TemplateName))
	}
	if !e.SentAt.IsZero() {
		fields = append(fields, zap.Time("sent_at", e.SentAt))
	}
	if e.LastErrMsg != "" {
		fields = append(fields, zap.String("error", e.LastErrMsg))
	}
	if e.DeliverySrvcResp != "" {
		fields = append(fields, zap.String("delivery_srvc_resp", e.DeliverySrvcResp))
	}
	if e.DeliverSrvcID != "" {
		fields = append(fields, zap.String("deliver_srvc_id", e.DeliverSrvcID))
	}

	switch level {
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
		logger.Info(msg, fields...)
	}
}

// LicenseState is an enum for valid license states
type LicenseState string

const (
	LicenseStateRequested LicenseState = "requested"
	LicenseStateActive    LicenseState = "active"
	LicenseStateInactive  LicenseState = "inactive"
)

// License represents a license record
type License struct {
	ID         int64
	UserID     int64
	Email      string
	HostID     string
	InstanceID string
	LicenseKey string
	ValidFrom  time.Time
	ValidUntil time.Time
	State      LicenseState
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type VerificationStatus int

const (
	EmailNotFound VerificationStatus = iota
	EmailVerified
	EmailAlreadyVerified
)

// String provides human-readable descriptions of verification statuses
func (s VerificationStatus) String() string {
	switch s {
	case EmailNotFound:
		return "email not found"
	case EmailVerified:
		return "email verified"
	case EmailAlreadyVerified:
		return "email already verified"
	default:
		return "unknown status"
	}
}

// Tx represents a database transaction
type Tx interface {
	Commit() error
	Rollback() error
}

// Store defines the interface for all storage operations
type Store interface {
	// Transaction handling
	BeginTx(ctx context.Context) (Tx, error)

	// User operations
	CreateUser(ctx context.Context, tx Tx, user *User) error
	UpdateUser(ctx context.Context, tx Tx, user *User) error
	DeleteUser(ctx context.Context, tx Tx, id int64) error
	GetUserByEmail(ctx context.Context, email string) (*User, error)
	GetUserByEmailTx(ctx context.Context, tx Tx, email string) (*User, error)
	MarkUserAsVerified(ctx context.Context, tx Tx, id int64) error

	// UserIP operations
	CreateUserIP(ctx context.Context, tx Tx, userIP *UserIP) error
	DeleteUserIP(ctx context.Context, tx Tx, ipAddr net.IP, userID int64) error
	//GetIPsByUserID(ctx context.Context, tx Tx, userID int64) ([]*UserIP, error)
	GetUserIDsByIPAddr(ctx context.Context, tx Tx, ipAddr net.IP) ([]int64, error)
	GetUserIPsByUserID(ctx context.Context, tx Tx, userID int64) ([]*UserIP, error)
	BlockUserIP(ctx context.Context, tx Tx, ipAddr net.IP, userID int64) error
	IsIPBlocked(ctx context.Context, ipAddr net.IP) (bool, error)

	// Email operations
	CreateEmail(ctx context.Context, tx Tx, email *Email) error
	UpdateEmail(ctx context.Context, tx Tx, email *Email) error
	DeleteEmail(ctx context.Context, tx Tx, id int64) error
	DeleteAllEmails(ctx context.Context, tx Tx) error
	GetEmailByID(ctx context.Context, tx Tx, id int64) (*Email, error)
	GetEmailByVerificationToken(ctx context.Context, tx Tx, token string) (*Email, error)
	GetScheduledEmailCnt(ctx context.Context, tx Tx) (int64, error)
	GetScheduledEmailBatch(ctx context.Context, tx Tx, batchSize int) ([]*Email, error)
	MarkEmailAsSent(ctx context.Context, tx Tx, email *Email) error
	MarkEmailAsVerified(ctx context.Context, tx Tx, token string) (VerificationStatus, error)
	MarkEmailAsFailed(ctx context.Context, tx Tx, id int64, errorMsg string) error

	// License operations
	CreateLicense(ctx context.Context, tx Tx, license *License) error
	UpdateLicense(ctx context.Context, tx Tx, license *License) error
	DeleteLicense(ctx context.Context, tx Tx, id int64) error
	GetLicensesByEmail(ctx context.Context, tx Tx, email string) ([]*License, error)
	GetLicenseCntByUserID(ctx context.Context, tx Tx, userID int64) (int64, error)
	GetLicenseByInstanceID(ctx context.Context, tx Tx, instanceID string) (*License, error)
	GetLicenseByID(ctx context.Context, tx Tx, id int64) (*License, error)
	SetLicenseState(ctx context.Context, tx Tx, id int64, state LicenseState) error
	DeactivateExpiredLicenses(ctx context.Context, tx Tx) error
}
