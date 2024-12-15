package store

import (
	"context"
	"net"
	"time"
)

// User represents a user record from the database
type User struct {
	ID                int64
	Email             string
	EmailsSentCount   int
	VerificationToken string
	VerifiedAt        *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
	DeletedAt         *time.Time
}

// UserIP represents a record in the user_ips table
type UserIP struct {
	IPAddr net.IP
	UserID int64
}

// Email represents an email sent record
type Email struct {
	ID           int64
	ToEmail      string
	TemplateName string
	Subject      string
	Body         string
	SentAt       time.Time
}

// LicenseStatus is an enum for valid license states
type LicenseStatus string

const (
	LicenseStatusRequested LicenseStatus = "requested"
	LicenseStatusActive    LicenseStatus = "active"
	LicenseStatusInactive  LicenseStatus = "inactive"
)

// License represents a license record
type License struct {
	ID         int64
	Email      string
	HostID     string
	InstanceID string
	LicenseKey string
	ValidFrom  time.Time
	ValidUntil time.Time
	Status     LicenseStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
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
	GetUserByEmail(ctx context.Context, tx Tx, email string) (*User, error)

	// UserIP operations
	CreateUserIP(ctx context.Context, tx Tx, userIP *UserIP) error
	DeleteUserIP(ctx context.Context, tx Tx, ipAddr net.IP, userID int64) error
	GetUserIPsByUserID(ctx context.Context, tx Tx, userID int64) ([]*UserIP, error)
	GetUserIDsByIPAddr(ctx context.Context, tx Tx, ipAddr net.IP) ([]int64, error)

	// Email operations
	CreateEmail(ctx context.Context, tx Tx, email *Email) error
	UpdateEmail(ctx context.Context, tx Tx, email *Email) error
	DeleteEmail(ctx context.Context, tx Tx, id int64) error
	GetEmailsByToEmail(ctx context.Context, tx Tx, email string) ([]*Email, error)

	// License operations
	CreateLicense(ctx context.Context, tx Tx, license *License) error
	UpdateLicense(ctx context.Context, tx Tx, license *License) error
	DeleteLicense(ctx context.Context, tx Tx, id int64) error
	GetLicensesByEmail(ctx context.Context, tx Tx, email string) ([]*License, error)
	GetLicenseByInstanceID(ctx context.Context, tx Tx, instanceID string) (*License, error)
}
