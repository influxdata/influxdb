package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
	"net"
)

// Store implements the store.Store interface for Postgres
type Store struct {
	db *sql.DB
}

// NewStore creates a new Postgres store
func NewStore(db *sql.DB) *Store {
	return &Store{
		db: db,
	}
}

func (s *Store) BeginTx(ctx context.Context) (store.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	return tx, nil
}

// User operations
func (s *Store) CreateUser(ctx context.Context, tx store.Tx, user *store.User) error {
	query := `
        INSERT INTO users (email, emails_sent_cnt, verification_token, verified_at, deleted_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, created_at, updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		user.Email,
		user.EmailsSentCount,
		user.VerificationToken,
		user.VerifiedAt,
		user.DeletedAt,
	).Scan(&user.ID, &user.CreatedAt, &user.UpdatedAt)

	if err != nil {
		return fmt.Errorf("creating user: %w", err)
	}
	return nil
}

func (s *Store) UpdateUser(ctx context.Context, tx store.Tx, user *store.User) error {
	query := `
        UPDATE users
        SET email = $1, emails_sent_cnt = $2, verification_token = $3,
            verified_at = $4, deleted_at = $5, updated_at = statement_timestamp()
        WHERE id = $6
        RETURNING updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		user.Email,
		user.EmailsSentCount,
		user.VerificationToken,
		user.VerifiedAt,
		user.DeletedAt,
		user.ID,
	).Scan(&user.UpdatedAt)

	if err != nil {
		return fmt.Errorf("updating user: %w", err)
	}
	return nil
}

func (s *Store) DeleteUser(ctx context.Context, tx store.Tx, id int64) error {
	query := `DELETE FROM users WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("deleting user: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no user found with id %d", id)
	}
	return nil
}

func (s *Store) GetUserByEmail(ctx context.Context, tx store.Tx, email string) (*store.User, error) {
	var user store.User

	query := `
        SELECT id, email, emails_sent_cnt, verification_token,
               verified_at, created_at, updated_at
        FROM users
        WHERE email = $1 AND deleted_at IS NULL`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.EmailsSentCount,
		&user.VerificationToken,
		&user.VerifiedAt,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting user by email: %w", err)
	}

	return &user, nil
}

func (s *Store) CreateUserIP(ctx context.Context, tx store.Tx, userIP *store.UserIP) error {
	query := `
        INSERT INTO user_ips (ipaddr, user_id)
        VALUES ($1, $2)`

	sqlTx := tx.(*sql.Tx)
	_, err := sqlTx.ExecContext(
		ctx, query,
		userIP.IPAddr.String(), // Convert net.IP to string for storage
		userIP.UserID,
	)

	if err != nil {
		return fmt.Errorf("creating user IP record: %w", err)
	}
	return nil
}

func (s *Store) DeleteUserIP(ctx context.Context, tx store.Tx, ipAddr net.IP, userID int64) error {
	query := `DELETE FROM user_ips WHERE ipaddr = $1 AND user_id = $2`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, ipAddr.String(), userID)
	if err != nil {
		return fmt.Errorf("deleting user IP record: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no user IP record found for IP %s and user ID %d", ipAddr.String(), userID)
	}
	return nil
}

func (s *Store) GetUserIPsByUserID(ctx context.Context, tx store.Tx, userID int64) ([]*store.UserIP, error) {
	query := `
        SELECT ipaddr, user_id
        FROM user_ips
        WHERE user_id = $1`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("querying user IP records: %w", err)
	}
	defer rows.Close()

	var userIPs []*store.UserIP
	for rows.Next() {
		var userIP store.UserIP
		var ipAddrStr string
		err := rows.Scan(&ipAddrStr, &userIP.UserID)
		if err != nil {
			return nil, fmt.Errorf("scanning user IP record: %w", err)
		}

		// Parse the IP address string back into net.IP
		userIP.IPAddr = net.ParseIP(ipAddrStr)
		if userIP.IPAddr == nil {
			return nil, fmt.Errorf("invalid IP address format in database: %s", ipAddrStr)
		}

		userIPs = append(userIPs, &userIP)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating user IP records: %w", err)
	}

	return userIPs, nil
}

func (s *Store) GetUserIDsByIPAddr(ctx context.Context, tx store.Tx, ipAddr net.IP) ([]int64, error) {
	query := `
        SELECT user_id
        FROM user_ips
        WHERE ipaddr = $1`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, ipAddr.String())
	if err != nil {
		return nil, fmt.Errorf("querying user IDs by IP: %w", err)
	}
	defer rows.Close()

	var userIDs []int64
	for rows.Next() {
		var userID int64
		err := rows.Scan(&userID)
		if err != nil {
			return nil, fmt.Errorf("scanning user ID: %w", err)
		}
		userIDs = append(userIDs, userID)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating user IDs: %w", err)
	}

	return userIDs, nil
}

// Email operations
func (s *Store) CreateEmail(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        INSERT INTO emails_sent (to_email, email_template_name, subject, body)
        VALUES ($1, $2, $3, $4)
        RETURNING id, sent_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		email.ToEmail,
		email.TemplateName,
		email.Subject,
		email.Body,
	).Scan(&email.ID, &email.SentAt)

	if err != nil {
		return fmt.Errorf("creating email record: %w", err)
	}
	return nil
}

func (s *Store) UpdateEmail(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        UPDATE emails_sent
        SET to_email = $1, email_template_name = $2, subject = $3, body = $4
        WHERE id = $5`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(
		ctx, query,
		email.ToEmail,
		email.TemplateName,
		email.Subject,
		email.Body,
		email.ID,
	)

	if err != nil {
		return fmt.Errorf("updating email record: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no email record found with id %d", email.ID)
	}
	return nil
}

func (s *Store) DeleteEmail(ctx context.Context, tx store.Tx, id int64) error {
	query := `DELETE FROM emails_sent WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("deleting email record: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no email record found with id %d", id)
	}
	return nil
}

func (s *Store) GetEmailsByToEmail(ctx context.Context, tx store.Tx, email string) ([]*store.Email, error) {
	query := `
        SELECT id, to_email, email_template_name, subject, body, sent_at
        FROM emails_sent
        WHERE to_email = $1
        ORDER BY sent_at DESC`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, email)
	if err != nil {
		return nil, fmt.Errorf("querying email records: %w", err)
	}
	defer rows.Close()

	var emails []*store.Email
	for rows.Next() {
		var email store.Email
		err := rows.Scan(
			&email.ID,
			&email.ToEmail,
			&email.TemplateName,
			&email.Subject,
			&email.Body,
			&email.SentAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning email record: %w", err)
		}
		emails = append(emails, &email)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating email records: %w", err)
	}

	return emails, nil
}

// License operations
func (s *Store) CreateLicense(ctx context.Context, tx store.Tx, license *store.License) error {
	query := `
        INSERT INTO licenses (
            email, host_id, instance_id, license_key, valid_until
        ) VALUES (
            $1, $2, $3, $4, $5
        )
        RETURNING id, email, host_id, instance_id, license_key,
                  valid_from, valid_until, status, created_at, updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		license.Email,
		license.HostID,
		license.InstanceID,
		license.LicenseKey,
		license.ValidUntil,
	).Scan(
		&license.ID,
		&license.Email,
		&license.HostID,
		&license.InstanceID,
		&license.LicenseKey,
		&license.ValidFrom,
		&license.ValidUntil,
		&license.Status,
		&license.CreatedAt,
		&license.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("creating license: %w", err)
	}
	return nil
}

func (s *Store) UpdateLicense(ctx context.Context, tx store.Tx, license *store.License) error {
	query := `
        UPDATE licenses
        SET email = $1, host_id = $2, instance_id = $3, license_key = $4,
            valid_from = $5, valid_until = $6, status = $7, updated_at = statement_timestamp()
        WHERE id = $8
        RETURNING updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		license.Email,
		license.HostID,
		license.InstanceID,
		license.LicenseKey,
		license.ValidFrom,
		license.ValidUntil,
		license.Status,
		license.ID,
	).Scan(&license.UpdatedAt)

	if err != nil {
		return fmt.Errorf("updating license: %w", err)
	}
	return nil
}

func (s *Store) DeleteLicense(ctx context.Context, tx store.Tx, id int64) error {
	query := `DELETE FROM licenses WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("deleting license: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no license found with id %d", id)
	}
	return nil
}

func (s *Store) GetLicensesByEmail(ctx context.Context, tx store.Tx, email string) ([]*store.License, error) {
	query := `
        SELECT id, email, host_id, instance_id, license_key, valid_from,
               valid_until, status, created_at, updated_at
        FROM licenses
        WHERE email = $1`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, email)
	if err != nil {
		return nil, fmt.Errorf("querying licenses: %w", err)
	}
	defer rows.Close()

	var licenses []*store.License
	for rows.Next() {
		var license store.License
		err := rows.Scan(
			&license.ID,
			&license.Email,
			&license.HostID,
			&license.InstanceID,
			&license.LicenseKey,
			&license.ValidFrom,
			&license.ValidUntil,
			&license.Status,
			&license.CreatedAt,
			&license.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning license: %w", err)
		}
		licenses = append(licenses, &license)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating licenses: %w", err)
	}

	return licenses, nil
}

func (s *Store) GetLicenseByInstanceID(ctx context.Context, tx store.Tx, instanceID string) (*store.License, error) {
	var license store.License

	query := `
        SELECT id, email, host_id, instance_id, license_key, valid_from,
               valid_until, status, created_at, updated_at
        FROM licenses
        WHERE instance_id = $1`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(ctx, query, instanceID).Scan(
		&license.ID,
		&license.Email,
		&license.HostID,
		&license.InstanceID,
		&license.LicenseKey,
		&license.ValidFrom,
		&license.ValidUntil,
		&license.Status,
		&license.CreatedAt,
		&license.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting license by instance ID: %w", err)
	}

	return &license, nil
}
