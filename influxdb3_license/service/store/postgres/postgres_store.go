package postgres

import (
	"context"
	"database/sql"
	"errors"
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
        INSERT INTO users (email, emails_sent_cnt, verified_at, deleted_at)
        VALUES ($1, $2, $3, $4)
        RETURNING id, created_at, updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		user.Email,
		user.EmailsSentCount,
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
        SET email = $1, emails_sent_cnt = $2,
            verified_at = $3, deleted_at = $4, updated_at = statement_timestamp()
        WHERE id = $5
        RETURNING updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		user.Email,
		user.EmailsSentCount,
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
        SELECT id, email, emails_sent_cnt,
               verified_at, created_at, updated_at
        FROM users
        WHERE email = $1 AND deleted_at IS NULL`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.EmailsSentCount,
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
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

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
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

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

func (s *Store) BlockUserAndIP(ctx context.Context, tx store.Tx, ipAddr net.IP, userID int64) error {
	// First select for update to lock the row
	query := `
        SELECT ipaddr
        FROM user_ips
        WHERE ipaddr = $1 AND user_id = $2
        FOR UPDATE`

	sqlTx := tx.(*sql.Tx)
	row := sqlTx.QueryRowContext(ctx, query, ipAddr.String(), userID)
	if err := row.Scan(&ipAddr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("no user IP record found for IP %s and user ID %d", ipAddr.String(), userID)
		}
		return fmt.Errorf("selecting user IP for update: %w", err)
	}

	// Now update the blocked status
	updateQuery := `
        UPDATE user_ips
        SET blocked = true
        WHERE ipaddr = $1 AND user_id = $2`

	result, err := sqlTx.ExecContext(ctx, updateQuery, ipAddr.String(), userID)
	if err != nil {
		return fmt.Errorf("blocking user IP: %w", err)
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

// Email operations
func (s *Store) CreateEmail(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        INSERT INTO emails (
            user_id, user_ip, verification_token, license_id, email_template_name,
            to_email, subject, body
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, state, scheduled_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		email.UserID,
		email.UserIP.String(),
		email.VerificationToken,
		email.LicenseID,
		email.TemplateName,
		email.ToEmail,
		email.Subject,
		email.Body,
	).Scan(&email.ID, &email.State, &email.ScheduledAt)

	if err != nil {
		return fmt.Errorf("creating email: %w", err)
	}
	return nil
}

func (s *Store) UpdateEmail(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        UPDATE emails
        SET user_id = $1,
            user_ip = $2,
            verification_token = $3,
            license_id = $4,
            email_template_name = $5,
            to_email = $6,
            subject = $7,
            body = $8,
            state = $9,
            scheduled_at = $10,
            sent_at = $11,
            send_cnt = $12,
            send_fail_cnt = $13,
            last_err_msg = $14,
            delivery_srvc_resp = $15,
            delivery_srvc_id = $16
        WHERE id = $17`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(
		ctx, query,
		email.UserID,
		email.UserIP,
		email.VerificationToken,
		email.LicenseID,
		email.TemplateName,
		email.ToEmail,
		email.Subject,
		email.Body,
		email.State,
		email.ScheduledAt,
		email.SentAt,
		email.SendCnt,
		email.SendFailCnt,
		email.LastErrMsg,
		email.DeliverySrvcResp,
		email.DeliverSrvcID,
		email.ID,
	)

	if err != nil {
		return fmt.Errorf("updating email: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no email found with id %d", email.ID)
	}
	return nil
}

func (s *Store) DeleteEmail(ctx context.Context, tx store.Tx, id int64) error {
	query := `DELETE FROM emails WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("deleting email: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no email found with id %d", id)
	}
	return nil
}

func (s *Store) DeleteAllEmails(ctx context.Context, tx store.Tx) error {
	query := `DELETE FROM emails`
	sqlTx := tx.(*sql.Tx)
	_, err := sqlTx.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("deleting all emails: %w", err)
	}
	return nil
}

// GetEmailByID retrieves an email by its ID
func (s *Store) GetEmailByID(ctx context.Context, tx store.Tx, id int64) (*store.Email, error) {
	sqlTx, ok := tx.(*sql.Tx)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type: %T", tx)
	}

	var email store.Email
	// Use sql.NullTime for nullable timestamp fields
	var sentAt sql.NullTime
	var lastErrMsg sql.NullString
	var deliverySrvcResp sql.NullString
	var deliverySrvcID sql.NullString

	err := sqlTx.QueryRowContext(ctx, `
        SELECT id, user_id, user_ip, verification_token, license_id,
               email_template_name, to_email, subject, body,
               state, scheduled_at, sent_at, send_cnt,
               send_fail_cnt, last_err_msg, delivery_srvc_resp, delivery_srvc_id
        FROM emails
        WHERE id = $1`, id).Scan(
		&email.ID,
		&email.UserID,
		&email.UserIP,
		&email.VerificationToken,
		&email.LicenseID,
		&email.TemplateName,
		&email.ToEmail,
		&email.Subject,
		&email.Body,
		&email.State,
		&email.ScheduledAt,
		&sentAt,
		&email.SendCnt,
		&email.SendFailCnt,
		&lastErrMsg,
		&deliverySrvcResp,
		&deliverySrvcID,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetEmailByID: %w", err)
	}

	// Only set optional fields if we got valid data from the DB
	if sentAt.Valid {
		email.SentAt = sentAt.Time
	}
	if lastErrMsg.Valid {
		email.LastErrMsg = lastErrMsg.String
	}
	if deliverySrvcResp.Valid {
		email.DeliverySrvcResp = deliverySrvcResp.String
	}
	if deliverySrvcID.Valid {
		email.DeliverSrvcID = deliverySrvcID.String
	}

	return &email, nil
}

func (s *Store) GetScheduledEmailCnt(ctx context.Context, tx store.Tx) (int64, error) {
	sqlTx, ok := tx.(*sql.Tx)
	if !ok {
		return 0, fmt.Errorf("invalid transaction type: %T", tx)
	}

	var count int64
	query := `
		SELECT COUNT(*)
		FROM emails
		WHERE state = 'scheduled'
		  AND scheduled_at <= NOW()`

	err := sqlTx.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("counting scheduled emails: %w", err)
	}

	return count, nil
}

func (s *Store) GetScheduledEmailBatch(ctx context.Context, tx store.Tx, batchSize int) ([]*store.Email, error) {
	query := `
        SELECT id, user_id, user_ip, verification_token, license_id, email_template_name, to_email, subject, body,
               state, scheduled_at, sent_at, send_cnt, send_fail_cnt, last_err_msg, delivery_srvc_resp, delivery_srvc_id
        FROM emails
        WHERE state = 'scheduled'
          AND scheduled_at <= NOW()
        ORDER BY scheduled_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("querying scheduled emails: %w", err)
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)

	var sentAt sql.NullTime
	var lastErrMsg sql.NullString
	var deliverySrvcResp sql.NullString
	var deliverySrvcID sql.NullString

	var emails []*store.Email
	for rows.Next() {
		var email store.Email
		err := rows.Scan(
			&email.ID,
			&email.UserID,
			&email.UserIP,
			&email.VerificationToken,
			&email.LicenseID,
			&email.TemplateName,
			&email.ToEmail,
			&email.Subject,
			&email.Body,
			&email.State,
			&email.ScheduledAt,
			&sentAt,
			&email.SendCnt,
			&email.SendFailCnt,
			&lastErrMsg,
			&deliverySrvcResp,
			&deliverySrvcID,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning email row: %w", err)
		}

		if sentAt.Valid {
			email.SentAt = sentAt.Time
		}
		if lastErrMsg.Valid {
			email.LastErrMsg = lastErrMsg.String
		}
		if deliverySrvcResp.Valid {
			email.DeliverySrvcResp = deliverySrvcResp.String
		}
		if deliverySrvcID.Valid {
			email.DeliverSrvcID = deliverySrvcID.String
		}

		emails = append(emails, &email)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating email rows: %w", err)
	}

	return emails, nil
}

func (s *Store) MarkEmailAsSent(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        UPDATE emails
        SET state = 'sent',
            sent_at = $1,
            send_cnt = send_cnt + 1,
            delivery_srvc_resp = $2,
            delivery_srvc_id = $3
        WHERE id = $4
          AND state = 'scheduled'`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query,
		email.SentAt,
		email.DeliverySrvcResp,
		email.DeliverSrvcID,
		email.ID,
	)
	if err != nil {
		return fmt.Errorf("marking email as sent: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no scheduled email found with id %d", email.ID)
	}
	return nil
}

func (s *Store) MarkEmailAsFailed(ctx context.Context, tx store.Tx, id int64, errorMsg string) error {
	query := `
        UPDATE emails
        SET state = 'failed',
            send_fail_cnt = send_cnt + 1,
            last_err_msg = $1
        WHERE id = $2
          AND state = 'scheduled'`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, errorMsg, id)
	if err != nil {
		return fmt.Errorf("marking email as failed: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no scheduled email found with id %d", id)
	}
	return nil
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
                  valid_from, valid_until, state, created_at, updated_at`

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
		&license.State,
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
            valid_from = $5, valid_until = $6, state = $7, updated_at = statement_timestamp()
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
		license.State,
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
               valid_until, state, created_at, updated_at
        FROM licenses
        WHERE email = $1`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, email)
	if err != nil {
		return nil, fmt.Errorf("querying licenses: %w", err)
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

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
			&license.State,
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
               valid_until, state, created_at, updated_at
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
		&license.State,
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
