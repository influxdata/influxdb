package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/influxdata/influxdb_pro/influxdb3_license/service/store"
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

func (s *Store) GetUserByEmail(ctx context.Context, email string) (*store.User, error) {
	tx, err := s.BeginTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	return s.GetUserByEmailTx(ctx, tx, email)
}

func (s *Store) MarkUserAsVerified(ctx context.Context, tx store.Tx, id int64) error {
	query := `
		UPDATE users
		SET verified_at = statement_timestamp()
		WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("marking user as verified: %w", err)
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

func (s *Store) GetUserByEmailTx(ctx context.Context, tx store.Tx, email string) (*store.User, error) {
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
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("getting user by email: %w", err)
	}

	return &user, nil
}

func (s *Store) CreateUserIP(ctx context.Context, tx store.Tx, userIP *store.UserIP) error {
	query := `
        INSERT INTO user_ips (ipaddr, user_id)
        VALUES ($1, $2)
        ON CONFLICT (ipaddr, user_id) DO NOTHING`

	sqlTx := tx.(*sql.Tx)
	_, err := sqlTx.ExecContext(
		ctx, query,
		ipToPostgres(userIP.IPAddr), // Use the new conversion function
		userIP.UserID,
	)

	if err != nil {
		return fmt.Errorf("creating user IP record: %w", err)
	}
	return nil
}

// ipToPostgres converts a net.IP to a format suitable for PostgreSQL inet
func ipToPostgres(ip net.IP) string {
	// Ensure IP is in correct form
	if ip4 := ip.To4(); ip4 != nil {
		// IPv4 address
		return ip4.String()
	}
	// IPv6 address
	return ip.String()
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

		// Use the new conversion function
		ip, err := postgresIPToNet(ipAddrStr)
		if err != nil {
			return nil, fmt.Errorf("converting IP address from database: %w", err)
		}
		userIP.IPAddr = ip

		userIPs = append(userIPs, &userIP)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating user IP records: %w", err)
	}

	return userIPs, nil
}

// postgresIPToNet converts a PostgreSQL inet string to net.IP
func postgresIPToNet(ipStr string) (net.IP, error) {
	// Remove any CIDR notation if present
	if idx := strings.Index(ipStr, "/"); idx != -1 {
		ipStr = ipStr[:idx]
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", ipStr)
	}
	return ip, nil
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

func (s *Store) BlockUserIP(ctx context.Context, tx store.Tx, ipAddr net.IP, userID int64) error {
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

func (s *Store) IsIPBlocked(ctx context.Context, ipAddr net.IP) (bool, error) {
	query := `
        SELECT EXISTS (
            SELECT 1 
            FROM user_ips 
            WHERE ipaddr = $1 
            AND blocked = true
        )`

	var isBlocked bool
	err := s.db.QueryRowContext(ctx, query, ipAddr.String()).Scan(&isBlocked)
	if err != nil {
		return false, fmt.Errorf("checking if IP %s is blocked: %w", ipAddr.String(), err)
	}

	return isBlocked, nil
}

// Email operations
func (s *Store) CreateEmail(ctx context.Context, tx store.Tx, email *store.Email) error {
	query := `
        INSERT INTO emails (
            user_id, user_ip, verification_token, verification_url, license_id, email_template_name,
            from_email, to_email, subject, body
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id, state, scheduled_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		email.UserID,
		email.UserIP.String(),
		email.VerificationToken,
		email.VerificationURL,
		email.LicenseID,
		email.TemplateName,
		email.From,
		email.To,
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
			verification_url = $4,
			verified_at = $5,
			license_id = $6,
			email_template_name = $7,
			from_email = $8,
			to_email = $9,
			subject = $10,
			body = $11,
			state = $12,
			scheduled_at = $13,
			sent_at = $14,
			send_cnt = $15,
			send_fail_cnt = $16,
			last_err_msg = $17,
			delivery_srvc_resp = $18,
			delivery_srvc_id = $19
		WHERE id = $20`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(
		ctx, query,
		email.UserID,
		email.UserIP,
		email.VerificationToken,
		email.VerificationURL,
		email.VerifiedAt,
		email.LicenseID,
		email.TemplateName,
		email.From,
		email.To,
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
	var verifiedAt sql.NullTime
	var sentAt sql.NullTime
	var lastErrMsg sql.NullString
	var deliverySrvcResp sql.NullString
	var deliverySrvcID sql.NullString

	err := sqlTx.QueryRowContext(ctx, `
        SELECT id, user_id, user_ip, verification_token, verification_url, verified_at, license_id,
               email_template_name, from_email, to_email, subject, body,
               state, scheduled_at, sent_at, send_cnt,
               send_fail_cnt, last_err_msg, delivery_srvc_resp, delivery_srvc_id
        FROM emails
        WHERE id = $1`, id).Scan(
		&email.ID,
		&email.UserID,
		&email.UserIP,
		&email.VerificationToken,
		&email.VerificationURL,
		&verifiedAt,
		&email.LicenseID,
		&email.TemplateName,
		&email.From,
		&email.To,
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
	if verifiedAt.Valid {
		email.VerifiedAt = verifiedAt.Time
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

	return &email, nil
}

func (s *Store) GetEmailByVerificationToken(ctx context.Context, tx store.Tx, token string) (*store.Email, error) {
	sqlTx, ok := tx.(*sql.Tx)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type: %T", tx)
	}

	var email store.Email
	var verifiedAt sql.NullTime
	var sentAt sql.NullTime
	var lastErrMsg sql.NullString
	var deliverySrvcResp sql.NullString
	var deliverySrvcID sql.NullString

	err := sqlTx.QueryRowContext(ctx, `
		SELECT id, user_id, user_ip, verification_token, verification_url, verified_at, license_id,
			   email_template_name, from_email, to_email, subject, body,
			   state, scheduled_at, sent_at, send_cnt,
			   send_fail_cnt, last_err_msg, delivery_srvc_resp, delivery_srvc_id
		FROM emails
		WHERE verification_token = $1`, token).Scan(
		&email.ID,
		&email.UserID,
		&email.UserIP,
		&email.VerificationToken,
		&email.VerificationURL,
		&verifiedAt,
		&email.LicenseID,
		&email.TemplateName,
		&email.From,
		&email.To,
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
		return nil, fmt.Errorf("GetEmailByVerificationToken: %w", err)
	}

	// Only set optional fields if we got valid data from the DB
	if verifiedAt.Valid {
		email.VerifiedAt = verifiedAt.Time
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
        SELECT id, user_id, user_ip, verification_token, verification_url,
		verified_at, license_id, email_template_name, from_email, to_email,
		subject, body, state, scheduled_at, sent_at, send_cnt, send_fail_cnt,
		last_err_msg, delivery_srvc_resp, delivery_srvc_id
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

	var verifiedAt sql.NullTime
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
			&email.VerificationURL,
			&verifiedAt,
			&email.LicenseID,
			&email.TemplateName,
			&email.From,
			&email.To,
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

		if verifiedAt.Valid {
			email.VerifiedAt = verifiedAt.Time
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

func (s *Store) MarkEmailAsVerified(ctx context.Context, tx store.Tx, token string) (store.VerificationStatus, error) {
	// First check if the email exists and its current verification status
	query := `
        UPDATE emails 
        SET verified_at = CASE
            WHEN verified_at IS NULL THEN statement_timestamp()
            ELSE verified_at
        END
        WHERE verification_token = $1
        RETURNING (verified_at = statement_timestamp()) as newly_verified`

	sqlTx := tx.(*sql.Tx)
	var newlyVerified bool
	err := sqlTx.QueryRowContext(ctx, query, token).Scan(&newlyVerified)

	if err == sql.ErrNoRows {
		return store.EmailNotFound, nil
	}
	if err != nil {
		return store.EmailNotFound, fmt.Errorf("marking email as verified: %w", err)
	}

	if newlyVerified {
		return store.EmailVerified, nil
	}
	return store.EmailAlreadyVerified, nil
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
            user_id, email, writer_id, instance_id, license_key, valid_from, valid_until
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        RETURNING id, state, created_at, updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		license.UserID,
		license.Email,
		license.WriterID,
		license.InstanceID,
		license.LicenseKey,
		license.ValidFrom,
		license.ValidUntil,
	).Scan(
		&license.ID,
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
        SET user_id = $1, email = $2, writer_id = $3, instance_id = $4, license_key = $5,
            valid_from = $6, valid_until = $7, state = $8, updated_at = statement_timestamp()
        WHERE id = $9
        RETURNING updated_at`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(
		ctx, query,
		license.UserID,
		license.Email,
		license.WriterID,
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
        SELECT id, user_id, email, writer_id, instance_id, license_key, valid_from,
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
			&license.UserID,
			&license.Email,
			&license.WriterID,
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

func (s *Store) GetLicenseByEmailAndWriterID(ctx context.Context, tx store.Tx, email, writerID string) (*store.License, error) {
	doCommit := false
	var err error
	if tx == nil {
		tx, err = s.BeginTx(ctx)
		if err != nil {
			return nil, fmt.Errorf("beginning transaction: %w", err)
		}
		defer func() {
			_ = tx.Rollback()
		}()
		doCommit = true
	}

	var license store.License

	query := `
		SELECT id, user_id, email, writer_id, instance_id, license_key, valid_from,
			   valid_until, state, created_at, updated_at
		FROM licenses
		WHERE email = $1 AND writer_id = $2`

	sqlTx := tx.(*sql.Tx)
	err = sqlTx.QueryRowContext(ctx, query, email, writerID).Scan(
		&license.ID,
		&license.UserID,
		&license.Email,
		&license.WriterID,
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
		return nil, fmt.Errorf("getting license by writer ID: %w", err)
	}

	if doCommit {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("committing transaction: %w", err)
		}
	}

	return &license, nil
}

func (s *Store) GetLicensesByEmailAndWriterID(ctx context.Context, tx store.Tx, email, writerID string) ([]*store.License, error) {
	query := `
		SELECT id, user_id, email, writer_id, instance_id, license_key, valid_from,
			   valid_until, state, created_at, updated_at
		FROM licenses
		WHERE email = $1 AND writer_id = $2`

	sqlTx := tx.(*sql.Tx)
	rows, err := sqlTx.QueryContext(ctx, query, email, writerID)
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
			&license.UserID,
			&license.Email,
			&license.WriterID,
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
	doCommit := false
	var err error
	if tx == nil {
		tx, err = s.BeginTx(ctx)
		if err != nil {
			return nil, fmt.Errorf("beginning transaction: %w", err)
		}
		defer func() {
			_ = tx.Rollback()
		}()
		doCommit = true
	}

	var license store.License

	query := `
        SELECT id, user_id, email, writer_id, instance_id, license_key, valid_from,
               valid_until, state, created_at, updated_at
        FROM licenses
        WHERE instance_id = $1`

	sqlTx := tx.(*sql.Tx)
	err = sqlTx.QueryRowContext(ctx, query, instanceID).Scan(
		&license.ID,
		&license.UserID,
		&license.Email,
		&license.WriterID,
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

	if doCommit {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("committing transaction: %w", err)
		}
	}

	return &license, nil
}

func (s *Store) GetLicenseCntByUserID(ctx context.Context, tx store.Tx, userID int64) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM licenses
		WHERE user_id = $1`

	sqlTx := tx.(*sql.Tx)
	var count int64
	err := sqlTx.QueryRowContext(ctx, query, userID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("getting license count by user ID: %w", err)
	}

	return count, nil
}

func (s *Store) GetLicenseByID(ctx context.Context, tx store.Tx, id int64) (*store.License, error) {
	var license store.License

	query := `
		SELECT id, user_id, email, writer_id, instance_id, license_key, valid_from,
			   valid_until, state, created_at, updated_at
		FROM licenses
		WHERE id = $1`

	sqlTx := tx.(*sql.Tx)
	err := sqlTx.QueryRowContext(ctx, query, id).Scan(
		&license.ID,
		&license.UserID,
		&license.Email,
		&license.WriterID,
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
		return nil, fmt.Errorf("getting license by ID: %w", err)
	}

	return &license, nil
}

func (s *Store) SetLicenseState(ctx context.Context, tx store.Tx, id int64, state store.LicenseState) error {
	query := `
		UPDATE licenses
		SET state = $1
		WHERE id = $2`

	sqlTx := tx.(*sql.Tx)
	result, err := sqlTx.ExecContext(ctx, query, state, id)
	if err != nil {
		return fmt.Errorf("setting license state: %w", err)
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

func (s *Store) DeactivateExpiredLicenses(ctx context.Context, tx store.Tx) error {
	query := `
		UPDATE licenses
		SET state = 'inactive', updated_at = NOW()
		WHERE valid_until < NOW()`

	sqlTx := tx.(*sql.Tx)
	if _, err := sqlTx.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("deactivating expired licenses: %w", err)
	}

	return nil
}
