-- description: Reverse initial setup for the InfluxDB 3 Pro license service

-- Drop tables in reverse order (to handle dependencies)

-- Drop license table and its indexes
DROP INDEX IF EXISTS idx_licenses_state;
DROP INDEX IF EXISTS idx_licenses_email;
DROP INDEX IF EXISTS idx_licenses_user_id;
DROP INDEX IF EXISTS idx_licenses_instance;
DROP TABLE IF EXISTS licenses;

-- Drop email partitions and parent table with its indexes
DROP INDEX IF EXISTS idx_emails_delivery_srvc_id;
DROP INDEX IF EXISTS idx_emails_sent_at;
DROP INDEX IF EXISTS idx_emails_scheduled_at;
DROP INDEX IF EXISTS idx_emails_verification_token;
DROP INDEX IF EXISTS idx_emails_user_id;
DROP INDEX IF EXISTS idx_emails_to_email;

DROP TABLE IF EXISTS emails_failed;
DROP TABLE IF EXISTS emails_sent;
DROP TABLE IF EXISTS emails_scheduled;
DROP TABLE IF EXISTS emails;

-- Drop user_ips table and its indexes
DROP INDEX IF EXISTS idx_user_ips_blocked;
DROP INDEX IF EXISTS idx_user_ips_ipaddr;
DROP TABLE IF EXISTS user_ips;

-- Drop users table and its index
DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;

-- Drop custom enum types
DROP TYPE IF EXISTS license_state_enum;
DROP TYPE IF EXISTS email_state_enum;