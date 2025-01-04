-- filename: 000001_initial_setup.up.postgres.sql
-- description: Initial setup for the InfluxDB 3 Pro license service

-- Table to store users
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    emails_sent_cnt INT NOT NULL DEFAULT 0,
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX idx_users_email ON users(email);

-- Table to track IP addresses associated with users
CREATE TABLE user_ips (
    ipaddr inet,
    user_id BIGSERIAL,
    blocked BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT unique_ipaddr_user_id UNIQUE (ipaddr, user_id)
);

CREATE INDEX idx_user_ips_ipaddr ON user_ips(ipaddr);
CREATE INDEX idx_user_ips_blocked ON user_ips(blocked);

-- Create an enum for valid email states
CREATE TYPE email_state_enum AS ENUM ('scheduled', 'sent', 'failed');

-- Create a table to store emails and partition it on email state
CREATE TABLE emails (
    id BIGSERIAL,
    user_id BIGSERIAL NOT NULL,
    user_ip inet NOT NULL,
    verification_token UUID NOT NULL,
    verification_url TEXT NOT NULL,
    verified_at TIMESTAMP WITH TIME ZONE,
    license_id BIGSERIAL NOT NULL,
    email_template_name VARCHAR(255),
    from_email VARCHAR(255) NOT NULL,
    to_email VARCHAR(255) NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    state email_state_enum NOT NULL DEFAULT 'scheduled',
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMP WITH TIME ZONE, -- Date/time of last successful send
    send_cnt INT NOT NULL DEFAULT 0, -- Count of successful sends
    send_fail_cnt INT NOT NULL DEFAULT 0, -- Count of failed send attempts
    last_err_msg TEXT, -- Error message from last failed send
    delivery_srvc_resp TEXT, -- Response from the mail service (eg, Mailgun)
    delivery_srvc_id VARCHAR(255), -- ID from the mail service for this email
    CONSTRAINT pk_emails PRIMARY KEY (id, state)  -- Named primary key constraint
) PARTITION BY LIST (state);

-- Create partitions
CREATE TABLE emails_scheduled PARTITION OF emails
    FOR VALUES IN ('scheduled');

CREATE TABLE emails_sent PARTITION OF emails
    FOR VALUES IN ('sent');

CREATE TABLE emails_failed PARTITION OF emails
    FOR VALUES IN ('failed');

-- Indexes (created on parent table, inherited by partitions)
CREATE INDEX idx_emails_to_email ON emails(to_email);
CREATE INDEX idx_emails_user_id ON emails(user_id);
CREATE INDEX idx_emails_verification_token ON emails(verification_token);
CREATE INDEX idx_emails_scheduled_at ON emails(scheduled_at)
    WHERE state = 'scheduled';
CREATE INDEX idx_emails_sent_at ON emails(sent_at)
    WHERE state = 'sent';
CREATE INDEX idx_emails_delivery_srvc_id ON emails(delivery_srvc_id)
    WHERE state = 'sent';

-- Enum of valid license states
CREATE TYPE license_state_enum AS ENUM ('requested', 'active', 'inactive');

-- Table to store users' licenses
CREATE TABLE licenses (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGSERIAL NOT NULL,
    email VARCHAR(255) NOT NULL,
    host_id TEXT NOT NULL,
    instance_id UUID NOT NULL,
    license_key TEXT NOT NULL,
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    valid_until TIMESTAMP WITH TIME ZONE NOT NULL,
    state license_state_enum NOT NULL DEFAULT 'requested',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_email_host UNIQUE(email, host_id),      -- host must be unique per email
    CONSTRAINT unique_email_instance_id UNIQUE(email, instance_id)   -- instance must be unique per email
);

CREATE INDEX idx_licenses_instance ON licenses(instance_id);
CREATE INDEX idx_licenses_user_id ON licenses(user_id);
CREATE INDEX idx_licenses_email ON licenses(email);
CREATE INDEX idx_licenses_state ON licenses(state);
