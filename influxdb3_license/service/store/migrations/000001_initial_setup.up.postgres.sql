-- filename: 000001_initial_setup.up.postgres.sql
-- description: Initial setup for the InfluxDB 3 Pro license service

-- Table to store users
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    emails_sent_cnt INT NOT NULL DEFAULT 0,
    verification_token UUID NOT NULL,
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

CREATE UNIQUE INDEX idx_users_email ON users(email);
CREATE UNIQUE INDEX idx_users_verification_token ON users(verification_token);

-- Table to track IP addresses associated with users
CREATE TABLE user_ips (
    ipaddr inet,
    user_id BIGSERIAL,
    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX idx_user_ips_ipaddr ON user_ips(ipaddr);

-- Table to store emails sent to users
CREATE TABLE emails_sent (
    id BIGSERIAL PRIMARY KEY,
    license_id BIGSERIAL NOT NULL,
    email_template_name VARCHAR(255),
    to_email VARCHAR(255) NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    sent_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_emails_sent_to_email ON emails_sent(to_email);
CREATE INDEX idx_emails_sent_sent_at ON emails_sent(sent_at);

-- Table to store users' licenses
CREATE TYPE status_enum AS ENUM ('requested', 'active', 'inactive');

CREATE TABLE licenses (
    id BIGSERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    host_id TEXT NOT NULL,
    instance_id UUID NOT NULL,
    license_key TEXT NOT NULL,
    valid_from TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    valid_until TIMESTAMP WITH TIME ZONE NOT NULL,
    status status_enum NOT NULL DEFAULT 'requested',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_email_host UNIQUE(email, host_id),      -- host must be unique per email
    CONSTRAINT unique_email_instance_id UNIQUE(email, instance_id)   -- instance must be unique per email
);

CREATE INDEX idx_licenses_instance ON licenses(instance_id);
CREATE INDEX idx_licenses_email ON licenses(email);
