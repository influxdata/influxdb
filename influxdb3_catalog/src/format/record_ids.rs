//! Record ID constants for catalog records.
//!
//! # Rules
//!
//! 1. Never delete a record ID — once assigned, it persists forever.
//! 2. Never reorder — the raw value is persisted in catalog files.
//! 3. Assign sequentially within each partition (core / enterprise).
//! 4. Deprecate, don't remove — keep the record in the registry but stop
//!    producing it on the write path.
//!
//! # Core vs Enterprise
//!
//! Core records use `RecordId::core(seq)` (bit 15 = 0).
//! Enterprise records use `RecordId::enterprise(seq)` (bit 15 = 1).
//! See the design doc for the full rationale.

use super::RecordId;

/// Reserved for unknown/invalid operations (never written to files).
#[allow(dead_code)]
pub(crate) const UNKNOWN: u16 = 0;

// --- Core records ---

// Cluster-wide feature-level advancement (the cluster-state record sits at
// the start of the partition, ahead of any feature-bearing record).
pub(crate) const ADVANCE_FEATURE_LEVEL: RecordId = RecordId::core(1);

// Node operations
pub(crate) const REGISTER_NODE: RecordId = RecordId::core(2);
pub(crate) const STOP_NODE: RecordId = RecordId::core(3);

// Database operations
pub(crate) const CREATE_DATABASE: RecordId = RecordId::core(4);
pub(crate) const SOFT_DELETE_DATABASE: RecordId = RecordId::core(5);

// Table operations
pub(crate) const CREATE_TABLE: RecordId = RecordId::core(6);
pub(crate) const SOFT_DELETE_TABLE: RecordId = RecordId::core(7);
pub(crate) const ADD_COLUMNS: RecordId = RecordId::core(8);

// Cache operations
pub(crate) const CREATE_DISTINCT_CACHE: RecordId = RecordId::core(9);
pub(crate) const DELETE_DISTINCT_CACHE: RecordId = RecordId::core(10);
pub(crate) const CREATE_LAST_CACHE: RecordId = RecordId::core(11);
pub(crate) const DELETE_LAST_CACHE: RecordId = RecordId::core(12);

// Trigger operations
pub(crate) const CREATE_TRIGGER: RecordId = RecordId::core(13);
pub(crate) const DELETE_TRIGGER: RecordId = RecordId::core(14);
pub(crate) const ENABLE_TRIGGER: RecordId = RecordId::core(15);
pub(crate) const DISABLE_TRIGGER: RecordId = RecordId::core(16);

// Database retention
pub(crate) const SET_DB_RETENTION_PERIOD: RecordId = RecordId::core(17);
pub(crate) const CLEAR_DB_RETENTION_PERIOD: RecordId = RecordId::core(18);

// Token operations (admin only — resource-scoped tokens are enterprise)
pub(crate) const CREATE_ADMIN_TOKEN: RecordId = RecordId::core(19);
pub(crate) const REGENERATE_ADMIN_TOKEN: RecordId = RecordId::core(20);
pub(crate) const DELETE_TOKEN: RecordId = RecordId::core(21);

// Hard delete operations
pub(crate) const DELETE_DATABASE: RecordId = RecordId::core(22);
pub(crate) const DELETE_TABLE: RecordId = RecordId::core(23);

// Config operations
pub(crate) const SET_GENERATION_DURATION: RecordId = RecordId::core(24);
pub(crate) const SET_STORAGE_MODE: RecordId = RecordId::core(25);

// Repository id counters
pub(crate) const SET_NEXT_ID: RecordId = RecordId::core(26);

// User operations (OAuth login identities are enterprise)
pub(crate) const CREATE_USER: RecordId = RecordId::core(27);
pub(crate) const UPDATE_USER_DISPLAY_NAME: RecordId = RecordId::core(28);
pub(crate) const DELETE_USER: RecordId = RecordId::core(29);
pub(crate) const RESTORE_USER: RecordId = RecordId::core(30);
pub(crate) const CREATE_LOGIN_IDENTITY_USERNAME_PASSWORD: RecordId = RecordId::core(31);
pub(crate) const UPDATE_LOGIN_IDENTITY_PASSWORD_HASH: RecordId = RecordId::core(32);
pub(crate) const UPDATE_LOGIN_IDENTITY_REQUIRES_PASSWORD_RESET: RecordId = RecordId::core(33);
pub(crate) const DELETE_LOGIN_IDENTITY_USERNAME_PASSWORD: RecordId = RecordId::core(34);
pub(crate) const CREATE_REFRESH_TOKEN: RecordId = RecordId::core(35);
pub(crate) const REVOKE_REFRESH_TOKEN: RecordId = RecordId::core(36);
pub(crate) const REVOKE_ALL_REFRESH_TOKENS_FOR_USER: RecordId = RecordId::core(37);
pub(crate) const UPDATE_USER_ROLES: RecordId = RecordId::core(38);

// Role operations
pub(crate) const CREATE_ROLE: RecordId = RecordId::core(39);
pub(crate) const UPDATE_ROLE_PERMISSIONS: RecordId = RecordId::core(40);
pub(crate) const UPDATE_ROLE: RecordId = RecordId::core(41);
pub(crate) const DELETE_ROLE: RecordId = RecordId::core(42);

// Node lifecycle (request-stop / ack-stop / remove / unregister)
pub(crate) const REQUEST_STOP_NODE: RecordId = RecordId::core(43);
pub(crate) const ACK_STOP_NODE: RecordId = RecordId::core(44);
pub(crate) const REMOVE_NODE: RecordId = RecordId::core(45);
pub(crate) const UNREGISTER_NODE: RecordId = RecordId::core(46);

// --- Enterprise records ---

// Resource-scoped tokens
pub(crate) const CREATE_RESOURCE_SCOPED_TOKEN: RecordId = RecordId::enterprise(1);

// Table-level retention
pub(crate) const SET_TABLE_RETENTION_PERIOD: RecordId = RecordId::enterprise(2);
pub(crate) const CLEAR_TABLE_RETENTION_PERIOD: RecordId = RecordId::enterprise(3);

// OAuth login identities (enterprise-only feature)
pub(crate) const CREATE_LOGIN_IDENTITY_OAUTH: RecordId = RecordId::enterprise(4);
pub(crate) const DELETE_LOGIN_IDENTITY_OAUTH: RecordId = RecordId::enterprise(5);

// Restore (replaces catalog state from a backup)
pub(crate) const RESTORE_CATALOG: RecordId = RecordId::enterprise(6);

// Query groups
pub(crate) const CREATE_QUERY_GROUP: RecordId = RecordId::enterprise(7);
pub(crate) const UPDATE_QUERY_GROUP: RecordId = RecordId::enterprise(8);
pub(crate) const DELETE_QUERY_GROUP: RecordId = RecordId::enterprise(9);
