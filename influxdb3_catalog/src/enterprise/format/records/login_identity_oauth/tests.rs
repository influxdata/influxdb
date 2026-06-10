//! Tests for OAuth login identity records.

use crate::format::records::assert_roundtrip;

use super::*;

#[test]
fn create_login_identity_oauth_roundtrip() {
    assert_roundtrip!(
        CreateLoginIdentityOAuth {
            user_id: 1,
            oauth_id: "oauth|12345".to_string(),
            created_at: 1234567890,
        },
        "06010b6f617574687c313233343502d2029649"
    );
}

#[test]
fn delete_login_identity_oauth_roundtrip() {
    assert_roundtrip!(DeleteLoginIdentityOAuth { user_id: 1 }, "0601");
}
