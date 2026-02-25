use super::Permission;

/// Authorization information from a token. This contains the subset of
/// the requested permissions that are allowed by the token along with
/// additional information from the token.
#[derive(Debug)]
pub struct Authorization {
    subject: Option<String>,
    permissions: Vec<Permission>,
}

impl Authorization {
    /// Create a new Authorization object
    pub fn new(subject: Option<String>, permissions: Vec<Permission>) -> Self {
        Self {
            subject,
            permissions,
        }
    }

    /// Get the subject of the authorization, if there is one.
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    /// Take the subject from the authorization.
    pub fn into_subject(self) -> Option<String> {
        self.subject
    }

    /// Get the subset of requested permissions that were granted by the
    /// token. Error::Forbidden is returned if the authorization
    /// has no granted permissions.
    pub fn permissions(&self) -> &Vec<Permission> {
        &self.permissions
    }
}

impl From<Authorization> for Vec<Permission> {
    fn from(v: Authorization) -> Self {
        v.permissions
    }
}
