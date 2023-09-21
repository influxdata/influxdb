use super::proto;
use snafu::Snafu;

/// Action is the type of operation being attempted on a resource.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    /// The create action is used when a new instance of the resource will
    /// be created.
    Create,
    /// The delete action is used when a resource will be deleted.
    Delete,
    /// The read action is used when the data contained by a resource will
    /// be read.
    Read,
    /// The read-schema action is used when only metadata about a resource
    /// will be read.
    ReadSchema,
    /// The write action is used when data is being written to the resource.
    Write,
}

impl TryFrom<proto::resource_action_permission::Action> for Action {
    type Error = IncompatiblePermissionError;

    fn try_from(value: proto::resource_action_permission::Action) -> Result<Self, Self::Error> {
        match value {
            proto::resource_action_permission::Action::ReadSchema => Ok(Self::ReadSchema),
            proto::resource_action_permission::Action::Read => Ok(Self::Read),
            proto::resource_action_permission::Action::Write => Ok(Self::Write),
            proto::resource_action_permission::Action::Create => Ok(Self::Create),
            proto::resource_action_permission::Action::Delete => Ok(Self::Delete),
            _ => Err(IncompatiblePermissionError {}),
        }
    }
}

impl From<Action> for proto::resource_action_permission::Action {
    fn from(value: Action) -> Self {
        match value {
            Action::Create => Self::Create,
            Action::Delete => Self::Delete,
            Action::Read => Self::Read,
            Action::ReadSchema => Self::ReadSchema,
            Action::Write => Self::Write,
        }
    }
}

/// An incompatible-permission-error is the error that is returned if
/// there is an attempt to convert a permssion into a form that is
/// unsupported. For the most part this should not cause an error to
/// be returned to the user, but more as a signal that the conversion
/// can never succeed and therefore the permisison can never be granted.
/// This error will normally be silently dropped along with the source
/// permission that caused it.
#[derive(Clone, Copy, Debug, PartialEq, Snafu)]
#[snafu(display("incompatible permission"))]
pub struct IncompatiblePermissionError {}

/// A permission is an authorization that can be checked with an
/// authorizer. Not all authorizers neccessarily support all forms of
/// permission. If an authorizer doesn't support a permission then it
/// is not an error, the permission will always be denied.
#[derive(Clone, Debug, PartialEq)]
pub enum Permission {
    /// ResourceAction is a permission in the form of a reasource and an
    /// action.
    ResourceAction(Resource, Action),
}

impl TryFrom<proto::Permission> for Permission {
    type Error = IncompatiblePermissionError;

    fn try_from(value: proto::Permission) -> Result<Self, Self::Error> {
        match value.permission_one_of {
            Some(proto::permission::PermissionOneOf::ResourceAction(ra)) => {
                let r = Resource::try_from_proto(
                    proto::resource_action_permission::ResourceType::from_i32(ra.resource_type)
                        .ok_or(IncompatiblePermissionError {})?,
                    ra.resource_id,
                )?;
                let a = Action::try_from(
                    proto::resource_action_permission::Action::from_i32(ra.action)
                        .ok_or(IncompatiblePermissionError {})?,
                )?;
                Ok(Self::ResourceAction(r, a))
            }
            _ => Err(IncompatiblePermissionError {}),
        }
    }
}

impl TryFrom<Permission> for proto::Permission {
    type Error = IncompatiblePermissionError;

    fn try_from(value: Permission) -> Result<Self, Self::Error> {
        match value {
            Permission::ResourceAction(r, a) => {
                let (rt, ri) = r.try_into_proto()?;
                let a: proto::resource_action_permission::Action = a.into();
                Ok(Self {
                    permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                        proto::ResourceActionPermission {
                            resource_type: rt as i32,
                            resource_id: ri,
                            action: a as i32,
                        },
                    )),
                })
            }
        }
    }
}

/// A resource is the object that a request is trying to access.
#[derive(Clone, Debug, PartialEq)]
pub enum Resource {
    /// A database is a named IOx database.
    Database(String),
}

impl Resource {
    fn try_from_proto(
        rt: proto::resource_action_permission::ResourceType,
        ri: Option<String>,
    ) -> Result<Self, IncompatiblePermissionError> {
        match (rt, ri) {
            (proto::resource_action_permission::ResourceType::Database, Some(s)) => {
                Ok(Self::Database(s))
            }
            _ => Err(IncompatiblePermissionError {}),
        }
    }

    fn try_into_proto(
        self,
    ) -> Result<
        (
            proto::resource_action_permission::ResourceType,
            Option<String>,
        ),
        IncompatiblePermissionError,
    > {
        match self {
            Self::Database(s) => Ok((
                proto::resource_action_permission::ResourceType::Database,
                Some(s),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_try_from_proto() {
        assert_eq!(
            Action::Create,
            Action::try_from(proto::resource_action_permission::Action::Create).unwrap(),
        );
        assert_eq!(
            Action::Delete,
            Action::try_from(proto::resource_action_permission::Action::Delete).unwrap(),
        );
        assert_eq!(
            Action::Read,
            Action::try_from(proto::resource_action_permission::Action::Read).unwrap(),
        );
        assert_eq!(
            Action::ReadSchema,
            Action::try_from(proto::resource_action_permission::Action::ReadSchema).unwrap(),
        );
        assert_eq!(
            Action::Write,
            Action::try_from(proto::resource_action_permission::Action::Write).unwrap(),
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Action::try_from(proto::resource_action_permission::Action::Unspecified).unwrap_err(),
        );
    }

    #[test]
    fn action_into_proto() {
        assert_eq!(
            proto::resource_action_permission::Action::Create,
            proto::resource_action_permission::Action::from(Action::Create)
        );
        assert_eq!(
            proto::resource_action_permission::Action::Delete,
            proto::resource_action_permission::Action::from(Action::Delete)
        );
        assert_eq!(
            proto::resource_action_permission::Action::Read,
            proto::resource_action_permission::Action::from(Action::Read)
        );
        assert_eq!(
            proto::resource_action_permission::Action::ReadSchema,
            proto::resource_action_permission::Action::from(Action::ReadSchema)
        );
        assert_eq!(
            proto::resource_action_permission::Action::Write,
            proto::resource_action_permission::Action::from(Action::Write)
        );
    }

    #[test]
    fn resource_try_from_proto() {
        assert_eq!(
            Resource::Database("ns1".into()),
            Resource::try_from_proto(
                proto::resource_action_permission::ResourceType::Database,
                Some("ns1".into())
            )
            .unwrap()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Resource::try_from_proto(
                proto::resource_action_permission::ResourceType::Database,
                None
            )
            .unwrap_err()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Resource::try_from_proto(
                proto::resource_action_permission::ResourceType::Unspecified,
                Some("ns1".into())
            )
            .unwrap_err()
        );
    }

    #[test]
    fn resource_try_into_proto() {
        assert_eq!(
            (
                proto::resource_action_permission::ResourceType::Database,
                Some("ns1".into())
            ),
            Resource::Database("ns1".into()).try_into_proto().unwrap(),
        );
    }

    #[test]
    fn permission_try_from_proto() {
        assert_eq!(
            Permission::ResourceAction(Resource::Database("ns2".into()), Action::Create),
            Permission::try_from(proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        resource_id: Some("ns2".into()),
                        action: 4,
                    }
                ))
            })
            .unwrap()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Permission::try_from(proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 0,
                        resource_id: Some("ns2".into()),
                        action: 4,
                    }
                ))
            })
            .unwrap_err()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Permission::try_from(proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        resource_id: Some("ns2".into()),
                        action: 0,
                    }
                ))
            })
            .unwrap_err()
        );
    }

    #[test]
    fn permission_try_into_proto() {
        assert_eq!(
            proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        resource_id: Some("ns3".into()),
                        action: 4,
                    }
                ))
            },
            proto::Permission::try_from(Permission::ResourceAction(
                Resource::Database("ns3".into()),
                Action::Create
            ))
            .unwrap()
        );
    }
}
