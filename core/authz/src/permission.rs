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
    /// The describe action is used when non-schema resource metadata is being read.
    Describe,
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

impl TryFrom<Action> for proto::resource_action_permission::Action {
    type Error = IncompatiblePermissionError;

    fn try_from(value: Action) -> Result<Self, Self::Error> {
        match value {
            Action::Create => Ok(Self::Create),
            Action::Delete => Ok(Self::Delete),
            Action::Read => Ok(Self::Read),
            Action::ReadSchema => Ok(Self::ReadSchema),
            Action::Write => Ok(Self::Write),
            _ => Err(IncompatiblePermissionError {}),
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
                    proto::resource_action_permission::ResourceType::try_from(ra.resource_type)
                        .map_err(|_| IncompatiblePermissionError {})?,
                    ra.target,
                )?;
                let a = Action::try_from(
                    proto::resource_action_permission::Action::try_from(ra.action)
                        .map_err(|_| IncompatiblePermissionError {})?,
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
                let (resource_type, target) = r.try_into_proto()?;
                let a: proto::resource_action_permission::Action = a.try_into()?;
                Ok(Self {
                    permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                        proto::ResourceActionPermission {
                            resource_type: resource_type as i32,
                            target,
                            action: a as i32,
                        },
                    )),
                })
            }
        }
    }
}

/// A resource is the object that a request is trying to access.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resource {
    /// A database resource is a InfluxDB database specified by the given [Target] selector.
    Database(Target),
}

impl Resource {
    fn try_from_proto(
        resource_type: proto::resource_action_permission::ResourceType,
        resource_target: Option<proto::resource_action_permission::Target>,
    ) -> Result<Self, IncompatiblePermissionError> {
        match resource_type {
            proto::resource_action_permission::ResourceType::Database => {
                Ok(Self::Database(Target::try_from_proto(resource_target)?))
            }
            _ => Err(IncompatiblePermissionError {}),
        }
    }

    fn try_into_proto(
        self,
    ) -> Result<
        (
            proto::resource_action_permission::ResourceType,
            Option<proto::resource_action_permission::Target>,
        ),
        IncompatiblePermissionError,
    > {
        match self {
            Self::Database(target) => Ok((
                proto::resource_action_permission::ResourceType::Database,
                Some(target.try_into_proto()?),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// A specifier to select which resource a permission is requested for
pub enum Target {
    /// Selects a resource by its name.
    ResourceName(String),
    /// Selects a resource by its unique identifier
    ResourceId(String),
}

impl Target {
    fn try_from_proto(
        target: Option<proto::resource_action_permission::Target>,
    ) -> Result<Self, IncompatiblePermissionError> {
        target
            .map(|target| match target {
                proto::resource_action_permission::Target::ResourceName(name) => {
                    Self::ResourceName(name)
                }
                proto::resource_action_permission::Target::ResourceId(id) => Self::ResourceId(id),
            })
            .ok_or(IncompatiblePermissionError {})
    }

    fn try_into_proto(
        self,
    ) -> Result<proto::resource_action_permission::Target, IncompatiblePermissionError> {
        Ok(match self {
            Self::ResourceName(name) => {
                proto::resource_action_permission::Target::ResourceName(name)
            }
            Self::ResourceId(id) => proto::resource_action_permission::Target::ResourceId(id),
        })
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
            proto::resource_action_permission::Action::try_from(Action::Create).unwrap()
        );
        assert_eq!(
            proto::resource_action_permission::Action::Delete,
            proto::resource_action_permission::Action::try_from(Action::Delete).unwrap()
        );
        assert_eq!(
            proto::resource_action_permission::Action::Read,
            proto::resource_action_permission::Action::try_from(Action::Read).unwrap()
        );
        assert_eq!(
            proto::resource_action_permission::Action::ReadSchema,
            proto::resource_action_permission::Action::try_from(Action::ReadSchema).unwrap()
        );
        assert_eq!(
            proto::resource_action_permission::Action::Write,
            proto::resource_action_permission::Action::try_from(Action::Write).unwrap()
        );
    }

    #[test]
    fn resource_try_from_proto() {
        use proto::resource_action_permission as proto;
        assert_eq!(
            Resource::Database(Target::ResourceName("ns1".to_owned())),
            Resource::try_from_proto(
                proto::ResourceType::Database,
                Some(proto::Target::ResourceName("ns1".to_owned()))
            )
            .unwrap()
        );
        assert_eq!(
            Resource::Database(Target::ResourceId("1234".to_owned())),
            Resource::try_from_proto(
                proto::ResourceType::Database,
                Some(proto::Target::ResourceId("1234".to_owned())),
            )
            .unwrap()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Resource::try_from_proto(proto::ResourceType::Database, None).unwrap_err()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Resource::try_from_proto(
                proto::ResourceType::Unspecified,
                Some(proto::Target::ResourceName("ns1".to_owned())),
            )
            .unwrap_err()
        );
        assert_eq!(
            IncompatiblePermissionError {},
            Resource::try_from_proto(
                proto::ResourceType::Unspecified,
                Some(proto::Target::ResourceId("1234".to_owned())),
            )
            .unwrap_err()
        );
    }

    #[test]
    fn resource_try_into_proto() {
        use proto::resource_action_permission as proto;
        assert_eq!(
            (
                proto::ResourceType::Database,
                Some(proto::Target::ResourceName("ns1".to_owned()))
            ),
            Resource::Database(Target::ResourceName("ns1".to_owned()))
                .try_into_proto()
                .unwrap(),
        );
        assert_eq!(
            (
                proto::ResourceType::Database,
                Some(proto::Target::ResourceId("1234".to_owned()))
            ),
            Resource::Database(Target::ResourceId("1234".to_owned()))
                .try_into_proto()
                .unwrap(),
        );
    }

    #[test]
    fn permission_try_from_proto() {
        assert_eq!(
            Permission::ResourceAction(
                Resource::Database(Target::ResourceName("ns2".to_owned())),
                Action::Create
            ),
            Permission::try_from(proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        target: Some(proto::resource_action_permission::Target::ResourceName(
                            "ns2".to_owned()
                        )),
                        action: 4,
                    }
                ))
            })
            .unwrap()
        );
        assert_eq!(
            Permission::ResourceAction(
                Resource::Database(Target::ResourceId("1234".to_owned())),
                Action::Create
            ),
            Permission::try_from(proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        target: Some(proto::resource_action_permission::Target::ResourceId(
                            "1234".to_owned()
                        )),
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
                        target: Some(proto::resource_action_permission::Target::ResourceName(
                            "ns2".to_owned()
                        )),
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
                        target: Some(proto::resource_action_permission::Target::ResourceName(
                            "ns2".to_owned()
                        )),
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
                        target: Some(proto::resource_action_permission::Target::ResourceName(
                            "ns3".to_owned()
                        )),
                        action: 4,
                    }
                ))
            },
            proto::Permission::try_from(Permission::ResourceAction(
                Resource::Database(Target::ResourceName("ns3".to_owned())),
                Action::Create
            ))
            .unwrap()
        );

        assert_eq!(
            proto::Permission {
                permission_one_of: Some(proto::permission::PermissionOneOf::ResourceAction(
                    proto::ResourceActionPermission {
                        resource_type: 1,
                        target: Some(proto::resource_action_permission::Target::ResourceId(
                            "1234".to_owned()
                        )),
                        action: 4,
                    }
                ))
            },
            proto::Permission::try_from(Permission::ResourceAction(
                Resource::Database(Target::ResourceId("1234".to_owned())),
                Action::Create
            ))
            .unwrap()
        );
    }
}
