//! Protobuf types for errors from the google standards and
//! conversions to `tonic::Status`

pub use google_types::*;

pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
}

pub mod longrunning {
    include!(concat!(env!("OUT_DIR"), "/google.longrunning.rs"));
}

use self::protobuf::Any;
use prost::{
    bytes::{Bytes, BytesMut},
    Message,
};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;
use tonic::Status;
use tracing::error;

// A newtype struct to provide conversion into tonic::Status
struct EncodeError(prost::EncodeError);

impl From<EncodeError> for tonic::Status {
    fn from(error: EncodeError) -> Self {
        error!(error=%error.0, "failed to serialise error response details");
        tonic::Status::unknown(format!("failed to serialise server error: {}", error.0))
    }
}

impl From<prost::EncodeError> for EncodeError {
    fn from(e: prost::EncodeError) -> Self {
        Self(e)
    }
}

fn encode_status(code: tonic::Code, message: String, details: Any) -> tonic::Status {
    let mut buffer = BytesMut::new();

    let status = rpc::Status {
        code: code as i32,
        message: message.clone(),
        details: vec![details],
    };

    match status.encode(&mut buffer) {
        Ok(_) => tonic::Status::with_details(code, message, buffer.freeze()),
        Err(e) => EncodeError(e).into(),
    }
}

/// Error returned if a request field has an invalid value. Includes
/// machinery to add parent field names for context -- thus it will
/// report `rules.write_timeout` than simply `write_timeout`.
#[derive(Debug, Default, Clone)]
pub struct FieldViolation {
    pub field: String,
    pub description: String,
}

impl FieldViolation {
    pub fn required(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            description: "Field is required".to_string(),
        }
    }

    /// Re-scopes this error as the child of another field
    pub fn scope(self, field: impl Into<String>) -> Self {
        let field = if self.field.is_empty() {
            field.into()
        } else {
            [field.into(), self.field].join(".")
        };

        Self {
            field,
            description: self.description,
        }
    }
}

impl std::error::Error for FieldViolation {}

impl std::fmt::Display for FieldViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Violation for field \"{}\": {}",
            self.field, self.description
        )
    }
}

fn encode_bad_request(violation: Vec<FieldViolation>) -> Result<Any, EncodeError> {
    let mut buffer = BytesMut::new();

    rpc::BadRequest {
        field_violations: violation
            .into_iter()
            .map(|f| rpc::bad_request::FieldViolation {
                field: f.field,
                description: f.description,
            })
            .collect(),
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.BadRequest".to_string(),
        value: buffer.freeze(),
    })
}

impl From<FieldViolation> for tonic::Status {
    fn from(f: FieldViolation) -> Self {
        let message = f.to_string();

        match encode_bad_request(vec![f]) {
            Ok(details) => encode_status(tonic::Code::InvalidArgument, message, details),
            Err(e) => e.into(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct InternalError {}

impl From<InternalError> for tonic::Status {
    fn from(_: InternalError) -> Self {
        tonic::Status::new(tonic::Code::Internal, "Internal Error")
    }
}

#[derive(Debug, Default, Clone)]
pub struct AlreadyExists {
    pub resource_type: String,
    pub resource_name: String,
    pub owner: String,
    pub description: String,
}

fn encode_resource_info(
    resource_type: String,
    resource_name: String,
    owner: String,
    description: String,
) -> Result<Any, EncodeError> {
    let mut buffer = BytesMut::new();

    rpc::ResourceInfo {
        resource_type,
        resource_name,
        owner,
        description,
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.ResourceInfo".to_string(),
        value: buffer.freeze(),
    })
}

impl From<AlreadyExists> for tonic::Status {
    fn from(exists: AlreadyExists) -> Self {
        let message = format!(
            "Resource {}/{} already exists",
            exists.resource_type, exists.resource_name
        );
        match encode_resource_info(
            exists.resource_type,
            exists.resource_name,
            exists.owner,
            exists.description,
        ) {
            Ok(details) => encode_status(tonic::Code::AlreadyExists, message, details),
            Err(e) => e.into(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct NotFound {
    pub resource_type: String,
    pub resource_name: String,
    pub owner: String,
    pub description: String,
}

impl From<NotFound> for tonic::Status {
    fn from(not_found: NotFound) -> Self {
        let message = format!(
            "Resource {}/{} not found",
            not_found.resource_type, not_found.resource_name
        );
        match encode_resource_info(
            not_found.resource_type,
            not_found.resource_name,
            not_found.owner,
            not_found.description,
        ) {
            Ok(details) => encode_status(tonic::Code::NotFound, message, details),
            Err(e) => e.into(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct PreconditionViolation {
    pub category: String,
    pub subject: String,
    pub description: String,
}

fn encode_precondition_failure(violations: Vec<PreconditionViolation>) -> Result<Any, EncodeError> {
    use rpc::precondition_failure::Violation;

    let mut buffer = BytesMut::new();

    rpc::PreconditionFailure {
        violations: violations
            .into_iter()
            .map(|x| Violation {
                r#type: x.category,
                subject: x.subject,
                description: x.description,
            })
            .collect(),
    }
    .encode(&mut buffer)?;

    Ok(Any {
        type_url: "type.googleapis.com/google.rpc.PreconditionFailure".to_string(),
        value: buffer.freeze(),
    })
}

impl From<PreconditionViolation> for tonic::Status {
    fn from(violation: PreconditionViolation) -> Self {
        let message = format!(
            "Precondition violation {} - {}: {}",
            violation.subject, violation.category, violation.description
        );
        match encode_precondition_failure(vec![violation]) {
            Ok(details) => encode_status(tonic::Code::FailedPrecondition, message, details),
            Err(e) => e.into(),
        }
    }
}

/// An extension trait that adds the ability to convert an error
/// that can be converted to a String to a FieldViolation
pub trait FieldViolationExt {
    type Output;

    fn field(self, field: &'static str) -> Result<Self::Output, FieldViolation>;
}

impl<T, E> FieldViolationExt for Result<T, E>
where
    E: ToString,
{
    type Output = T;

    fn field(self, field: &'static str) -> Result<T, FieldViolation> {
        self.map_err(|e| FieldViolation {
            field: field.to_string(),
            description: e.to_string(),
        })
    }
}
