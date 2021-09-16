//! Protobuf types for errors from the google standards and
//! conversions to `tonic::Status`

pub use google_types::*;

pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    include!(concat!(env!("OUT_DIR"), "/google.rpc.serde.rs"));
}

pub mod longrunning {
    include!(concat!(env!("OUT_DIR"), "/google.longrunning.rs"));
    include!(concat!(env!("OUT_DIR"), "/google.longrunning.serde.rs"));

    use crate::google::{FieldViolation, FieldViolationExt};
    use crate::influxdata::iox::management::v1::{OperationMetadata, OPERATION_METADATA};
    use prost::{bytes::Bytes, Message};
    use std::convert::TryFrom;

    impl Operation {
        /// Return the IOx operation `id`. This `id` can
        /// be passed to the various APIs in the
        /// operations client such as `influxdb_iox_client::operations::Client::wait_operation`;
        pub fn id(&self) -> usize {
            self.name
                .parse()
                .expect("Internal error: id returned from server was not an integer")
        }

        /// Decodes an IOx `OperationMetadata` metadata payload
        pub fn iox_metadata(&self) -> Result<OperationMetadata, FieldViolation> {
            let metadata = self
                .metadata
                .as_ref()
                .ok_or_else(|| FieldViolation::required("metadata"))?;

            if !crate::protobuf_type_url_eq(&metadata.type_url, OPERATION_METADATA) {
                return Err(FieldViolation {
                    field: "metadata.type_url".to_string(),
                    description: "Unexpected field type".to_string(),
                });
            }

            Message::decode(Bytes::clone(&metadata.value)).field("metadata.value")
        }
    }

    /// Groups together an `Operation` with a decoded `OperationMetadata`
    ///
    /// When serialized this will serialize the encoded Any field on `Operation` along
    /// with its decoded representation as `OperationMetadata`
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct IoxOperation {
        /// The `Operation` message returned from the API
        pub operation: Operation,
        /// The decoded `Operation::metadata` contained within `IoxOperation::operation`
        pub metadata: OperationMetadata,
    }

    impl TryFrom<Operation> for IoxOperation {
        type Error = FieldViolation;

        fn try_from(operation: Operation) -> Result<Self, Self::Error> {
            Ok(Self {
                metadata: operation.iox_metadata()?,
                operation,
            })
        }
    }
}

use self::protobuf::Any;
use observability_deps::tracing::error;
use prost::{bytes::BytesMut, Message};
use std::convert::TryInto;

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

#[derive(Debug, Default, Clone)]
pub struct QuotaFailure {
    pub subject: String,
    pub description: String,
}

impl From<QuotaFailure> for tonic::Status {
    fn from(quota_failure: QuotaFailure) -> Self {
        tonic::Status::new(
            tonic::Code::ResourceExhausted,
            format!("{}: {}", quota_failure.subject, quota_failure.description),
        )
    }
}

/// An extension trait that adds the method `scope` to any type
/// implementing `TryInto<U, Error = FieldViolation>`
pub(crate) trait FromField<T> {
    fn scope(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromField<U> for T
where
    T: TryInto<U, Error = FieldViolation>,
{
    /// Try to convert type using TryInto calling `FieldViolation::scope`
    /// on any returned error
    fn scope(self, field: impl Into<String>) -> Result<U, FieldViolation> {
        self.try_into().map_err(|e| e.scope(field))
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = FieldViolation>`
pub trait FromFieldOpt<T> {
    /// Try to convert inner type, if any, using TryInto calling
    /// `FieldViolation::scope` on any error encountered
    ///
    /// Returns None if empty
    fn optional(self, field: impl Into<String>) -> Result<Option<T>, FieldViolation>;

    /// Try to convert inner type, using TryInto calling `FieldViolation::scope`
    /// on any error encountered
    ///
    /// Returns an error if empty
    fn required(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromFieldOpt<U> for Option<T>
where
    T: TryInto<U, Error = FieldViolation>,
{
    fn optional(self, field: impl Into<String>) -> Result<Option<U>, FieldViolation> {
        self.map(|t| t.scope(field)).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, FieldViolation> {
        match self {
            None => Err(FieldViolation::required(field)),
            Some(t) => t.scope(field),
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// String
///
/// Prost will default string fields to empty, whereas IOx sometimes
/// uses Option<String>, this helper aids mapping between them
///
/// TODO: Review mixed use of Option<String> and String in IOX
pub(crate) trait FromFieldString {
    /// Returns a Ok if the String is not empty
    fn required(self, field: impl Into<String>) -> Result<String, FieldViolation>;

    /// Wraps non-empty strings in Some(_), returns None for empty strings
    fn optional(self) -> Option<String>;
}

impl FromFieldString for String {
    fn required(self, field: impl Into<Self>) -> Result<String, FieldViolation> {
        if self.is_empty() {
            return Err(FieldViolation::required(field));
        }
        Ok(self)
    }

    fn optional(self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        Some(self)
    }
}

/// An extension trait that adds the method `vec_field` to any Vec of a type
/// implementing `TryInto<U, Error = FieldViolation>`
pub(crate) trait FromFieldVec<T> {
    /// Converts to a `Vec<U>`, short-circuiting on the first error and
    /// returning a correctly scoped `FieldViolation` for where the error
    /// was encountered
    fn vec_field(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromFieldVec<Vec<U>> for Vec<T>
where
    T: TryInto<U, Error = FieldViolation>,
{
    fn vec_field(self, field: impl Into<String>) -> Result<Vec<U>, FieldViolation> {
        let res: Result<_, _> = self
            .into_iter()
            .enumerate()
            .map(|(i, t)| t.scope(i.to_string()))
            .collect();

        res.map_err(|e| e.scope(field))
    }
}
