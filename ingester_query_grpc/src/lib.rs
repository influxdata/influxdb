// This crate deliberately does not use the same linting rules as the other
// crates because of all the generated code it contains that we don't have much
// control over.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_debug_implementations,
    unused_crate_dependencies
)]
#![allow(clippy::derive_partial_eq_without_eq, clippy::needless_borrow)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use crate::influxdata::iox::ingester::v1 as proto;
use crate::influxdata::iox::ingester::v2 as proto2;
use base64::{prelude::BASE64_STANDARD, Engine};
use data_types::{
    NamespaceId, PartitionHashId, PartitionId, TableId, TimestampRange, TransitionPartitionId,
};
use datafusion::{common::DataFusionError, prelude::Expr};
use datafusion_proto::bytes::Serializeable;
use predicate::{Predicate, ValueExpr};
use prost::Message;
use snafu::{ResultExt, Snafu};

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
#[allow(clippy::use_self)]
pub mod influxdata {
    pub mod iox {
        pub mod ingester {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.ingester.v1.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.ingester.v1.serde.rs"
                ));
            }

            pub mod v2 {
                // generated code violates a few lints, so opt-out of them
                #![allow(clippy::future_not_send)]

                include!(concat!(env!("OUT_DIR"), "/influxdata.iox.ingester.v2.rs"));
                include!(concat!(
                    env!("OUT_DIR"),
                    "/influxdata.iox.ingester.v2.serde.rs"
                ));
            }
        }
    }
}

pub mod arrow_serde;

/// Error returned if a request field has an invalid value. Includes
/// machinery to add parent field names for context -- thus it will
/// report `rules.write_timeout` than simply `write_timeout`.
#[derive(Debug, Default, Clone, PartialEq)]
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

fn expr_to_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error converting Expr to bytes: {e}"),
    }
}

fn expr_from_bytes_violation(field: impl Into<String>, e: DataFusionError) -> FieldViolation {
    FieldViolation {
        field: field.into(),
        description: format!("Error creating Expr from bytes: {e}"),
    }
}

/// Request from the querier service to the ingester service
#[derive(Debug, PartialEq, Clone)]
pub struct IngesterQueryRequest {
    /// namespace to search
    pub namespace_id: NamespaceId,

    /// Table to search
    pub table_id: TableId,

    /// Columns the query service is interested in
    pub columns: Vec<String>,

    /// Predicate for filtering
    pub predicate: Option<Predicate>,
}

impl IngesterQueryRequest {
    /// Make a request to return data for a specified table
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        predicate: Option<Predicate>,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            columns,
            predicate,
        }
    }
}

impl TryFrom<proto::IngesterQueryRequest> for IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(proto: proto::IngesterQueryRequest) -> Result<Self, Self::Error> {
        let proto::IngesterQueryRequest {
            namespace_id,
            table_id,
            columns,
            predicate,
        } = proto;

        let namespace_id = NamespaceId::new(namespace_id);
        let table_id = TableId::new(table_id);
        let predicate = predicate.map(TryInto::try_into).transpose()?;

        Ok(Self::new(namespace_id, table_id, columns, predicate))
    }
}

impl TryFrom<IngesterQueryRequest> for proto::IngesterQueryRequest {
    type Error = FieldViolation;

    fn try_from(query: IngesterQueryRequest) -> Result<Self, Self::Error> {
        let IngesterQueryRequest {
            namespace_id,
            table_id,
            columns,
            predicate,
        } = query;

        Ok(Self {
            namespace_id: namespace_id.get(),
            table_id: table_id.get(),
            columns,
            predicate: predicate.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Request from the querier service to the ingester service
#[derive(Debug, PartialEq, Clone)]
pub struct IngesterQueryRequest2 {
    /// namespace to search
    pub namespace_id: NamespaceId,

    /// Table to search
    pub table_id: TableId,

    /// Columns the query service is interested in
    pub columns: Vec<String>,

    /// Predicate for filtering
    pub filters: Vec<Expr>,
}

impl IngesterQueryRequest2 {
    /// Make a request to return data for a specified table
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        columns: Vec<String>,
        filters: Vec<Expr>,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            columns,
            filters,
        }
    }
}

impl TryFrom<proto2::QueryRequest> for IngesterQueryRequest2 {
    type Error = FieldViolation;

    fn try_from(proto: proto2::QueryRequest) -> Result<Self, Self::Error> {
        let proto2::QueryRequest {
            namespace_id,
            table_id,
            columns,
            filters,
        } = proto;

        let namespace_id = NamespaceId::new(namespace_id);
        let table_id = TableId::new(table_id);
        let filters = filters
            .map(TryInto::try_into)
            .transpose()?
            .unwrap_or_default();

        Ok(Self::new(namespace_id, table_id, columns, filters))
    }
}

impl TryFrom<IngesterQueryRequest2> for proto2::QueryRequest {
    type Error = FieldViolation;

    fn try_from(query: IngesterQueryRequest2) -> Result<Self, Self::Error> {
        let IngesterQueryRequest2 {
            namespace_id,
            table_id,
            columns,
            filters,
        } = query;

        Ok(Self {
            namespace_id: namespace_id.get(),
            table_id: table_id.get(),
            columns,
            filters: Some(filters.try_into()?),
        })
    }
}

impl TryFrom<Predicate> for proto::Predicate {
    type Error = FieldViolation;

    fn try_from(pred: Predicate) -> Result<Self, Self::Error> {
        let Predicate {
            field_columns,
            range,
            exprs,
            value_expr,
        } = pred;

        let field_columns = field_columns.into_iter().flatten().collect();
        let range = range.map(|r| proto::TimestampRange {
            start: r.start(),
            end: r.end(),
        });

        let exprs = exprs
            .iter()
            .map(|expr| {
                expr.to_bytes()
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| expr_to_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let value_expr = value_expr
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            field_columns,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<proto::Predicate> for Predicate {
    type Error = FieldViolation;

    fn try_from(proto: proto::Predicate) -> Result<Self, Self::Error> {
        let proto::Predicate {
            field_columns,
            range,
            exprs,
            value_expr,
        } = proto;

        let field_columns = if field_columns.is_empty() {
            None
        } else {
            Some(field_columns.into_iter().collect())
        };

        let range = range.map(|r| TimestampRange::new(r.start, r.end));

        let exprs = exprs
            .into_iter()
            .map(|bytes| {
                Expr::from_bytes_with_registry(&bytes, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("exprs", e))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let value_expr = value_expr
            .into_iter()
            .map(|ve| {
                let expr = Expr::from_bytes_with_registry(&ve.expr, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("value_expr.expr", e))?;
                // try to convert to ValueExpr
                expr.try_into().map_err(|e| FieldViolation {
                    field: "expr".into(),
                    description: format!("Internal: Serialized expr a valid ValueExpr: {e:?}"),
                })
            })
            .collect::<Result<Vec<ValueExpr>, FieldViolation>>()?;

        Ok(Self {
            field_columns,
            range,
            exprs,
            value_expr,
        })
    }
}

impl TryFrom<ValueExpr> for proto::ValueExpr {
    type Error = FieldViolation;

    fn try_from(value_expr: ValueExpr) -> Result<Self, Self::Error> {
        let expr: Expr = value_expr.into();

        let expr = expr
            .to_bytes()
            .map_err(|e| expr_to_bytes_violation("value_expr.expr", e))?
            .to_vec();

        Ok(Self { expr })
    }
}

impl TryFrom<Vec<Expr>> for proto2::Filters {
    type Error = FieldViolation;

    fn try_from(filters: Vec<Expr>) -> Result<Self, Self::Error> {
        let exprs = filters
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                expr.to_bytes()
                    .map_err(|e| expr_to_bytes_violation(i.to_string(), e).scope("expr"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { exprs })
    }
}

impl TryFrom<proto2::Filters> for Vec<Expr> {
    type Error = FieldViolation;

    fn try_from(proto: proto2::Filters) -> Result<Self, Self::Error> {
        let proto2::Filters { exprs } = proto;

        let exprs = exprs
            .into_iter()
            .map(|bytes| {
                Expr::from_bytes_with_registry(&bytes, query_functions::registry())
                    .map_err(|e| expr_from_bytes_violation("exprs", e))
            })
            .collect::<Result<Self, _>>()?;

        Ok(exprs)
    }
}

#[derive(Debug, Snafu)]
pub enum EncodeProtoPredicateFromBase64Error {
    #[snafu(display("Cannot encode protobuf: {source}"))]
    ProtobufEncode { source: prost::EncodeError },
}

/// Encodes [`proto::Predicate`] as base64.
pub fn encode_proto_predicate_as_base64(
    predicate: &proto::Predicate,
) -> Result<String, EncodeProtoPredicateFromBase64Error> {
    let mut buf = vec![];
    predicate.encode(&mut buf).context(ProtobufEncodeSnafu)?;
    Ok(BASE64_STANDARD.encode(&buf))
}

/// Encodes [`proto2::Filters`] as base64.
pub fn encode_proto2_filters_as_base64(
    filters: &proto2::Filters,
) -> Result<String, EncodeProtoPredicateFromBase64Error> {
    let mut buf = vec![];
    filters.encode(&mut buf).context(ProtobufEncodeSnafu)?;
    Ok(BASE64_STANDARD.encode(&buf))
}

#[derive(Debug, Snafu)]
pub enum DecodeProtoPredicateFromBase64Error {
    #[snafu(display("Cannot decode base64: {source}"))]
    Base64Decode { source: base64::DecodeError },

    #[snafu(display("Cannot decode protobuf: {source}"))]
    ProtobufDecode { source: prost::DecodeError },
}

/// Decodes [`proto::Predicate`] from base64 string.
pub fn decode_proto_predicate_from_base64(
    s: &str,
) -> Result<proto::Predicate, DecodeProtoPredicateFromBase64Error> {
    let predicate_binary = BASE64_STANDARD.decode(s).context(Base64DecodeSnafu)?;
    proto::Predicate::decode(predicate_binary.as_slice()).context(ProtobufDecodeSnafu)
}

/// Decodes [`proto2::Filters`] from base64 string.
pub fn decode_proto2_filters_from_base64(
    s: &str,
) -> Result<proto2::Filters, DecodeProtoPredicateFromBase64Error> {
    let predicate_binary = BASE64_STANDARD.decode(s).context(Base64DecodeSnafu)?;
    proto2::Filters::decode(predicate_binary.as_slice()).context(ProtobufDecodeSnafu)
}

impl TryFrom<proto2::PartitionIdentifier> for TransitionPartitionId {
    type Error = FieldViolation;

    fn try_from(value: proto2::PartitionIdentifier) -> Result<Self, Self::Error> {
        let proto2::PartitionIdentifier {
            partition_identifier,
        } = value;
        let id =
            partition_identifier.ok_or_else(|| FieldViolation::required("partition_identifier"))?;
        let id = match id {
            proto2::partition_identifier::PartitionIdentifier::CatalogId(id) => {
                Self::Deprecated(PartitionId::new(id))
            }
            proto2::partition_identifier::PartitionIdentifier::HashId(id) => {
                Self::Deterministic(PartitionHashId::try_from(id.as_ref()).map_err(|e| {
                    FieldViolation {
                        field: "partition_identifier".to_owned(),
                        description: e.to_string(),
                    }
                })?)
            }
        };
        Ok(id)
    }
}

impl From<TransitionPartitionId> for proto2::PartitionIdentifier {
    fn from(id: TransitionPartitionId) -> Self {
        let id = match id {
            TransitionPartitionId::Deprecated(id) => {
                proto2::partition_identifier::PartitionIdentifier::CatalogId(id.get())
            }
            TransitionPartitionId::Deterministic(id) => {
                proto2::partition_identifier::PartitionIdentifier::HashId(
                    id.as_bytes().to_vec().into(),
                )
            }
        };
        Self {
            partition_identifier: Some(id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use super::*;
    use datafusion::{logical_expr::LogicalPlanBuilder, prelude::*};

    #[test]
    fn query_round_trip() {
        let rust_predicate = predicate::Predicate::new()
            .with_range(1, 100)
            .with_expr(col("foo"))
            .with_value_expr(col("_value").eq(lit("bar")).try_into().unwrap());

        let rust_query = IngesterQueryRequest::new(
            NamespaceId::new(42),
            TableId::new(1337),
            vec!["usage".into(), "time".into()],
            Some(rust_predicate),
        );

        let proto_query: proto::IngesterQueryRequest = rust_query.clone().try_into().unwrap();

        let rust_query_converted: IngesterQueryRequest = proto_query.try_into().unwrap();

        assert_eq!(rust_query, rust_query_converted);
    }

    #[test]
    fn query2_round_trip() {
        let rust_query = IngesterQueryRequest2::new(
            NamespaceId::new(42),
            TableId::new(1337),
            vec!["usage".into(), "time".into()],
            vec![col("foo").eq(lit(1i64))],
        );

        let proto_query: proto2::QueryRequest = rust_query.clone().try_into().unwrap();

        let rust_query_converted: IngesterQueryRequest2 = proto_query.try_into().unwrap();

        assert_eq!(rust_query, rust_query_converted);
    }

    #[test]
    fn predicate_proto_base64_roundtrip() {
        let predicate = Predicate {
            field_columns: Some(BTreeSet::from([String::from("foo"), String::from("bar")])),
            range: Some(TimestampRange::new(13, 42)),
            exprs: vec![Expr::Wildcard],
            value_expr: vec![col("_value").eq(lit("bar")).try_into().unwrap()],
        };
        let predicate: proto::Predicate = predicate.try_into().unwrap();
        let base64 = encode_proto_predicate_as_base64(&predicate).unwrap();
        let predicate2 = decode_proto_predicate_from_base64(&base64).unwrap();
        assert_eq!(predicate, predicate2);
    }

    #[test]
    fn filters_proto2_base64_roundtrip() {
        let filters = vec![col("col").eq(lit(1i64))];
        let filters_1: proto2::Filters = filters.try_into().unwrap();

        let base64_1 = encode_proto2_filters_as_base64(&filters_1).unwrap();
        let filters_2 = decode_proto2_filters_from_base64(&base64_1).unwrap();
        let base64_2 = encode_proto2_filters_as_base64(&filters_2).unwrap();

        assert_eq!(filters_1, filters_2);
        assert_eq!(base64_1, base64_2);
    }

    #[test]
    fn filters_not_serializable_error() {
        let subquery = Arc::new(LogicalPlanBuilder::empty(true).build().unwrap());
        let filters = vec![
            col("col").eq(lit(1i64)),
            exists(subquery),
            col("col").eq(lit(1i64)),
        ];

        let err = proto2::Filters::try_from(filters).unwrap_err();
        assert_eq!(err.field, "expr.1",)
    }
}
