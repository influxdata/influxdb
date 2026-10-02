//! `system.tokens` system table implementation [`TokenSystemTable`]
use std::{fmt::Debug, sync::Arc};

use arrow::array::{StringViewBuilder, TimestampMillisecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_authz::TokenInfo;
use influxdb3_catalog::catalog::{Catalog, DEFAULT_OPERATOR_TOKEN_NAME};
use iox_system_tables::IoxSystemTable;

/// API to format a token's `permissions` column for `system.tokens`
pub trait TokenPermissionsFormatter: Debug + Send + Sync {
    /// Format the `permissions` column value for each of `tokens`, in the same order.
    fn format_permissions(&self, tokens: &[Arc<TokenInfo>]) -> Vec<String>;
}

/// `system.tokens` system table implementation
#[derive(Debug)]
pub struct TokenSystemTable {
    catalog: Arc<Catalog>,
    schema: SchemaRef,
    started_with_auth: bool,
    hide_operator_token: bool,
    permissions_formatter: Arc<dyn TokenPermissionsFormatter>,
}

impl TokenSystemTable {
    pub fn new(
        catalog: Arc<Catalog>,
        started_with_auth: bool,
        permissions_formatter: Arc<dyn TokenPermissionsFormatter>,
    ) -> Self {
        Self {
            catalog,
            schema: table_schema(started_with_auth),
            started_with_auth,
            hide_operator_token: false,
            permissions_formatter,
        }
    }

    /// Omit the operator token from the table when `hide_operator_token` is
    /// true. The operator token still authenticates requests.
    pub fn with_operator_token_hidden(mut self, hide_operator_token: bool) -> Self {
        self.hide_operator_token = hide_operator_token;
        self
    }
}

#[async_trait::async_trait]
impl IoxSystemTable for TokenSystemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let mut results = self.catalog.get_tokens();
        if self.hide_operator_token {
            results.retain(|token| token.name.as_ref() != DEFAULT_OPERATOR_TOKEN_NAME);
        }
        to_record_batch(
            &self.schema,
            self.permissions_formatter.as_ref(),
            results,
            self.started_with_auth,
        )
    }
}

fn table_schema(started_with_auth: bool) -> SchemaRef {
    let fields = &[
        Field::new("token_id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8View, false),
        Field::new("hash", DataType::Utf8View, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("description", DataType::Utf8View, true),
        Field::new("created_by_token_id", DataType::UInt64, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("updated_by_token_id", DataType::UInt64, true),
        Field::new(
            "expiry",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("permissions", DataType::Utf8View, false),
    ];

    let mut all_fields = vec![];
    for field in fields {
        if field.name() == "hash" && !started_with_auth {
            continue;
        }
        all_fields.push(field.clone());
    }

    Arc::new(Schema::new(all_fields))
}

fn to_record_batch(
    schema: &SchemaRef,
    permissions_formatter: &dyn TokenPermissionsFormatter,
    tokens: Vec<Arc<TokenInfo>>,
    started_with_auth: bool,
) -> Result<RecordBatch, DataFusionError> {
    let mut id_arr = UInt64Builder::with_capacity(tokens.len());
    let mut name_arr = StringViewBuilder::with_capacity(tokens.len());
    let mut hash_arr = StringViewBuilder::with_capacity(tokens.len());
    let mut created_at_arr = TimestampMillisecondBuilder::with_capacity(tokens.len());
    let mut description_arr = StringViewBuilder::with_capacity(tokens.len());
    let mut created_by_arr = UInt64Builder::with_capacity(tokens.len());
    let mut updated_at_arr = TimestampMillisecondBuilder::with_capacity(tokens.len());
    let mut updated_by_arr = UInt64Builder::with_capacity(tokens.len());
    let mut expiry_arr = TimestampMillisecondBuilder::with_capacity(tokens.len());
    let mut permissions_arr = StringViewBuilder::with_capacity(tokens.len());

    let permissions_strs = permissions_formatter.format_permissions(&tokens);

    for (token, permissions_str) in tokens.iter().zip(permissions_strs) {
        id_arr.append_value(token.id.get());
        name_arr.append_value(&token.name);

        if started_with_auth {
            hash_arr.append_value(&hex::encode(&token.hash)[..9]);
        }
        created_at_arr.append_value(token.created_at);
        if token.description.is_some() {
            description_arr.append_value(token.description.clone().unwrap());
        } else {
            description_arr.append_null();
        }

        if let Some(created_by) = token.created_by {
            created_by_arr.append_value(created_by.get());
        } else {
            created_by_arr.append_null();
        }

        if let Some(updated_at) = token.updated_at {
            updated_at_arr.append_value(updated_at);
        } else {
            updated_at_arr.append_null();
        }

        if let Some(updated_by) = token.updated_by {
            updated_by_arr.append_value(updated_by.get());
        } else {
            updated_by_arr.append_null();
        }

        // when expiry is not passed in, we default it to i64::MAX (which is same as null)
        if token.expiry_millis == i64::MAX {
            expiry_arr.append_null();
        } else {
            expiry_arr.append_value(token.expiry_millis);
        }

        permissions_arr.append_value(permissions_str);
    }

    if started_with_auth {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(id_arr.finish()),
            Arc::new(name_arr.finish()),
            Arc::new(hash_arr.finish()),
            Arc::new(created_at_arr.finish()),
            Arc::new(description_arr.finish()),
            Arc::new(created_by_arr.finish()),
            Arc::new(updated_at_arr.finish()),
            Arc::new(updated_by_arr.finish()),
            Arc::new(expiry_arr.finish()),
            Arc::new(permissions_arr.finish()),
        ];
        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    } else {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(id_arr.finish()),
            Arc::new(name_arr.finish()),
            Arc::new(created_at_arr.finish()),
            Arc::new(description_arr.finish()),
            Arc::new(created_by_arr.finish()),
            Arc::new(updated_at_arr.finish()),
            Arc::new(updated_by_arr.finish()),
            Arc::new(expiry_arr.finish()),
            Arc::new(permissions_arr.finish()),
        ];
        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    }
}

#[cfg(test)]
mod tests;
