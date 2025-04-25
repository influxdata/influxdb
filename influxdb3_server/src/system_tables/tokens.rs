use std::sync::Arc;

use arrow::array::{StringViewBuilder, TimestampMillisecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::{error::DataFusionError, prelude::Expr};
use influxdb3_authz::TokenInfo;
use influxdb3_catalog::catalog::Catalog;
use iox_system_tables::IoxSystemTable;
use tonic::async_trait;

#[derive(Debug)]
pub(crate) struct TokenSystemTable {
    catalog: Arc<Catalog>,
    schema: SchemaRef,
    started_with_auth: bool,
}

impl TokenSystemTable {
    pub(crate) fn new(catalog: Arc<Catalog>, started_with_auth: bool) -> Self {
        Self {
            catalog,
            schema: table_schema(started_with_auth),
            started_with_auth,
        }
    }
}

#[async_trait]
impl IoxSystemTable for TokenSystemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let results = self.catalog.get_tokens();
        to_record_batch(&self.schema, results, self.started_with_auth)
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

    for token in &tokens {
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

        if token.created_by.is_some() {
            created_by_arr.append_value(token.created_by.unwrap().get());
        } else {
            created_by_arr.append_null();
        }

        if token.updated_at.is_some() {
            updated_at_arr.append_value(token.updated_at.unwrap());
        } else {
            updated_at_arr.append_null();
        }

        if token.updated_by.is_some() {
            updated_by_arr.append_value(token.updated_by.unwrap().get());
        } else {
            updated_by_arr.append_null();
        }

        // when expiry is not passed in, we default it to i64::MAX (which is same as null)
        if token.expiry_millis == i64::MAX {
            expiry_arr.append_null();
        } else {
            expiry_arr.append_value(token.expiry_millis);
        }

        // core only
        let permissions_str = "*:*:*".to_string();

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
mod tests {
    use super::table_schema;

    #[test]
    fn test_schema_with_auth() {
        let schema = table_schema(true);
        // should have at least one field with name "hash"
        assert_eq!(schema.fields().iter().len(), 10);
        assert!(schema.fields.iter().any(|field| field.name() == "hash"));
    }

    #[test]
    fn test_schema_without_auth() {
        let schema = table_schema(false);
        // no field should have name "hash"
        assert_eq!(schema.fields().iter().len(), 9);
        schema
            .fields
            .iter()
            .for_each(|field| assert!(field.name() != "hash"));
    }
}
