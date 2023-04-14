use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

/// Prepare an arrow Schema for transport over the Arrow Flight protocol
///
/// Converts dictionary types to underlying types due to <https://github.com/apache/arrow-rs/issues/3389>
pub fn prepare_schema_for_flight(schema: SchemaRef) -> SchemaRef {
    let fields: Fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Arc::new(
                Field::new(
                    field.name(),
                    value_type.as_ref().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            ),
            _ => Arc::clone(field),
        })
        .collect();

    Arc::new(Schema::new(fields).with_metadata(schema.metadata().clone()))
}
