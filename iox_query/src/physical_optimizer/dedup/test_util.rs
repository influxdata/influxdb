use std::sync::Arc;

use arrow::datatypes::{Fields, Schema as ArrowSchema};
use datafusion::physical_plan::ExecutionPlan;
use schema::Schema;

use crate::{
    chunk_order_field,
    provider::{chunks_to_physical_nodes, DeduplicateExec},
    test::TestChunk,
    util::arrow_sort_key_exprs,
    QueryChunk,
};

pub fn dedup_plan(schema: Schema, chunks: Vec<TestChunk>) -> Arc<dyn ExecutionPlan> {
    dedup_plan_impl(schema, chunks, false)
}

pub fn dedup_plan_with_chunk_order_col(
    schema: Schema,
    chunks: Vec<TestChunk>,
) -> Arc<dyn ExecutionPlan> {
    dedup_plan_impl(schema, chunks, true)
}

fn dedup_plan_impl(
    schema: Schema,
    chunks: Vec<TestChunk>,
    use_chunk_order_col: bool,
) -> Arc<dyn ExecutionPlan> {
    let chunks = chunks
        .into_iter()
        .map(|c| Arc::new(c) as _)
        .collect::<Vec<Arc<dyn QueryChunk>>>();
    let arrow_schema = if use_chunk_order_col {
        Arc::new(ArrowSchema::new(
            schema
                .as_arrow()
                .fields
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ))
    } else {
        schema.as_arrow()
    };
    let plan = chunks_to_physical_nodes(&arrow_schema, None, chunks, 2);

    let sort_key = schema::sort::SortKey::from_columns(schema.primary_key());
    let sort_exprs = arrow_sort_key_exprs(&sort_key, &plan.schema());
    Arc::new(DeduplicateExec::new(plan, sort_exprs, use_chunk_order_col))
}

pub fn chunk(id: u128) -> TestChunk {
    TestChunk::new("table")
        .with_id(id)
        .with_tag_column("tag1")
        .with_tag_column("tag2")
        .with_i64_field_column("field")
        .with_time_column()
}
