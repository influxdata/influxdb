use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use predicate::Predicate;
use schema::{sort::SortKeyBuilder, Schema, TIME_COLUMN_NAME};

use crate::{
    provider::{chunks_to_physical_nodes, DeduplicateExec},
    test::TestChunk,
    util::arrow_sort_key_exprs,
    QueryChunk,
};

pub fn dedup_plan(schema: Schema, chunks: Vec<TestChunk>) -> Arc<dyn ExecutionPlan> {
    let chunks = chunks
        .into_iter()
        .map(|c| Arc::new(c) as _)
        .collect::<Vec<Arc<dyn QueryChunk>>>();
    let plan = chunks_to_physical_nodes(&schema, None, chunks, Predicate::new(), 2);
    let sort_key = SortKeyBuilder::new()
        .with_col("tag1")
        .with_col("tag2")
        .with_col(TIME_COLUMN_NAME)
        .build();
    let sort_exprs = arrow_sort_key_exprs(&sort_key, &schema.as_arrow());
    Arc::new(DeduplicateExec::new(plan, sort_exprs))
}

pub fn chunk(id: u128) -> TestChunk {
    TestChunk::new("table")
        .with_id(id)
        .with_tag_column("tag1")
        .with_tag_column("tag2")
        .with_i64_field_column("field")
        .with_time_column()
}
