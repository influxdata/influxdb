use std::sync::Arc;

use datafusion::physical_plan::{
    empty::EmptyExec, file_format::ParquetExec, union::UnionExec, visit_execution_plan,
    ExecutionPlan, ExecutionPlanVisitor,
};
use schema::Schema;

use crate::{
    provider::{PartitionedFileExt, RecordBatchesExec},
    QueryChunk,
};

/// Extract chunks and schema from plans created with [`chunks_to_physical_nodes`].
///
/// Returns `None` if no chunks (or an [`EmptyExec`] in case that no chunks where passed to
/// [`chunks_to_physical_nodes`]) were found or if the chunk data is inconsistent.
///
/// Note that this only works on the direct output of [`chunks_to_physical_nodes`]. If the plan is wrapped into
/// additional nodes (like de-duplication, filtering, projection) then NO data will be returned.
///
/// [`chunks_to_physical_nodes`]: crate::provider::chunks_to_physical_nodes
#[allow(dead_code)]
pub fn extract_chunks(plan: &dyn ExecutionPlan) -> Option<(Schema, Vec<Arc<dyn QueryChunk>>)> {
    let mut visitor = ExtractChunksVisitor::default();
    visit_execution_plan(plan, &mut visitor).ok()?;
    visitor.schema.map(|schema| (schema, visitor.chunks))
}

#[derive(Debug, Default)]
struct ExtractChunksVisitor {
    chunks: Vec<Arc<dyn QueryChunk>>,
    schema: Option<Schema>,
}

impl ExtractChunksVisitor {
    fn add_schema(&mut self, schema: &Schema) -> Result<(), ()> {
        if let Some(existing) = &self.schema {
            if existing != schema {
                return Err(());
            }
        } else {
            self.schema = Some(schema.clone());
        }

        Ok(())
    }

    fn add_chunk(&mut self, chunk: Arc<dyn QueryChunk>) -> Result<(), ()> {
        self.add_schema(chunk.schema())?;
        self.chunks.push(chunk);
        Ok(())
    }
}

impl ExecutionPlanVisitor for ExtractChunksVisitor {
    type Error = ();

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let plan_any = plan.as_any();

        if let Some(record_batches_exec) = plan_any.downcast_ref::<RecordBatchesExec>() {
            for chunk in record_batches_exec.chunks() {
                self.add_chunk(Arc::clone(chunk))?;
            }
        } else if let Some(parquet_exec) = plan_any.downcast_ref::<ParquetExec>() {
            for group in &parquet_exec.base_config().file_groups {
                for file in group {
                    let ext = file
                        .extensions
                        .as_ref()
                        .and_then(|any| any.downcast_ref::<PartitionedFileExt>())
                        .ok_or(())?;
                    self.add_chunk(Arc::clone(&ext.0))?;
                }
            }
        } else if let Some(empty_exec) = plan_any.downcast_ref::<EmptyExec>() {
            // should not produce dummy data
            if empty_exec.produce_one_row() {
                return Err(());
            }

            let schema = Schema::try_from(empty_exec.schema()).map_err(|_| ())?;
            self.add_schema(&schema)?;
        } else if plan_any.downcast_ref::<UnionExec>().is_some() {
            // continue visiting
        } else {
            // unsupported node
            return Err(());
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        provider::chunks_to_physical_nodes, test::TestChunk, util::df_physical_expr, QueryChunkMeta,
    };
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use data_types::ChunkId;
    use datafusion::{
        execution::context::TaskContext,
        physical_plan::filter::FilterExec,
        prelude::{col, lit, SessionConfig, SessionContext},
    };
    use predicate::Predicate;

    use super::*;

    #[test]
    fn test_roundtrip_empty() {
        let schema = chunk(1).schema().clone();
        assert_roundtrip(schema, vec![]);
    }

    #[test]
    fn test_roundtrip_single_record_batch() {
        let chunk1 = chunk(1);
        assert_roundtrip(chunk1.schema().clone(), vec![Arc::new(chunk1)]);
    }

    #[test]
    fn test_roundtrip_single_parquet() {
        let chunk1 = chunk(1).with_dummy_parquet_file();
        assert_roundtrip(chunk1.schema().clone(), vec![Arc::new(chunk1)]);
    }

    #[test]
    fn test_roundtrip_many_chunks() {
        let chunk1 = chunk(1).with_dummy_parquet_file();
        let chunk2 = chunk(2).with_dummy_parquet_file();
        let chunk3 = chunk(3).with_dummy_parquet_file();
        let chunk4 = chunk(4);
        let chunk5 = chunk(5);
        assert_roundtrip(
            chunk1.schema().clone(),
            vec![
                Arc::new(chunk1),
                Arc::new(chunk2),
                Arc::new(chunk3),
                Arc::new(chunk4),
                Arc::new(chunk5),
            ],
        );
    }

    #[test]
    fn test_different_schemas() {
        let some_chunk = chunk(1);
        let iox_schema = some_chunk.schema();
        let schema1 = iox_schema.as_arrow();
        let schema2 = iox_schema.select_by_indices(&[]).as_arrow();
        let plan = UnionExec::new(vec![
            Arc::new(EmptyExec::new(false, schema1)),
            Arc::new(EmptyExec::new(false, schema2)),
        ]);
        assert!(extract_chunks(&plan).is_none());
    }

    #[test]
    fn test_empty_exec_with_rows() {
        let schema = chunk(1).schema().as_arrow();
        let plan = EmptyExec::new(true, schema);
        assert!(extract_chunks(&plan).is_none());
    }

    #[test]
    fn test_empty_exec_no_iox_schema() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            DataType::Float64,
            true,
        )]));
        let plan = EmptyExec::new(false, schema);
        assert!(extract_chunks(&plan).is_none());
    }

    #[test]
    fn test_stop_at_other_node_types() {
        let chunk1 = chunk(1);
        let schema = chunk1.schema().clone();
        let plan = chunks_to_physical_nodes(
            &schema,
            None,
            vec![Arc::new(chunk1)],
            Predicate::default(),
            task_ctx(),
        );
        let plan = FilterExec::try_new(
            df_physical_expr(plan.as_ref(), col("tag1").eq(lit("foo"))).unwrap(),
            plan,
        )
        .unwrap();
        assert!(extract_chunks(&plan).is_none());
    }

    #[track_caller]
    fn assert_roundtrip(schema: Schema, chunks: Vec<Arc<dyn QueryChunk>>) {
        let plan = chunks_to_physical_nodes(
            &schema,
            None,
            chunks.clone(),
            Predicate::default(),
            task_ctx(),
        );
        let (schema2, chunks2) = extract_chunks(plan.as_ref()).expect("data found");
        assert_eq!(schema, schema2);
        assert_eq!(chunk_ids(&chunks), chunk_ids(&chunks2));
    }

    fn task_ctx() -> Arc<TaskContext> {
        let session_ctx =
            SessionContext::with_config(SessionConfig::default().with_target_partitions(2));
        Arc::new(TaskContext::from(&session_ctx))
    }

    fn chunk_ids(chunks: &[Arc<dyn QueryChunk>]) -> Vec<ChunkId> {
        let mut ids = chunks.iter().map(|c| c.id()).collect::<Vec<_>>();
        ids.sort();
        ids
    }

    fn chunk(id: u128) -> TestChunk {
        TestChunk::new("table")
            .with_id(id)
            .with_tag_column("tag1")
            .with_tag_column("tag2")
            .with_i64_field_column("field")
            .with_time_column()
    }
}
