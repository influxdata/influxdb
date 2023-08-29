use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::physical_plan::ParquetExec,
    error::DataFusionError,
    physical_plan::{
        empty::EmptyExec, union::UnionExec, visit_execution_plan, ExecutionPlan,
        ExecutionPlanVisitor,
    },
};
use observability_deps::tracing::debug;
use schema::sort::SortKey;

use crate::{
    provider::{PartitionedFileExt, RecordBatchesExec},
    QueryChunk,
};

/// List of [`QueryChunk`]s.
pub type QueryChunks = Vec<Arc<dyn QueryChunk>>;

/// Extract chunks, schema, and output sort key from plans created with [`chunks_to_physical_nodes`].
///
/// Returns `None` if no chunks (or an [`EmptyExec`] in case that no chunks where passed to
/// [`chunks_to_physical_nodes`]) were found or if the chunk data is inconsistent.
///
/// When no chunks were passed to [`chunks_to_physical_nodes`] and hence an [`EmptyExec`] was created, then no output
/// sort key can be reconstructed. However this is usually OK because it does not have any effect anyways.
///
/// Note that this only works on the direct output of [`chunks_to_physical_nodes`]. If the plan is wrapped into
/// additional nodes (like de-duplication, filtering, projection) then NO data will be returned. Also [`ParquetExec`]
/// MUST NOT have a predicate attached.
///
///
/// [`chunks_to_physical_nodes`]: crate::provider::chunks_to_physical_nodes
pub fn extract_chunks(
    plan: &dyn ExecutionPlan,
) -> Option<(SchemaRef, QueryChunks, Option<SortKey>)> {
    let mut visitor = ExtractChunksVisitor::default();
    if let Err(e) = visit_execution_plan(plan, &mut visitor) {
        debug!(
            %e,
            "cannot extract chunks",
        );
        return None;
    }
    visitor
        .schema
        .map(|schema| (schema, visitor.chunks, visitor.sort_key))
}

#[derive(Debug, Default)]
struct ExtractChunksVisitor {
    chunks: Vec<Arc<dyn QueryChunk>>,
    schema: Option<SchemaRef>,
    sort_key: Option<SortKey>,
}

impl ExtractChunksVisitor {
    fn add_chunk(&mut self, chunk: Arc<dyn QueryChunk>) {
        self.chunks.push(chunk);
    }

    fn add_schema_from_exec(&mut self, exec: &dyn ExecutionPlan) -> Result<(), DataFusionError> {
        let schema = exec.schema();
        if let Some(existing) = &self.schema {
            if existing != &schema {
                return Err(DataFusionError::External(
                    String::from("Different schema").into(),
                ));
            }
        } else {
            self.schema = Some(schema);
        }
        Ok(())
    }

    fn add_sort_key(&mut self, sort_key: Option<&SortKey>) -> Result<(), DataFusionError> {
        let Some(sort_key) = sort_key else {
            return Ok(());
        };

        if let Some(existing) = &self.sort_key {
            if existing != sort_key {
                return Err(DataFusionError::External(
                    String::from("Different sort key").into(),
                ));
            }
        } else {
            self.sort_key = Some(sort_key.clone());
        }

        Ok(())
    }
}

impl ExecutionPlanVisitor for ExtractChunksVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let plan_any = plan.as_any();

        if let Some(record_batches_exec) = plan_any.downcast_ref::<RecordBatchesExec>() {
            self.add_schema_from_exec(record_batches_exec)
                .map_err(|e| {
                    DataFusionError::Context(
                        "add schema from RecordBatchesExec".to_owned(),
                        Box::new(e),
                    )
                })?;

            self.add_sort_key(record_batches_exec.output_sort_key_memo())?;

            for chunk in record_batches_exec.chunks() {
                self.add_chunk(Arc::clone(chunk));
            }
        } else if let Some(parquet_exec) = plan_any.downcast_ref::<ParquetExec>() {
            if parquet_exec.predicate().is_some() {
                return Err(DataFusionError::External(
                    String::from("ParquetExec has predicate").into(),
                ));
            }

            self.add_schema_from_exec(parquet_exec).map_err(|e| {
                DataFusionError::Context("add schema from ParquetExec".to_owned(), Box::new(e))
            })?;

            for group in &parquet_exec.base_config().file_groups {
                for file in group {
                    let ext = file
                        .extensions
                        .as_ref()
                        .and_then(|any| any.downcast_ref::<PartitionedFileExt>())
                        .ok_or_else(|| {
                            DataFusionError::External(
                                String::from("PartitionedFileExt not found").into(),
                            )
                        })?;
                    self.add_sort_key(ext.output_sort_key_memo.as_ref())?;
                    self.add_chunk(Arc::clone(&ext.chunk));
                }
            }
        } else if let Some(empty_exec) = plan_any.downcast_ref::<EmptyExec>() {
            // should not produce dummy data
            if empty_exec.produce_one_row() {
                return Err(DataFusionError::External(
                    String::from("EmptyExec produces row").into(),
                ));
            }

            self.add_schema_from_exec(empty_exec).map_err(|e| {
                DataFusionError::Context("add schema from EmptyExec".to_owned(), Box::new(e))
            })?;
        } else if plan_any.downcast_ref::<UnionExec>().is_some() {
            // continue visiting
        } else {
            // unsupported node
            return Err(DataFusionError::External(
                String::from("Unsupported node").into(),
            ));
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::{provider::chunks_to_physical_nodes, test::TestChunk, util::df_physical_expr};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use data_types::ChunkId;
    use datafusion::{
        common::tree_node::{Transformed, TreeNode},
        physical_plan::{expressions::Literal, filter::FilterExec},
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use schema::{merge::SchemaMerger, sort::SortKeyBuilder, SchemaBuilder, TIME_COLUMN_NAME};

    use super::*;

    #[test]
    fn test_roundtrip_empty() {
        let schema = chunk(1).schema().as_arrow();
        assert_roundtrip(schema, vec![], None);
    }

    #[test]
    fn test_roundtrip_single_record_batch() {
        let chunk1 = chunk(1);
        let sort_key = Some(sort_key());
        assert_roundtrip(chunk1.schema().as_arrow(), vec![Arc::new(chunk1)], sort_key);
    }

    #[test]
    fn test_roundtrip_single_parquet() {
        let chunk1 = chunk(1).with_dummy_parquet_file();
        let sort_key = Some(sort_key());
        assert_roundtrip(chunk1.schema().as_arrow(), vec![Arc::new(chunk1)], sort_key);
    }

    #[test]
    fn test_roundtrip_many_chunks() {
        let chunk1 = chunk(1).with_dummy_parquet_file();
        let chunk2 = chunk(2).with_dummy_parquet_file();
        let chunk3 = chunk(3).with_dummy_parquet_file();
        let chunk4 = chunk(4);
        let chunk5 = chunk(5);
        let sort_key = Some(sort_key());
        assert_roundtrip(
            chunk1.schema().as_arrow(),
            vec![
                Arc::new(chunk1),
                Arc::new(chunk2),
                Arc::new(chunk3),
                Arc::new(chunk4),
                Arc::new(chunk5),
            ],
            sort_key,
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
        let plan = EmptyExec::new(false, Arc::clone(&schema));
        let (schema2, chunks, sort_key) = extract_chunks(&plan).unwrap();
        assert_eq!(schema, schema2);
        assert!(chunks.is_empty());
        assert!(sort_key.is_none());
    }

    #[test]
    fn test_different_sort_keys() {
        let sort_key1 = Arc::new(SortKeyBuilder::new().with_col("tag1").build());
        let sort_key2 = Arc::new(SortKeyBuilder::new().with_col("tag2").build());
        let chunk1 = Arc::new(chunk(1)) as Arc<dyn QueryChunk>;
        let schema = chunk1.schema().as_arrow();
        let plan = UnionExec::new(vec![
            chunks_to_physical_nodes(&schema, Some(&sort_key1), vec![Arc::clone(&chunk1)], 1),
            chunks_to_physical_nodes(&schema, Some(&sort_key2), vec![chunk1], 1),
        ]);
        assert!(extract_chunks(&plan).is_none());
    }

    #[test]
    fn test_stop_at_other_node_types() {
        let chunk1 = chunk(1);
        let schema = chunk1.schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk1)], 2);
        let plan = FilterExec::try_new(
            df_physical_expr(plan.as_ref(), col("tag1").eq(lit("foo"))).unwrap(),
            plan,
        )
        .unwrap();
        assert!(extract_chunks(&plan).is_none());
    }

    #[test]
    fn test_preserve_record_batches_exec_schema() {
        let chunk = chunk(1);
        let schema_ext = SchemaBuilder::new().tag("zzz").build().unwrap();
        let schema = SchemaMerger::new()
            .merge(chunk.schema())
            .unwrap()
            .merge(&schema_ext)
            .unwrap()
            .build()
            .as_arrow();
        assert_roundtrip(schema, vec![Arc::new(chunk)], None);
    }

    #[test]
    fn test_preserve_parquet_exec_schema() {
        let chunk = chunk(1).with_dummy_parquet_file();
        let schema_ext = SchemaBuilder::new().tag("zzz").build().unwrap();
        let schema = SchemaMerger::new()
            .merge(chunk.schema())
            .unwrap()
            .merge(&schema_ext)
            .unwrap()
            .build()
            .as_arrow();
        assert_roundtrip(schema, vec![Arc::new(chunk)], None);
    }

    #[test]
    fn test_parquet_with_predicate_fails() {
        let chunk = chunk(1).with_dummy_parquet_file();
        let schema = chunk.schema().as_arrow();
        let plan = chunks_to_physical_nodes(&schema, None, vec![Arc::new(chunk)], 2);
        let plan = plan
            .transform_down(&|plan| {
                if let Some(exec) = plan.as_any().downcast_ref::<ParquetExec>() {
                    let exec = ParquetExec::new(
                        exec.base_config().clone(),
                        Some(Arc::new(Literal::new(ScalarValue::from(false)))),
                        None,
                    );
                    return Ok(Transformed::Yes(Arc::new(exec)));
                }
                Ok(Transformed::No(plan))
            })
            .unwrap();
        assert!(extract_chunks(plan.as_ref()).is_none());
    }

    #[track_caller]
    fn assert_roundtrip(
        schema: SchemaRef,
        chunks: Vec<Arc<dyn QueryChunk>>,
        output_sort_key: Option<SortKey>,
    ) {
        let plan = chunks_to_physical_nodes(&schema, output_sort_key.as_ref(), chunks.clone(), 2);
        let (schema2, chunks2, output_sort_key2) =
            extract_chunks(plan.as_ref()).expect("data found");
        assert_eq!(schema, schema2);
        assert_eq!(chunk_ids(&chunks), chunk_ids(&chunks2));
        assert_eq!(output_sort_key, output_sort_key2);
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

    fn sort_key() -> SortKey {
        SortKeyBuilder::new()
            .with_col("tag2")
            .with_col("tag1")
            .with_col(TIME_COLUMN_NAME)
            .build()
    }
}
