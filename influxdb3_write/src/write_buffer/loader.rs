//! This module contains logic to load the initial server state from the persister and wal
//! if configured.

use crate::catalog::Catalog;
use crate::wal::WalSegmentWriterNoopImpl;
use crate::write_buffer::{
    buffer_segment::{load_buffer_from_segment, ClosedBufferSegment, OpenBufferSegment},
    Result,
};
use crate::{persister, Wal};
use crate::{PersistedCatalog, PersistedSegment, Persister, SegmentId};
use std::sync::Arc;

const SEGMENTS_TO_LOAD: usize = 1000;

/// The state loaded and initialized from the persister and wal.
#[derive(Debug)]
pub struct LoadedState {
    pub catalog: Arc<Catalog>,
    pub open_segment: OpenBufferSegment,
    pub persisting_buffer_segments: Vec<ClosedBufferSegment>,
    pub persisted_segments: Vec<PersistedSegment>,
}

pub async fn load_starting_state<W: Wal>(
    persister: Arc<dyn Persister<Error = persister::Error>>,
    wal: Option<Arc<W>>,
) -> Result<LoadedState> {
    let PersistedCatalog { catalog, .. } = persister.load_catalog().await?.unwrap_or_default();
    let catalog = Arc::new(Catalog::from_inner(catalog));

    let persisted_segments = persister.load_segments(SEGMENTS_TO_LOAD).await?;

    let last_persisted_segment_id = persisted_segments
        .last()
        .map(|s| s.segment_id)
        .unwrap_or(SegmentId::new(0));
    let mut open_segment_id = last_persisted_segment_id.next();
    let mut persisting_buffer_segments = Vec::new();

    let open_segment = if let Some(wal) = wal {
        // read any segments that don't show up in the list of persisted segments

        // first load up any segments from the wal that haven't been persisted yet, except for the
        // last one, which is the open segment.
        let wal_segments = wal.segment_files()?;
        if !wal_segments.is_empty() {
            // update the segment_id of the open segment to be for the last wal file
            open_segment_id = wal_segments.last().unwrap().segment_id;

            for segment_file in wal_segments.iter().take(wal_segments.len() - 1) {
                // if persisted segemnts is empty, load all segments from the wal, otherwise
                // only load segments that haven't been persisted yet
                if segment_file.segment_id >= last_persisted_segment_id
                    && !persisted_segments.is_empty()
                {
                    continue;
                }

                let segment_reader = wal.open_segment_reader(segment_file.segment_id)?;
                let starting_sequence_number = catalog.sequence_number();
                let buffer = load_buffer_from_segment(&catalog, segment_reader)?;

                let segment = OpenBufferSegment::new(
                    segment_file.segment_id,
                    starting_sequence_number,
                    Box::new(WalSegmentWriterNoopImpl::new(segment_file.segment_id)),
                    Some(buffer),
                );
                let closed_segment = segment.into_closed_segment(Arc::clone(&catalog));
                persisting_buffer_segments.push(closed_segment);
            }
        }

        // read the last segment into an open segment
        let segment_reader = match wal.open_segment_reader(open_segment_id) {
            Ok(reader) => Some(reader),
            Err(crate::wal::Error::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                None
            }
            Err(e) => return Err(e.into()),
        };
        let buffered = match segment_reader {
            Some(reader) => Some(load_buffer_from_segment(&catalog, reader)?),
            None => None,
        };
        let segment_writer = wal.open_segment_writer(open_segment_id)?;
        OpenBufferSegment::new(
            open_segment_id,
            catalog.sequence_number(),
            segment_writer,
            buffered,
        )
    } else {
        OpenBufferSegment::new(
            open_segment_id,
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(open_segment_id)),
            None,
        )
    };

    Ok(LoadedState {
        catalog,
        open_segment,
        persisting_buffer_segments,
        persisted_segments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persister::PersisterImpl;
    use crate::test_helpers::lp_to_write_batch;
    use crate::wal::{WalImpl, WalSegmentWriterNoopImpl};
    use crate::{DatabaseTables, LpWriteOp, ParquetFile, SequenceNumber, TableParquetFiles, WalOp};
    use arrow_util::assert_batches_eq;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::collections::HashMap;

    #[tokio::test]
    async fn loads_without_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister: Arc<dyn Persister<Error = persister::Error>> =
            Arc::new(PersisterImpl::new(Arc::clone(&object_store)));

        let segment_id = SegmentId::new(4);
        let segment_writer = Box::new(WalSegmentWriterNoopImpl::new(segment_id));
        let mut open_segment =
            OpenBufferSegment::new(segment_id, SequenceNumber::new(0), segment_writer, None);

        let catalog = Catalog::new();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "db1".to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, "db1", lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let catalog = Arc::new(catalog);
        let closed_buffer_segment = open_segment.into_closed_segment(Arc::clone(&catalog));
        closed_buffer_segment
            .persist(Arc::clone(&persister))
            .await
            .unwrap();

        let loaded_state = load_starting_state(persister, None::<Arc<crate::wal::WalImpl>>)
            .await
            .unwrap();
        let expected_catalog = catalog.clone_inner();
        let loaded_catalog = loaded_state.catalog.clone_inner();

        assert_eq!(expected_catalog, loaded_catalog);
        // the open segment it creates should be the next value in line
        assert_eq!(loaded_state.open_segment.segment_id(), SegmentId::new(5));
        let persisted_segment = loaded_state.persisted_segments.first().unwrap();
        assert_eq!(persisted_segment.segment_id, segment_id);
        assert_eq!(persisted_segment.segment_row_count, 3);

        let tables_persisted = persisted_segment.databases.get("db1").unwrap();
        assert_eq!(
            tables_persisted
                .tables
                .get("cpu")
                .unwrap()
                .parquet_files
                .len(),
            1
        );
        assert_eq!(
            tables_persisted
                .tables
                .get("mem")
                .unwrap()
                .parquet_files
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn loads_with_no_persisted_segments_and_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister: Arc<dyn Persister<Error = persister::Error>> =
            Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut open_segment,
            ..
        } = load_starting_state(Arc::clone(&persister), Some(Arc::clone(&wal)))
            .await
            .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let loaded_state = load_starting_state(persister, Some(wal)).await.unwrap();

        assert!(loaded_state.persisting_buffer_segments.is_empty());
        assert!(loaded_state.persisted_segments.is_empty());
        let db = loaded_state.catalog.db_schema(db_name).unwrap();
        assert_eq!(db.tables.len(), 2);
        assert!(db.tables.contains_key("cpu"));
        assert!(db.tables.contains_key("mem"));

        let cpu_table = db.get_table("cpu").unwrap();
        let cpu_data = open_segment
            .table_buffer(db_name, "cpu")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&cpu_table.schema, cpu_table.columns());
        let expected = vec![
            "+-----+----------+--------------------------------+",
            "| bar | tag1     | time                           |",
            "+-----+----------+--------------------------------+",
            "| 1.0 | cupcakes | 1970-01-01T00:00:00.000000010Z |",
            "+-----+----------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[cpu_data]);

        let mem_table = db.get_table("mem").unwrap();
        let mem_data = open_segment
            .table_buffer(db_name, "mem")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&mem_table.schema, mem_table.columns());
        let expected = vec![
            "+-----+---------+--------------------------------+",
            "| bar | tag2    | time                           |",
            "+-----+---------+--------------------------------+",
            "| 3.0 | turtles | 1970-01-01T00:00:00.000000015Z |",
            "| 2.0 | snakes  | 1970-01-01T00:00:00.000000020Z |",
            "+-----+---------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[mem_data]);
    }

    #[tokio::test]
    async fn loads_with_persisted_segments_and_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister: Arc<dyn Persister<Error = persister::Error>> =
            Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut open_segment,
            ..
        } = load_starting_state(Arc::clone(&persister), Some(Arc::clone(&wal)))
            .await
            .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let segment_id = open_segment.segment_id();
        let next_segment_id = open_segment.segment_id().next();

        open_segment
            .into_closed_segment(Arc::clone(&catalog))
            .persist(Arc::clone(&persister))
            .await
            .unwrap();

        let mut open_segment = OpenBufferSegment::new(
            next_segment_id,
            catalog.sequence_number(),
            wal.open_segment_writer(next_segment_id).unwrap(),
            None,
        );

        let lp = "cpu,tag1=cupcakes bar=3 20\nfoo val=1 123";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let loaded_state = load_starting_state(persister, Some(wal)).await.unwrap();

        assert!(loaded_state.persisting_buffer_segments.is_empty());
        assert_eq!(
            loaded_state.persisted_segments[0],
            PersistedSegment {
                segment_id,
                segment_wal_size_bytes: 201,
                segment_parquet_size_bytes: 3398,
                segment_row_count: 3,
                segment_min_time: 10,
                segment_max_time: 20,
                databases: HashMap::from([(
                    "db1".to_string(),
                    DatabaseTables {
                        tables: HashMap::from([
                            (
                                "cpu".to_string(),
                                TableParquetFiles {
                                    table_name: "cpu".to_string(),
                                    parquet_files: vec![ParquetFile {
                                        path: "dbs/db1/cpu/1970-01-01/4294967294.parquet"
                                            .to_string(),
                                        size_bytes: 1690,
                                        row_count: 1,
                                        min_time: 10,
                                        max_time: 10,
                                    }],
                                    sort_key: vec![],
                                }
                            ),
                            (
                                "mem".to_string(),
                                TableParquetFiles {
                                    table_name: "mem".to_string(),
                                    parquet_files: vec![ParquetFile {
                                        path: "dbs/db1/mem/1970-01-01/4294967294.parquet"
                                            .to_string(),
                                        size_bytes: 1708,
                                        row_count: 2,
                                        min_time: 15,
                                        max_time: 20,
                                    }],
                                    sort_key: vec![],
                                }
                            )
                        ])
                    }
                )])
            }
        );
        let db = loaded_state.catalog.db_schema(db_name).unwrap();
        assert_eq!(db.tables.len(), 3);
        assert!(db.tables.contains_key("cpu"));
        assert!(db.tables.contains_key("mem"));
        assert!(db.tables.contains_key("foo"));

        let cpu_table = db.get_table("cpu").unwrap();
        let cpu_data = open_segment
            .table_buffer(db_name, "cpu")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&cpu_table.schema, cpu_table.columns());
        let expected = vec![
            "+-----+----------+--------------------------------+",
            "| bar | tag1     | time                           |",
            "+-----+----------+--------------------------------+",
            "| 3.0 | cupcakes | 1970-01-01T00:00:00.000000020Z |",
            "+-----+----------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[cpu_data]);

        let foo_table = db.get_table("foo").unwrap();
        let foo_data = open_segment
            .table_buffer(db_name, "foo")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&foo_table.schema, foo_table.columns());
        let expected = vec![
            "+--------------------------------+-----+",
            "| time                           | val |",
            "+--------------------------------+-----+",
            "| 1970-01-01T00:00:00.000000123Z | 1.0 |",
            "+--------------------------------+-----+",
        ];
        assert_batches_eq!(&expected, &[foo_data]);
    }

    #[tokio::test]
    async fn loads_with_persisting() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister: Arc<dyn Persister<Error = persister::Error>> =
            Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut open_segment,
            ..
        } = load_starting_state(Arc::clone(&persister), Some(Arc::clone(&wal)))
            .await
            .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let next_segment_id = open_segment.segment_id().next();

        let closed_segment = open_segment.into_closed_segment(Arc::clone(&catalog));

        let mut open_segment = OpenBufferSegment::new(
            next_segment_id,
            catalog.sequence_number(),
            wal.open_segment_writer(next_segment_id).unwrap(),
            None,
        );

        let lp = "cpu,tag1=apples bar=3 20\nfoo val=1 123";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        open_segment.write_batch(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let loaded_state = load_starting_state(persister, Some(wal)).await.unwrap();

        assert_eq!(loaded_state.persisting_buffer_segments.len(), 1);
        let loaded_closed_segment = &loaded_state.persisting_buffer_segments[0];
        assert_eq!(loaded_closed_segment.segment_id, closed_segment.segment_id);
        assert_eq!(
            loaded_closed_segment.catalog_end_sequence_number,
            closed_segment.catalog_end_sequence_number
        );
        assert_eq!(
            loaded_closed_segment.catalog_start_sequence_number,
            closed_segment.catalog_start_sequence_number
        );
        assert_eq!(
            loaded_closed_segment.buffered_data,
            closed_segment.buffered_data
        );

        let db = loaded_state.catalog.db_schema(db_name).unwrap();
        assert_eq!(db.tables.len(), 3);
        assert!(db.tables.contains_key("cpu"));
        assert!(db.tables.contains_key("mem"));
        assert!(db.tables.contains_key("foo"));

        let cpu_table = db.get_table("cpu").unwrap();
        let cpu_data = open_segment
            .table_buffer(db_name, "cpu")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&cpu_table.schema, cpu_table.columns());
        let expected = vec![
            "+-----+--------+--------------------------------+",
            "| bar | tag1   | time                           |",
            "+-----+--------+--------------------------------+",
            "| 3.0 | apples | 1970-01-01T00:00:00.000000020Z |",
            "+-----+--------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &[cpu_data]);

        let foo_table = db.get_table("foo").unwrap();
        let foo_data = open_segment
            .table_buffer(db_name, "foo")
            .unwrap()
            .partition_buffers
            .get("1970-01-01")
            .unwrap()
            .rows_to_record_batch(&foo_table.schema, foo_table.columns());
        let expected = vec![
            "+--------------------------------+-----+",
            "| time                           | val |",
            "+--------------------------------+-----+",
            "| 1970-01-01T00:00:00.000000123Z | 1.0 |",
            "+--------------------------------+-----+",
        ];
        assert_batches_eq!(&expected, &[foo_data]);
    }
}
