//! This module contains logic to load the initial server state from the persister and wal
//! if configured.

use crate::catalog::Catalog;
use crate::wal::WalSegmentWriterNoopImpl;
use crate::write_buffer::{
    buffer_segment::{load_buffer_from_segment, ClosedBufferSegment, OpenBufferSegment},
    Result,
};
use crate::{PersistedCatalog, PersistedSegment, Persister, SegmentId};
use crate::{SegmentDuration, SegmentRange, Wal};
use iox_time::Time;
use std::sync::Arc;

use super::Error;

const SEGMENTS_TO_LOAD: usize = 1000;

/// The state loaded and initialized from the persister and wal.
#[derive(Debug)]
pub struct LoadedState {
    pub catalog: Arc<Catalog>,
    pub current_segment: OpenBufferSegment,
    pub next_segment: OpenBufferSegment,
    pub persisting_buffer_segments: Vec<ClosedBufferSegment>,
    pub persisted_segments: Vec<PersistedSegment>,
    pub last_segment_id: SegmentId,
}

pub async fn load_starting_state<W, P>(
    persister: Arc<P>,
    wal: Option<Arc<W>>,
    server_load_time: Time,
    segment_duration: SegmentDuration,
) -> Result<LoadedState>
where
    W: Wal,
    P: Persister,
    Error: From<P::Error>,
{
    let PersistedCatalog { catalog, .. } = persister.load_catalog().await?.unwrap_or_default();
    let catalog = Arc::new(Catalog::from_inner(catalog));
    println!("catalog sN {:?}", catalog.sequence_number());

    let persisted_segments = persister.load_segments(SEGMENTS_TO_LOAD).await?;

    let last_persisted_segment_id = persisted_segments
        .last()
        .map(|s| s.segment_id)
        .unwrap_or(SegmentId::new(0));
    let mut persisting_buffer_segments = Vec::new();

    let mut last_segment_id = last_persisted_segment_id;

    let current_segment_range =
        SegmentRange::from_time_and_duration(server_load_time, segment_duration, false);
    let next_segment_range = current_segment_range.next();

    let (current_segment, next_segment) = if let Some(wal) = wal {
        // read any segments that don't show up in the list of persisted segments

        // first load up any segments from the wal that haven't been persisted yet
        let wal_segments = wal.segment_files()?;
        let mut current_segment = None;
        let mut next_segment = None;
        let max_segment_id = last_persisted_segment_id.max(
            wal_segments
                .last()
                .map(|s| s.segment_id)
                .unwrap_or(SegmentId::new(0)),
        );

        for segment_file in wal_segments {
            last_segment_id = last_segment_id.max(segment_file.segment_id);

            // if persisted segemnts is empty, load all segments from the wal, otherwise
            // only load segments that haven't been persisted yet
            if segment_file.segment_id <= last_persisted_segment_id
                && !persisted_segments.is_empty()
            {
                println!("skipping segment {:?}", segment_file.segment_id);
                continue;
            }

            println!("loading segment {:?}", segment_file.segment_id);
            let starting_sequence_number = catalog.sequence_number();
            let segment_reader = wal.open_segment_reader(segment_file.segment_id)?;
            let segment_header = *segment_reader.header();
            println!("segment header {:?}", segment_header);
            let buffer = load_buffer_from_segment(&catalog, segment_reader)?;

            let segment = OpenBufferSegment::new(
                segment_header.id,
                segment_header.range,
                starting_sequence_number,
                wal.open_segment_writer(segment_file.segment_id)?,
                Some(buffer),
            );

            if segment_header.range == current_segment_range {
                current_segment = Some(segment);
            } else if segment_header.range == next_segment_range {
                next_segment = Some(segment);
            } else {
                persisting_buffer_segments.push(segment.into_closed_segment(Arc::clone(&catalog)));
            }
        }

        match (current_segment, next_segment) {
            (Some(current_segment), Some(next_segment)) => (current_segment, next_segment),
            (Some(current_segment), None) => {
                let next_segment = OpenBufferSegment::new(
                    current_segment.segment_id().next(),
                    next_segment_range,
                    catalog.sequence_number(),
                    wal.new_segment_writer(
                        current_segment.segment_id().next(),
                        next_segment_range,
                    )?,
                    None,
                );
                (current_segment, next_segment)
            }
            (None, Some(next_segment)) => {
                return Err(Error::CorruptLoadState(format!(
                    "Found next segment {:?} without current segment",
                    next_segment.segment_id()
                )))
            }
            (None, None) => {
                let current_segment_id = max_segment_id.next();
                let next_segment_id = current_segment_id.next();

                let current_segment = OpenBufferSegment::new(
                    current_segment_id,
                    current_segment_range,
                    catalog.sequence_number(),
                    wal.new_segment_writer(current_segment_id, current_segment_range)?,
                    None,
                );

                let next_segment = OpenBufferSegment::new(
                    next_segment_id,
                    next_segment_range,
                    catalog.sequence_number(),
                    wal.new_segment_writer(next_segment_id, next_segment_range)?,
                    None,
                );

                (current_segment, next_segment)
            }
        }
    } else {
        let current_segment_id = last_persisted_segment_id.next();
        let next_segment_id = current_segment_id.next();

        let current_segment = OpenBufferSegment::new(
            current_segment_id,
            current_segment_range,
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(current_segment_id)),
            None,
        );

        let next_segment = OpenBufferSegment::new(
            next_segment_id,
            next_segment_range,
            catalog.sequence_number(),
            Box::new(WalSegmentWriterNoopImpl::new(next_segment_id)),
            None,
        );

        (current_segment, next_segment)
    };

    last_segment_id = last_segment_id.max(last_persisted_segment_id);
    last_segment_id = last_segment_id.max(current_segment.segment_id());
    last_segment_id = last_segment_id.max(next_segment.segment_id());

    Ok(LoadedState {
        catalog,
        last_segment_id,
        current_segment,
        next_segment,
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
    use crate::{
        DatabaseTables, LpWriteOp, ParquetFile, SegmentRange, SequenceNumber, TableParquetFiles,
        WalOp,
    };
    use arrow_util::assert_batches_eq;
    use iox_time::Time;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;

    #[tokio::test]
    async fn loads_without_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));

        let segment_id = SegmentId::new(4);
        let segment_writer = Box::new(WalSegmentWriterNoopImpl::new(segment_id));
        let mut open_segment = OpenBufferSegment::new(
            segment_id,
            SegmentRange::test_range(),
            SequenceNumber::new(0),
            segment_writer,
            None,
        );

        let catalog = Catalog::new();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: "db1".to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, "db1", lp);

        open_segment.write_wal_ops(vec![wal_op]).unwrap();
        open_segment.buffer_writes(write_batch).unwrap();

        let catalog = Arc::new(catalog);
        let closed_buffer_segment = open_segment.into_closed_segment(Arc::clone(&catalog));
        closed_buffer_segment
            .persist(Arc::clone(&persister))
            .await
            .unwrap();

        let loaded_state = load_starting_state(
            persister,
            None::<Arc<crate::wal::WalImpl>>,
            Time::from_timestamp_nanos(0),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();
        let expected_catalog = catalog.clone_inner();
        let loaded_catalog = loaded_state.catalog.clone_inner();

        assert_eq!(expected_catalog, loaded_catalog);
        // the open segment it creates should be the next value in line
        assert_eq!(loaded_state.current_segment.segment_id(), SegmentId::new(5));
        assert_eq!(loaded_state.next_segment.segment_id(), SegmentId::new(6));
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

        assert_eq!(loaded_state.last_segment_id, SegmentId::new(6));
    }

    #[tokio::test]
    async fn loads_with_no_persisted_segments_and_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut current_segment,
            ..
        } = load_starting_state(
            Arc::clone(&persister),
            Some(Arc::clone(&wal)),
            Time::from_timestamp_nanos(0),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        current_segment.write_wal_ops(vec![wal_op.clone()]).unwrap();
        current_segment.buffer_writes(write_batch).unwrap();

        let loaded_state = load_starting_state(
            persister,
            Some(wal),
            Time::from_timestamp_nanos(0),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

        assert!(loaded_state.persisting_buffer_segments.is_empty());
        assert!(loaded_state.persisted_segments.is_empty());
        let db = loaded_state.catalog.db_schema(db_name).unwrap();
        assert_eq!(db.tables.len(), 2);
        assert!(db.tables.contains_key("cpu"));
        assert!(db.tables.contains_key("mem"));

        let cpu_table = db.get_table("cpu").unwrap();
        let cpu_data = loaded_state
            .current_segment
            .table_buffer(db_name, "cpu")
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
        let mem_data = loaded_state
            .current_segment
            .table_buffer(db_name, "mem")
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

        assert_eq!(loaded_state.last_segment_id, SegmentId::new(2));
    }

    #[tokio::test]
    async fn loads_with_persisted_segments_and_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut current_segment,
            mut next_segment,
            ..
        } = load_starting_state(
            Arc::clone(&persister),
            Some(Arc::clone(&wal)),
            Time::from_timestamp_nanos(0),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        current_segment.write_wal_ops(vec![wal_op]).unwrap();
        current_segment.buffer_writes(write_batch).unwrap();

        let segment_id = current_segment.segment_id();

        // close and persist the current segment
        current_segment
            .into_closed_segment(Arc::clone(&catalog))
            .persist(Arc::clone(&persister))
            .await
            .unwrap();

        // write data into the next segment
        let lp = "cpu,tag1=cupcakes bar=3 20\nfoo val=1 123";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        next_segment.write_wal_ops(vec![wal_op]).unwrap();
        next_segment.buffer_writes(write_batch).unwrap();

        // now load up with a start time that puts us in next segment period
        let loaded_state = load_starting_state(
            persister,
            Some(wal),
            Time::from_timestamp(6 * 60, 0).unwrap(),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

        // verify that the persisted segment doesn't show up as one that should be persisting
        assert!(loaded_state.persisting_buffer_segments.is_empty());

        // verify the data was persisted
        assert_eq!(
            loaded_state.persisted_segments[0],
            PersistedSegment {
                segment_id,
                segment_wal_size_bytes: 227,
                segment_parquet_size_bytes: 3458,
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
                                        path: "dbs/db1/cpu/1970-01-01T00-00/4294967294.parquet"
                                            .to_string(),
                                        size_bytes: 1721,
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
                                        path: "dbs/db1/mem/1970-01-01T00-00/4294967294.parquet"
                                            .to_string(),
                                        size_bytes: 1737,
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
        let cpu_data = loaded_state
            .current_segment
            .table_buffer(db_name, "cpu")
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
        let foo_data = loaded_state
            .current_segment
            .table_buffer(db_name, "foo")
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

        assert_eq!(loaded_state.last_segment_id, SegmentId::new(3));
    }

    #[tokio::test]
    async fn loads_with_persisting() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Arc::new(WalImpl::new(dir.clone()).unwrap());
        let db_name = "db1";

        let LoadedState {
            catalog,
            mut current_segment,
            mut next_segment,
            ..
        } = load_starting_state(
            Arc::clone(&persister),
            Some(Arc::clone(&wal)),
            Time::from_timestamp_nanos(0),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

        let lp = "cpu,tag1=cupcakes bar=1 10\nmem,tag2=turtles bar=3 15\nmem,tag2=snakes bar=2 20";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        current_segment.write_wal_ops(vec![wal_op]).unwrap();
        current_segment.buffer_writes(write_batch).unwrap();

        // close the current segment
        let closed_segment = current_segment.into_closed_segment(Arc::clone(&catalog));

        // write data into the next segment
        let lp = "cpu,tag1=apples bar=3 20\nfoo val=1 123";

        let wal_op = WalOp::LpWrite(LpWriteOp {
            db_name: db_name.to_string(),
            lp: lp.to_string(),
            default_time: 0,
        });

        let write_batch = lp_to_write_batch(&catalog, db_name, lp);

        next_segment.write_wal_ops(vec![wal_op]).unwrap();
        next_segment.buffer_writes(write_batch).unwrap();

        // now load up with a start time that puts us in next segment period. we should now
        // have the previous current_segment in persisting, the previous next_segment as the
        // new current_segment, and a brand new next_segment
        let loaded_state = load_starting_state(
            persister,
            Some(wal),
            Time::from_timestamp(6 * 60, 0).unwrap(),
            SegmentDuration::FiveMinutes,
        )
        .await
        .unwrap();

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
        let cpu_data = next_segment
            .table_buffer(db_name, "cpu")
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
        let foo_data = next_segment
            .table_buffer(db_name, "foo")
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

        assert_eq!(loaded_state.last_segment_id, SegmentId::new(3));
    }
}
