use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::TryStreamExt;
use observability_deps::tracing::info;
use persistence_windows::checkpoint::ReplayPlan;
use snafu::{ResultExt, Snafu};
use write_buffer::config::WriteBufferConfig;

use crate::Db;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Cannot seek sequencer {} during replay: {}", sequencer_id, source))]
    SeekError {
        sequencer_id: u32,
        source: write_buffer::core::WriteBufferError,
    },

    #[snafu(display(
        "Cannot get entry from sequencer {} during replay: {}",
        sequencer_id,
        source
    ))]
    EntryError {
        sequencer_id: u32,
        source: write_buffer::core::WriteBufferError,
    },

    #[snafu(display(
        "For sequencer {} expected to find sequence {} but replay jumped to {}",
        sequencer_id,
        expected_sequence_number,
        actual_sequence_number,
    ))]
    EntryLostError {
        sequencer_id: u32,
        actual_sequence_number: u64,
        expected_sequence_number: u64,
    },

    #[snafu(display(
        "Cannot store from sequencer {} during replay: {}",
        sequencer_id,
        source
    ))]
    StoreError {
        sequencer_id: u32,
        source: Box<crate::db::Error>,
    },

    #[snafu(display(
        "Replay plan references unknown sequencer {}, known sequencers are {:?}",
        sequencer_id,
        sequencer_ids,
    ))]
    UnknownSequencer {
        sequencer_id: u32,
        sequencer_ids: Vec<u32>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Seek to latest known sequence.
///
/// This can be used when no replay is wanted.
///
/// # Error Handling
/// This function may return an error if the watermarks (= last known sequence number) cannot be fetched or a seek
/// operation fails. In that case some of the sequencers in the write buffers might already be seeked and others not.
/// The caller must NOT use the write buffer in that case without ensuring that it is put into some proper state, e.g.
/// by retrying this function.
pub async fn seek_to_end(db: &Db) -> Result<()> {
    if let Some(WriteBufferConfig::Reading(write_buffer)) = &db.write_buffer {
        let mut write_buffer = write_buffer
            .try_lock()
            .expect("no streams should exist at this point");

        let mut watermarks = vec![];
        for (sequencer_id, stream) in write_buffer.streams() {
            let watermark = (stream.fetch_high_watermark)()
                .await
                .context(SeekError { sequencer_id })?;
            watermarks.push((sequencer_id, watermark));
        }

        for (sequencer_id, watermark) in watermarks {
            write_buffer
                .seek(sequencer_id, watermark)
                .await
                .context(SeekError { sequencer_id })?;
        }
    }

    Ok(())
}

/// Perform sequencer-driven replay for this DB.
pub async fn perform_replay(db: &Db, replay_plan: &ReplayPlan) -> Result<()> {
    if let Some(WriteBufferConfig::Reading(write_buffer)) = &db.write_buffer {
        let db_name = db.rules.read().db_name().to_string();
        info!(%db_name, "starting replay");

        let mut write_buffer = write_buffer
            .try_lock()
            .expect("no streams should exist at this point");

        // check if write buffer and replay plan agree on the set of sequencer ids
        let sequencer_ids: BTreeSet<_> = write_buffer
            .streams()
            .into_iter()
            .map(|(sequencer_id, _stream)| sequencer_id)
            .collect();
        for sequencer_id in replay_plan.sequencer_ids() {
            if !sequencer_ids.contains(&sequencer_id) {
                return Err(Error::UnknownSequencer {
                    sequencer_id,
                    sequencer_ids: sequencer_ids.iter().copied().collect(),
                });
            }
        }

        // determine replay ranges based on the plan
        let replay_ranges: BTreeMap<_, _> = sequencer_ids
            .into_iter()
            .filter_map(|sequencer_id| {
                replay_plan
                    .replay_range(sequencer_id)
                    .map(|min_max| (sequencer_id, min_max))
            })
            .collect();

        // seek write buffer according to the plan
        for (sequencer_id, min_max) in &replay_ranges {
            if let Some(min) = min_max.min() {
                info!(%db_name, sequencer_id, sequence_number=min, "seek sequencer in preperation for replay");
                write_buffer
                    .seek(*sequencer_id, min)
                    .await
                    .context(SeekError {
                        sequencer_id: *sequencer_id,
                    })?;
            } else {
                let sequence_number = min_max.max() + 1;
                info!(%db_name, sequencer_id, sequence_number, "seek sequencer that did not require replay");
                write_buffer
                    .seek(*sequencer_id, sequence_number)
                    .await
                    .context(SeekError {
                        sequencer_id: *sequencer_id,
                    })?;
            }
        }

        // replay ranges
        for (sequencer_id, mut stream) in write_buffer.streams() {
            if let Some(min_max) = replay_ranges.get(&sequencer_id) {
                if min_max.min().is_none() {
                    // no replay required
                    continue;
                }
                info!(
                    %db_name,
                    sequencer_id,
                    sequence_number_min=min_max.min().expect("checked above"),
                    sequence_number_max=min_max.max(),
                    "replay sequencer",
                );

                while let Some(entry) = stream
                    .stream
                    .try_next()
                    .await
                    .context(EntryError { sequencer_id })?
                {
                    let sequence = *entry.sequence().expect("entry must be sequenced");
                    if sequence.number > min_max.max() {
                        return Err(Error::EntryLostError {
                            sequencer_id,
                            actual_sequence_number: sequence.number,
                            expected_sequence_number: min_max.max(),
                        });
                    }

                    let entry = Arc::new(entry);
                    let mut logged_hard_limit = false;
                    let n_tries = 100;
                    for n_try in 1..=n_tries {
                        match db.store_sequenced_entry(Arc::clone(&entry)) {
                            Ok(_) => {
                                break;
                            }
                            Err(crate::db::Error::HardLimitReached {}) if n_try < n_tries => {
                                if !logged_hard_limit {
                                    info!(
                                        %db_name,
                                        sequencer_id,
                                        n_try,
                                        n_tries,
                                        "Hard limit reached while replaying, waiting for compaction to catch up",
                                    );
                                    logged_hard_limit = true;
                                }
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                continue;
                            }
                            Err(e) => {
                                return Err(Error::StoreError {
                                    sequencer_id,
                                    source: Box::new(e),
                                });
                            }
                        }
                    }

                    // done replaying?
                    if sequence.number == min_max.max() {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        convert::TryFrom,
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow_util::assert_batches_eq;
    use chrono::Utc;
    use data_types::{
        database_rules::{PartitionTemplate, Partitioner, TemplatePart},
        server_id::ServerId,
    };
    use entry::{
        test_helpers::{lp_to_entries, lp_to_entry},
        Sequence, SequencedEntry,
    };
    use object_store::ObjectStore;
    use persistence_windows::{
        checkpoint::{PartitionCheckpoint, PersistCheckpointBuilder, ReplayPlanner},
        min_max_sequence::OptionalMinMaxSequence,
    };
    use query::{exec::ExecutorType, frontend::sql::SqlQueryPlanner, QueryChunk};
    use test_helpers::{assert_contains, tracing::TracingCapture};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use write_buffer::{
        config::WriteBufferConfig,
        mock::{MockBufferForReading, MockBufferSharedState},
    };

    use crate::utils::TestDb;

    #[derive(Debug)]
    struct TestSequencedEntry {
        sequencer_id: u32,
        sequence_number: u64,
        lp: &'static str,
    }

    impl TestSequencedEntry {
        fn build(self, partitioner: &impl Partitioner) -> SequencedEntry {
            let Self {
                sequencer_id,
                sequence_number,
                lp,
            } = self;

            let mut entries = lp_to_entries(lp, partitioner);
            assert_eq!(entries.len(), 1);

            SequencedEntry::new_from_sequence(
                Sequence::new(sequencer_id, sequence_number),
                Utc::now(),
                entries.pop().unwrap(),
            )
        }
    }

    /// Different checks for replay tests
    #[derive(Debug)]
    enum Check {
        /// Check that the set of partitions is as expected.
        ///
        /// The partitions are by table name and partition key.
        ///
        /// **NOTE: These must be in order, otherwise the check might fail!**
        Partitions(Vec<(&'static str, &'static str)>),

        /// Check query results.
        Query(&'static str, Vec<&'static str>),
    }

    /// Action or check for replay test.
    #[derive(Debug)]
    enum Step {
        /// Ingest new entries into the write buffer.
        Ingest(Vec<TestSequencedEntry>),

        /// Restart DB
        ///
        /// Background loop is started as well but neither persistence nor write buffer reads are allowed until
        /// [`Await`](Self::Await) is used.
        Restart,

        /// Perform replay
        Replay,

        /// Persist partitions.
        ///
        /// The partitions are by table name and partition key.
        Persist(Vec<(&'static str, &'static str)>),

        /// Assert that all these checks pass.
        Assert(Vec<Check>),

        /// Wait that background loop generates desired state (all checks pass).
        ///
        /// Persistence and write buffer reads are enabled in preparation to this step.
        Await(Vec<Check>),
    }

    #[derive(Debug)]
    struct ReplayTest {
        /// Number of sequencers in this test setup.
        n_sequencers: u32,

        /// What to do in which order.
        ///
        /// # Serialization
        /// Every step is finished and
        /// checked for errors before the next is started (e.g. [`Replay`](Step::Replay) is fully executed and
        /// it is ensured that there were no errors before a subsequent [`Assert`](Step::Assert) is evaluated).
        ///
        /// # Await / Background Worker
        /// The database background worker is started during when the DB is created but persistence and reads from the
        /// write buffer are disabled until [`Await`](Step::Await) is used.
        steps: Vec<Step>,
    }

    impl ReplayTest {
        async fn run(self) {
            // Test that data is replayed from write buffers

            // ==================== setup ====================
            let object_store = Arc::new(ObjectStore::new_in_memory());
            let server_id = ServerId::try_from(1).unwrap();
            let db_name = "replay_test";
            let partition_template = PartitionTemplate {
                parts: vec![TemplatePart::Column("tag_partition_by".to_string())],
            };
            let write_buffer_state =
                MockBufferSharedState::empty_with_n_sequencers(self.n_sequencers);
            let (mut test_db, mut shutdown, mut join_handle) = Self::create_test_db(
                Arc::clone(&object_store),
                write_buffer_state.clone(),
                server_id,
                db_name,
                partition_template.clone(),
            )
            .await;

            // ==================== do: main loop ====================
            for (step, action_or_check) in self.steps.into_iter().enumerate() {
                println!("===== step {} =====\n{:?}", step + 1, action_or_check);

                match action_or_check {
                    Step::Ingest(entries) => {
                        for se in entries {
                            write_buffer_state.push_entry(se.build(&partition_template));
                        }
                    }
                    Step::Restart => {
                        // stop background worker
                        shutdown.cancel();
                        join_handle.await.unwrap();

                        // drop old DB
                        drop(test_db);

                        // then create new one
                        let (test_db_tmp, shutdown_tmp, join_handle_tmp) = Self::create_test_db(
                            Arc::clone(&object_store),
                            write_buffer_state.clone(),
                            server_id,
                            db_name,
                            partition_template.clone(),
                        )
                        .await;
                        test_db = test_db_tmp;
                        shutdown = shutdown_tmp;
                        join_handle = join_handle_tmp;
                    }
                    Step::Replay => {
                        let db = &test_db.db;

                        db.perform_replay(&test_db.replay_plan, false)
                            .await
                            .unwrap();
                    }
                    Step::Persist(partitions) => {
                        let db = &test_db.db;

                        for (table_name, partition_key) in partitions {
                            // Mark the MB chunk close
                            let chunk_id = {
                                let mb_chunk = db
                                    .rollover_partition(table_name, partition_key)
                                    .await
                                    .unwrap()
                                    .unwrap();
                                mb_chunk.id()
                            };
                            // Move that MB chunk to RB chunk and drop it from MB
                            db.move_chunk_to_read_buffer(table_name, partition_key, chunk_id)
                                .await
                                .unwrap();

                            // Write the RB chunk to Object Store but keep it in RB
                            db.persist_partition(
                                table_name,
                                partition_key,
                                Instant::now() + Duration::from_secs(1),
                            )
                            .await
                            .unwrap();
                        }
                    }
                    Step::Assert(checks) => {
                        Self::eval_checks(&checks, true, &test_db).await;
                    }
                    Step::Await(checks) => {
                        let db = &test_db.db;
                        db.unsuppress_persistence().await;
                        db.allow_write_buffer_read();

                        // wait until checks pass
                        let t_0 = Instant::now();
                        loop {
                            println!("Try checks...");
                            if Self::eval_checks(&checks, false, &test_db).await {
                                break;
                            }

                            if t_0.elapsed() >= Duration::from_secs(10) {
                                println!("Running into timeout...");
                                // try to produce nice assertion message
                                Self::eval_checks(&checks, true, &test_db).await;
                                println!("being lucky, assertion passed on last try.");
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }

        fn get_partitions(db: &Db) -> Vec<(String, String)> {
            let mut partitions: Vec<_> = db
                .catalog
                .partitions()
                .into_iter()
                .map(|partition| {
                    let partition = partition.read();
                    (
                        partition.table_name().to_string(),
                        partition.key().to_string(),
                    )
                })
                .collect();
            partitions.sort();
            partitions
        }

        async fn create_test_db(
            object_store: Arc<ObjectStore>,
            write_buffer_state: MockBufferSharedState,
            server_id: ServerId,
            db_name: &'static str,
            partition_template: PartitionTemplate,
        ) -> (TestDb, CancellationToken, JoinHandle<()>) {
            let write_buffer = MockBufferForReading::new(write_buffer_state);
            let test_db = TestDb::builder()
                .object_store(object_store)
                .server_id(server_id)
                .write_buffer(WriteBufferConfig::Reading(Arc::new(
                    tokio::sync::Mutex::new(Box::new(write_buffer) as _),
                )))
                .lifecycle_rules(data_types::database_rules::LifecycleRules {
                    buffer_size_hard: Some(NonZeroUsize::new(10_000).unwrap()),
                    late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                    mub_row_threshold: NonZeroUsize::new(10).unwrap(),
                    ..Default::default()
                })
                .partition_template(partition_template)
                .db_name(db_name)
                .build()
                .await;

            // start background worker
            let shutdown: CancellationToken = Default::default();
            let shutdown_captured = shutdown.clone();
            let db_captured = Arc::clone(&test_db.db);
            let join_handle =
                tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });

            (test_db, shutdown, join_handle)
        }

        /// Evaluates given checks.
        ///
        /// If `use_assert = true` this function will panic in case of a failure. If `use_assert = false` it will just
        /// return `false` in the error case. `true` is returned in all checks passed.
        async fn eval_checks(checks: &[Check], use_assert: bool, test_db: &TestDb) -> bool {
            let db = &test_db.db;

            for (step, check) in checks.iter().enumerate() {
                println!("check {}: {:?}", step + 1, check);

                let res = match check {
                    Check::Partitions(partitions) => {
                        let partitions_actual = Self::get_partitions(db);
                        let partitions_actual: Vec<(&str, &str)> = partitions_actual
                            .iter()
                            .map(|(s1, s2)| (s1.as_ref(), s2.as_ref()))
                            .collect();

                        Self::eval_assert(
                            || {
                                assert_eq!(&partitions_actual, partitions);
                            },
                            use_assert,
                        )
                    }
                    Check::Query(query, expected) => {
                        let db = Arc::clone(db);
                        let planner = SqlQueryPlanner::default();
                        let executor = db.executor();

                        match planner.query(db, query, &executor) {
                            Ok(physical_plan) => {
                                match executor.collect(physical_plan, ExecutorType::Query).await {
                                    Ok(batches) => {
                                        // we are throwing away the record batches after the assert, so we don't care about interior
                                        // mutability
                                        let batches = std::panic::AssertUnwindSafe(batches);

                                        Self::eval_assert(
                                            || {
                                                assert_batches_eq!(expected, &batches);
                                            },
                                            use_assert,
                                        )
                                    }
                                    err if use_assert => {
                                        err.unwrap();
                                        unreachable!()
                                    }
                                    _ => false,
                                }
                            }
                            err if use_assert => {
                                err.unwrap();
                                unreachable!()
                            }
                            _ => false,
                        }
                    }
                };

                if !res {
                    return false;
                }
            }

            true
        }

        /// Evaluates given function that may contain an `assert`.
        ///
        /// This helper allows you use the same `assert` statement no matter if you want the error case to panic or if
        /// you just want to have an boolean expression that states "did it succeed?".
        ///
        /// Behavior dependson `raise`:
        ///
        /// - `raise = true`: The given function `f` will be executed. If it panics, `eval_assert` will just bubble up
        ///   the error (aka panic as well). If the function `f` does not panic, `true` is returned (aka "it
        ///   succeeded").
        /// - `raise = false`: The given function `f` will be executed but unwinds will be caught. If `f` did panic
        ///   `false` will be returned (aka "it failed"), otherwise `true` will be returned (aka "it succeeded").
        fn eval_assert<F>(f: F, raise: bool) -> bool
        where
            F: Fn() + std::panic::UnwindSafe,
        {
            if raise {
                f();
                true
            } else {
                std::panic::catch_unwind(f).is_ok()
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: ")]
    async fn test_framework_fail_assert_partitions() {
        // Test the test framework: checking the partitions should actually fail
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                // that passes
                Step::Assert(vec![Check::Partitions(vec![])]),
                // that fails
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: ")]
    async fn test_framework_fail_assert_query() {
        // Test the test framework: checking the query results should actually fail
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 0,
                    lp: "table_1,tag_partition_by=a bar=1 0",
                }]),
                Step::Await(vec![Check::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                // that fails
                Step::Assert(vec![Check::Query(
                    "select * from table_1",
                    vec![
                        "+-----+------------------+----------------------+",
                        "| bar | tag_partition_by | time                 |",
                        "+-----+------------------+----------------------+",
                        "| 2   | a                | 1970-01-01T00:00:00Z |",
                        "+-----+------------------+----------------------+",
                    ],
                )]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_two_partitions_persist_second() {
        // acts as regression test for the following PRs:
        // - https://github.com/influxdata/influxdb_iox/pull/2079
        // - https://github.com/influxdata/influxdb_iox/pull/2084
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_2,tag_partition_by=a bar=20 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::Persist(vec![("table_2", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=b bar=11 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 3,
                        lp: "table_2,tag_partition_by=b bar=21 10",
                    },
                ]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_b"),
                ])]),
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z           |",
                            "| 11  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z           |",
                            "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_two_partitions_persist_first() {
        // acts as regression test for the following PRs:
        // - https://github.com/influxdata/influxdb_iox/pull/2079
        // - https://github.com/influxdata/influxdb_iox/pull/2084
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_2,tag_partition_by=a bar=20 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=b bar=11 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 3,
                        lp: "table_2,tag_partition_by=b bar=21 10",
                    },
                ]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_b"),
                ])]),
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z           |",
                            "| 11  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z           |",
                            "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_nothing_to_replay() {
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![])]),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    lp: "table_1,tag_partition_by=a bar=1 0",
                }]),
                Step::Replay,
                Step::Assert(vec![Check::Partitions(vec![])]),
                Step::Await(vec![Check::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by time",
                    vec![
                        "+-----+------------------+----------------------+",
                        "| bar | tag_partition_by | time                 |",
                        "+-----+------------------+----------------------+",
                        "| 1   | a                | 1970-01-01T00:00:00Z |",
                        "+-----+------------------+----------------------+",
                    ],
                )]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_different_sequencer_situations() {
        // three sequencers:
        //   0: no data at all
        //   1: replay required, additional incoming data during downtime
        //   2: replay required, no new data
        //
        // three partitions:
        //   table 1, partition a: comes from sequencer 1 and 2, gets persisted
        //   table 1, partition b: part of the new data in sequencer 1
        //   table 2: partition a: from sequencer 1, not persisted but recovered during replay
        ReplayTest {
            n_sequencers: 3,
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 1,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 2,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=11 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 2,
                        sequence_number: 1,
                        lp: "table_2,tag_partition_by=a bar=20 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 1,
                    lp: "table_1,tag_partition_by=b bar=12 20",
                }]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z           |",
                            "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z           |",
                            "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 12  | b                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                ]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_interleaved_writes() {
        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                // let's ingest some data for two partitions a and b
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=b bar=20 0",
                    },
                ]),
                Step::Await(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+----------------------+",
                        "| bar | tag_partition_by | time                 |",
                        "+-----+------------------+----------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z |",
                        "+-----+------------------+----------------------+",
                    ],
                )]),
                // only persist partition a
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                // ================================================================================
                // after restart the non-persisted partition (B) is gone
                Step::Restart,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+----------------------+",
                        "| bar | tag_partition_by | time                 |",
                        "+-----+------------------+----------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z |",
                        "+-----+------------------+----------------------+",
                    ],
                )]),
                // ...but replay can bring the data back without ingesting more data
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=a bar=11 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 3,
                        lp: "table_1,tag_partition_by=b bar=21 10",
                    },
                ]),
                Step::Replay,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+----------------------+",
                        "| bar | tag_partition_by | time                 |",
                        "+-----+------------------+----------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z |",
                        "+-----+------------------+----------------------+",
                    ],
                )]),
                // now wait for all the to-be-ingested data
                Step::Await(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // ...and only persist partition a (a 2nd time)
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                // ================================================================================
                // after restart partition b will be gone again
                Step::Restart,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // ...but again replay can bring the data back without ingesting more data
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 4,
                        lp: "table_1,tag_partition_by=b bar=22 20",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 5,
                        lp: "table_1,tag_partition_by=a bar=12 20",
                    },
                ]),
                Step::Replay,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // now wait for all the to-be-ingested data
                Step::Await(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 12  | a                | 1970-01-01T00:00:00.000000020Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "| 22  | b                | 1970-01-01T00:00:00.000000020Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // this time only persist partition b
                Step::Persist(vec![("table_1", "tag_partition_by_b")]),
                // ================================================================================
                // after restart partition b will be fully present but the latest data for partition a is gone
                Step::Restart,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "| 22  | b                | 1970-01-01T00:00:00.000000020Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // ...but again replay can bring the data back without ingesting more data
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 6,
                        lp: "table_1,tag_partition_by=b bar=23 30",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 7,
                        lp: "table_1,tag_partition_by=a bar=13 30",
                    },
                ]),
                Step::Replay,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 12  | a                | 1970-01-01T00:00:00.000000020Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "| 22  | b                | 1970-01-01T00:00:00.000000020Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // now wait for all the to-be-ingested data
                Step::Await(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 12  | a                | 1970-01-01T00:00:00.000000020Z |",
                        "| 13  | a                | 1970-01-01T00:00:00.000000030Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "| 22  | b                | 1970-01-01T00:00:00.000000020Z |",
                        "| 23  | b                | 1970-01-01T00:00:00.000000030Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
                // finally persist both partitions
                Step::Persist(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                ]),
                // ================================================================================
                // and after this restart nothing is lost
                Step::Restart,
                Step::Assert(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00Z           |",
                        "| 11  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 12  | a                | 1970-01-01T00:00:00.000000020Z |",
                        "| 13  | a                | 1970-01-01T00:00:00.000000030Z |",
                        "| 20  | b                | 1970-01-01T00:00:00Z           |",
                        "| 21  | b                | 1970-01-01T00:00:00.000000010Z |",
                        "| 22  | b                | 1970-01-01T00:00:00.000000020Z |",
                        "| 23  | b                | 1970-01-01T00:00:00.000000030Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
            ],
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_compacts() {
        let tracing_capture = TracingCapture::new();

        // these numbers are handtuned to trigger hard buffer limits w/o making the test too big
        let n_entries = 50u64;
        let sequenced_entries: Vec<_> = (0..n_entries)
            .map(|sequence_number| {
                let lp = format!(
                    "table_1,tag_partition_by=a foo=\"hello\",bar=10 {}",
                    sequence_number / 2
                );
                let lp: &'static str = Box::leak(Box::new(lp));
                TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number,
                    lp,
                }
            })
            .collect();

        ReplayTest {
            n_sequencers: 1,
            steps: vec![
                Step::Ingest(sequenced_entries),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: n_entries,
                    lp: "table_2,tag_partition_by=a bar=11 10",
                }]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::Persist(vec![("table_2", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
                Step::Replay,
            ],
        }
        .run()
        .await;

        // check that hard buffer limit was actually hit (otherwise this test is pointless/outdated)
        assert_contains!(
            tracing_capture.to_string(),
            "Hard limit reached while replaying, waiting for compaction to catch up"
        );
    }

    #[tokio::test]
    async fn replay_fail_sequencers_change() {
        // create write buffer w/ sequencer 0 and 1
        let write_buffer_state = MockBufferSharedState::empty_with_n_sequencers(2);
        let write_buffer = MockBufferForReading::new(write_buffer_state);

        // create DB
        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await
            .db;

        // construct replay plan for sequencers 0 and 2
        let mut sequencer_numbers = BTreeMap::new();
        sequencer_numbers.insert(0, OptionalMinMaxSequence::new(Some(0), 1));
        sequencer_numbers.insert(2, OptionalMinMaxSequence::new(Some(0), 1));
        let partition_checkpoint = PartitionCheckpoint::new(
            Arc::from("table"),
            Arc::from("partition"),
            sequencer_numbers,
            Utc::now(),
        );
        let builder = PersistCheckpointBuilder::new(partition_checkpoint);
        let (partition_checkpoint, database_checkpoint) = builder.build();
        let mut replay_planner = ReplayPlanner::new();
        replay_planner.register_checkpoints(&partition_checkpoint, &database_checkpoint);
        let replay_plan = replay_planner.build().unwrap();

        // replay fails
        let res = db.perform_replay(&replay_plan, false).await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Replay plan references unknown sequencer"
        );
    }

    #[tokio::test]
    async fn replay_fail_lost_entry() {
        // create write buffer state with sequence number 0 and 2, 1 is missing
        let write_buffer_state = MockBufferSharedState::empty_with_n_sequencers(1);
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 0),
            Utc::now(),
            lp_to_entry("cpu bar=1 0"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 2),
            Utc::now(),
            lp_to_entry("cpu bar=1 10"),
        ));
        let write_buffer = MockBufferForReading::new(write_buffer_state);

        // create DB
        let db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await
            .db;

        // construct replay plan to replay sequence numbers 0 and 1
        let mut sequencer_numbers = BTreeMap::new();
        sequencer_numbers.insert(0, OptionalMinMaxSequence::new(Some(0), 1));
        let partition_checkpoint = PartitionCheckpoint::new(
            Arc::from("table"),
            Arc::from("partition"),
            sequencer_numbers,
            Utc::now(),
        );
        let builder = PersistCheckpointBuilder::new(partition_checkpoint);
        let (partition_checkpoint, database_checkpoint) = builder.build();
        let mut replay_planner = ReplayPlanner::new();
        replay_planner.register_checkpoints(&partition_checkpoint, &database_checkpoint);
        let replay_plan = replay_planner.build().unwrap();

        // replay fails
        let res = db.perform_replay(&replay_plan, false).await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot replay: For sequencer 0 expected to find sequence 1 but replay jumped to 2"
        );
    }

    #[tokio::test]
    async fn seek_to_end_works() {
        // setup watermarks:
        // 0 -> 3 + 1 = 4
        // 1 -> 1 + 1 = 2
        // 2 -> no content = 0
        let write_buffer_state = MockBufferSharedState::empty_with_n_sequencers(3);
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 0),
            Utc::now(),
            lp_to_entry("cpu bar=0 0"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 3),
            Utc::now(),
            lp_to_entry("cpu bar=3 3"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(1, 1),
            Utc::now(),
            lp_to_entry("cpu bar=11 11"),
        ));
        let write_buffer = MockBufferForReading::new(write_buffer_state.clone());

        // create DB
        let test_db = TestDb::builder()
            .write_buffer(WriteBufferConfig::Reading(Arc::new(
                tokio::sync::Mutex::new(Box::new(write_buffer) as _),
            )))
            .build()
            .await;
        let db = &test_db.db;

        // seek
        db.perform_replay(&test_db.replay_plan, true).await.unwrap();

        // add more data
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(0, 4),
            Utc::now(),
            lp_to_entry("cpu bar=4 4"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(1, 9),
            Utc::now(),
            lp_to_entry("cpu bar=19 19"),
        ));
        write_buffer_state.push_entry(SequencedEntry::new_from_sequence(
            Sequence::new(2, 0),
            Utc::now(),
            lp_to_entry("cpu bar=20 20"),
        ));

        // start background worker
        let shutdown: CancellationToken = Default::default();
        let shutdown_captured = shutdown.clone();
        let db_captured = Arc::clone(db);
        let join_handle =
            tokio::spawn(async move { db_captured.background_worker(shutdown_captured).await });
        db.allow_write_buffer_read();

        // wait until checks pass
        let checks = vec![Check::Query(
            "select * from cpu order by time",
            vec![
                "+-----+--------------------------------+",
                "| bar | time                           |",
                "+-----+--------------------------------+",
                "| 4   | 1970-01-01T00:00:00.000000004Z |",
                "| 19  | 1970-01-01T00:00:00.000000019Z |",
                "| 20  | 1970-01-01T00:00:00.000000020Z |",
                "+-----+--------------------------------+",
            ],
        )];
        let t_0 = Instant::now();
        loop {
            println!("Try checks...");
            if ReplayTest::eval_checks(&checks, false, &test_db).await {
                break;
            }

            if t_0.elapsed() >= Duration::from_secs(10) {
                println!("Running into timeout...");
                // try to produce nice assertion message
                ReplayTest::eval_checks(&checks, true, &test_db).await;
                println!("being lucky, assertion passed on last try.");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // stop background worker
        shutdown.cancel();
        join_handle.await.unwrap();
    }
}
