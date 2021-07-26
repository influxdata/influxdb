use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use futures::TryStreamExt;
use observability_deps::tracing::info;
use persistence_windows::checkpoint::ReplayPlan;
use snafu::{ResultExt, Snafu};
use write_buffer::config::WriteBufferConfig;

use crate::Db;

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
                    db.store_sequenced_entry(entry)
                        .map_err(Box::new)
                        .context(StoreError { sequencer_id })?;

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
        num::NonZeroU32,
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow_util::assert_batches_eq;
    use chrono::Utc;
    use data_types::{
        database_rules::{PartitionTemplate, Partitioner, TemplatePart},
        server_id::ServerId,
    };
    use entry::{test_helpers::lp_to_entries, Sequence, SequencedEntry};
    use object_store::ObjectStore;
    use persistence_windows::{
        checkpoint::{PartitionCheckpoint, PersistCheckpointBuilder, ReplayPlanner},
        min_max_sequence::OptionalMinMaxSequence,
    };
    use query::QueryChunk;
    use test_helpers::assert_contains;
    use tokio_util::sync::CancellationToken;
    use write_buffer::{
        config::WriteBufferConfig,
        mock::{MockBufferForReading, MockBufferSharedState},
    };

    use crate::{db::test_helpers::run_query, utils::TestDb};

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
                entries.pop().unwrap(),
            )
            .unwrap()
        }
    }

    /// Different checks for replay tests
    #[derive(Debug)]
    enum ReplayTestCheck {
        /// Check that the set of partitions is as expected.
        ///
        /// The partitions are by table name and partition key.
        ///
        /// **NOTE: These must be in order, otherwise the check might fail!**
        Partitions(Vec<(&'static str, &'static str)>),

        /// Check query results.
        Query(&'static str, Vec<&'static str>),
    }

    /// Action cor check for replay test.
    #[derive(Debug)]
    enum ReplayTestAOC {
        /// Ingest new entries into the write buffer.
        Ingest(Vec<TestSequencedEntry>),

        /// Restart DB
        Restart,

        /// Perform replay
        Replay,

        /// Persist partitions.
        ///
        /// The partitions are by table name and partition key.
        Persist(Vec<(&'static str, &'static str)>),

        /// Assert that all these checks pass.
        Assert(Vec<ReplayTestCheck>),

        /// Wait that background loop generates desired state (all checks pass).
        ///
        /// Background loop is started before the check and stopped afterwards.
        Await(Vec<ReplayTestCheck>),
    }

    #[derive(Debug)]
    struct ReplayTest {
        /// Number of sequencers in this test setup.
        n_sequencers: u32,

        /// What to do in which order
        actions_and_checks: Vec<ReplayTestAOC>,
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
            let mut test_db = Some(
                Self::create_test_db(
                    Arc::clone(&object_store),
                    write_buffer_state.clone(),
                    server_id,
                    db_name,
                    partition_template.clone(),
                )
                .await,
            );

            // ==================== do: main loop ====================
            for (step, action_or_check) in self.actions_and_checks.into_iter().enumerate() {
                println!("===== step {} =====\n{:?}", step + 1, action_or_check);

                match action_or_check {
                    ReplayTestAOC::Ingest(entries) => {
                        for se in entries {
                            write_buffer_state.push_entry(se.build(&partition_template));
                        }
                    }
                    ReplayTestAOC::Restart => {
                        // first drop old DB
                        #[allow(unused_assignments)]
                        {
                            test_db = None;
                        }

                        // then create new one
                        test_db = Some(
                            Self::create_test_db(
                                Arc::clone(&object_store),
                                write_buffer_state.clone(),
                                server_id,
                                db_name,
                                partition_template.clone(),
                            )
                            .await,
                        );
                    }
                    ReplayTestAOC::Replay => {
                        let test_db = test_db.as_ref().unwrap();

                        test_db
                            .db
                            .perform_replay(&test_db.replay_plan)
                            .await
                            .unwrap();
                    }
                    ReplayTestAOC::Persist(partitions) => {
                        let test_db = test_db.as_ref().unwrap();
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
                    ReplayTestAOC::Assert(checks) => {
                        let test_db = test_db.as_ref().unwrap();
                        Self::eval_checks(&checks, true, test_db).await;
                    }
                    ReplayTestAOC::Await(checks) => {
                        let test_db = test_db.as_ref().unwrap();
                        let db = &test_db.db;

                        // start background worker
                        let shutdown: CancellationToken = Default::default();
                        let shutdown_captured = shutdown.clone();
                        let db_captured = Arc::clone(&db);
                        let join_handle = tokio::spawn(async move {
                            db_captured.background_worker(shutdown_captured).await
                        });

                        // wait until checks pass
                        let t_0 = Instant::now();
                        loop {
                            println!("Try checks...");
                            if Self::eval_checks(&checks, false, test_db).await {
                                break;
                            }

                            if t_0.elapsed() >= Duration::from_secs(10) {
                                println!("Running into timeout...");
                                // try to produce nice assertion message
                                Self::eval_checks(&checks, true, test_db).await;
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
        ) -> TestDb {
            let write_buffer = MockBufferForReading::new(write_buffer_state);
            TestDb::builder()
                .object_store(object_store)
                .server_id(server_id)
                .write_buffer(WriteBufferConfig::Reading(Arc::new(
                    tokio::sync::Mutex::new(Box::new(write_buffer) as _),
                )))
                .lifecycle_rules(data_types::database_rules::LifecycleRules {
                    late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                    ..Default::default()
                })
                .partition_template(partition_template)
                .db_name(db_name)
                .build()
                .await
        }

        async fn eval_checks(
            checks: &[ReplayTestCheck],
            use_assert: bool,
            test_db: &TestDb,
        ) -> bool {
            let db = &test_db.db;

            for (step, check) in checks.iter().enumerate() {
                println!("check {}: {:?}", step + 1, check);

                let res = match check {
                    ReplayTestCheck::Partitions(partitions) => {
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
                    ReplayTestCheck::Query(query, expected) => {
                        let batches = run_query(Arc::clone(&db), query).await;

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
                };

                if !res {
                    return false;
                }
            }

            true
        }

        fn eval_assert<F>(f: F, bubble: bool) -> bool
        where
            F: Fn() + std::panic::UnwindSafe,
        {
            if bubble {
                f();
                true
            } else {
                std::panic::catch_unwind(f).is_ok()
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: ")]
    async fn replay_metatest_fail_assert_partitions() {
        ReplayTest {
            n_sequencers: 1,
            actions_and_checks: vec![
                // that passes
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![])]),
                // that fails
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![(
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
    async fn replay_metatest_fail_assert_query() {
        ReplayTest {
            n_sequencers: 1,
            actions_and_checks: vec![
                ReplayTestAOC::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 0,
                    lp: "table_1,tag_partition_by=a bar=1 0",
                }]),
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                // that fails
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Query(
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
        ReplayTest {
            n_sequencers: 1,
            actions_and_checks: vec![
                ReplayTestAOC::Ingest(vec![
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                ReplayTestAOC::Persist(vec![("table_2", "tag_partition_by_a")]),
                ReplayTestAOC::Restart,
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
                ReplayTestAOC::Ingest(vec![
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
                ReplayTestAOC::Replay,
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    ReplayTestCheck::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    ReplayTestCheck::Query(
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_b"),
                ])]),
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Query(
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
                    ReplayTestCheck::Query(
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
        ReplayTest {
            n_sequencers: 1,
            actions_and_checks: vec![
                ReplayTestAOC::Ingest(vec![
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                ReplayTestAOC::Persist(vec![("table_1", "tag_partition_by_a")]),
                ReplayTestAOC::Restart,
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                ReplayTestAOC::Ingest(vec![
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
                ReplayTestAOC::Replay,
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    ReplayTestCheck::Query(
                        "select * from table_1 order by time",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    ReplayTestCheck::Query(
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_b"),
                ])]),
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Query(
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
                    ReplayTestCheck::Query(
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
            actions_and_checks: vec![
                ReplayTestAOC::Restart,
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![])]),
                ReplayTestAOC::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    lp: "table_1,tag_partition_by=a bar=1 0",
                }]),
                ReplayTestAOC::Replay,
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![])]),
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Query(
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
            actions_and_checks: vec![
                ReplayTestAOC::Ingest(vec![
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                ReplayTestAOC::Persist(vec![("table_1", "tag_partition_by_a")]),
                ReplayTestAOC::Restart,
                ReplayTestAOC::Assert(vec![ReplayTestCheck::Partitions(vec![(
                    "table_1",
                    "tag_partition_by_a",
                )])]),
                ReplayTestAOC::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 1,
                    lp: "table_1,tag_partition_by=b bar=12 20",
                }]),
                ReplayTestAOC::Replay,
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_2", "tag_partition_by_a"),
                    ]),
                    ReplayTestCheck::Query(
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
                    ReplayTestCheck::Query(
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
                ReplayTestAOC::Await(vec![ReplayTestCheck::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                ReplayTestAOC::Assert(vec![
                    ReplayTestCheck::Query(
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
                    ReplayTestCheck::Query(
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
        let res = db.perform_replay(&replay_plan).await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Replay plan references unknown sequencer"
        );
    }
}
