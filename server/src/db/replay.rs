use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use entry::{Sequence, TableBatch};
use futures::TryStreamExt;
use observability_deps::tracing::info;
use persistence_windows::{
    checkpoint::{PartitionCheckpoint, ReplayPlan},
    min_max_sequence::OptionalMinMaxSequence,
    persistence_windows::PersistenceWindows,
};
use snafu::{ResultExt, Snafu};
use write_buffer::core::WriteBufferReading;

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
pub async fn seek_to_end(db: &Db, write_buffer: &mut dyn WriteBufferReading) -> Result<()> {
    let mut watermarks = vec![];
    for (sequencer_id, stream) in write_buffer.streams() {
        let watermark = (stream.fetch_high_watermark)()
            .await
            .context(SeekError { sequencer_id })?;
        watermarks.push((sequencer_id, watermark));
    }

    for (sequencer_id, watermark) in &watermarks {
        write_buffer
            .seek(*sequencer_id, *watermark)
            .await
            .context(SeekError {
                sequencer_id: *sequencer_id,
            })?;
    }

    // remember max seen sequence numbers
    let late_arrival_window = db.rules().lifecycle_rules.late_arrive_window();
    let sequencer_numbers: BTreeMap<_, _> = watermarks
        .into_iter()
        .filter(|(_sequencer_id, watermark)| *watermark > 0)
        .map(|(sequencer_id, watermark)| {
            (
                sequencer_id,
                OptionalMinMaxSequence::new(None, watermark - 1),
            )
        })
        .collect();

    for partition in db.catalog.partitions() {
        let mut partition = partition.write();

        let dummy_checkpoint = PartitionCheckpoint::new(
            Arc::from(partition.table_name()),
            Arc::from(partition.key()),
            sequencer_numbers.clone(),
            Utc::now(),
        );

        match partition.persistence_windows_mut() {
            Some(windows) => {
                windows.mark_seen_and_persisted(&dummy_checkpoint);
            }
            None => {
                let mut windows = PersistenceWindows::new(
                    partition.addr().clone(),
                    late_arrival_window,
                    db.utc_now(),
                );
                windows.mark_seen_and_persisted(&dummy_checkpoint);
                partition.set_persistence_windows(windows);
            }
        }
    }

    Ok(())
}

/// Perform sequencer-driven replay for this DB.
pub async fn perform_replay(
    db: &Db,
    replay_plan: &ReplayPlan,
    write_buffer: &mut dyn WriteBufferReading,
) -> Result<()> {
    let db_name = db.rules.read().db_name().to_string();
    info!(%db_name, "starting replay");

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
                let n_tries = 600; // 600*100ms = 60s
                for n_try in 1..=n_tries {
                    match db.store_sequenced_entry(
                        Arc::clone(&entry),
                        |sequence, partition_key, table_batch| {
                            filter_entry(sequence, partition_key, table_batch, replay_plan)
                        },
                        db.utc_now(),
                    ) {
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

    // remember max seen sequence numbers even for partitions that were not touched during replay
    let late_arrival_window = db.rules().lifecycle_rules.late_arrive_window();
    for (table_name, partition_key) in replay_plan.partitions() {
        if let Ok(partition) = db.partition(&table_name, &partition_key) {
            let mut partition = partition.write();
            let partition_checkpoint = replay_plan
                .last_partition_checkpoint(&table_name, &partition_key)
                .expect("replay plan inconsistent");

            match partition.persistence_windows_mut() {
                Some(windows) => {
                    windows.mark_seen_and_persisted(partition_checkpoint);
                }
                None => {
                    let mut windows = PersistenceWindows::new(
                        partition.addr().clone(),
                        late_arrival_window,
                        db.utc_now(),
                    );
                    windows.mark_seen_and_persisted(partition_checkpoint);
                    partition.set_persistence_windows(windows);
                }
            }
        }
    }

    Ok(())
}

fn filter_entry(
    sequence: Option<&Sequence>,
    partition_key: &str,
    table_batch: &TableBatch<'_>,
    replay_plan: &ReplayPlan,
) -> (bool, Option<Vec<bool>>) {
    let sequence = sequence.expect("write buffer results must be sequenced");
    let table_name = table_batch.name();

    // Check if we have a partition checkpoint that contains data for this specific sequencer
    let min_unpersisted_ts_and_sequence_range = replay_plan
        .last_partition_checkpoint(table_name, partition_key)
        .map(|partition_checkpoint| {
            partition_checkpoint
                .sequencer_numbers(sequence.id)
                .map(|min_max| (partition_checkpoint.min_unpersisted_timestamp(), min_max))
        })
        .flatten();

    match min_unpersisted_ts_and_sequence_range {
        Some((min_unpersisted_ts, min_max)) => {
            // Figure out what the sequence number tells us about the entire batch
            match SequenceNumberSection::compare(sequence.number, min_max) {
                SequenceNumberSection::Persisted => {
                    // skip the entire batch
                    (false, None)
                }
                SequenceNumberSection::PartiallyPersisted => {
                    // TODO: implement row filtering, for now replay the entire batch
                    let maybe_mask = table_batch.timestamps().ok().map(|timestamps| {
                        let min_unpersisted_ts = min_unpersisted_ts.timestamp_nanos();
                        timestamps
                            .into_iter()
                            .map(|ts_row| ts_row >= min_unpersisted_ts)
                            .collect::<Vec<bool>>()
                    });
                    (true, maybe_mask)
                }
                SequenceNumberSection::Unpersisted => {
                    // replay entire batch
                    (true, None)
                }
            }
        }
        None => {
            // One of the following two cases:
            // - We have never written a checkpoint for this partition, which means nothing is persisted yet.
            // - Unknown sequencer (at least from the partitions point of view).
            //
            // => Replay full batch.
            (true, None)
        }
    }
}

/// Where is a given sequence number and the entire data batch associated with it compared to the range of persisted and
/// partially persisted sequence numbers (extracted from partition checkpoint).
#[derive(Debug, PartialEq)]
enum SequenceNumberSection {
    /// The entire batch of data has previously been persisted.
    Persisted,

    /// The entire batch of data may or may not have been persisted.
    ///
    /// The data in this batch may or may not have been persisted.
    /// The row timestamp must be compared against the flush timestamp to decide on a row-be-row basis if the data
    /// should be replayed or not.
    PartiallyPersisted,

    /// The entire batch of data is unpersisted and must be replayed.
    Unpersisted,
}

impl SequenceNumberSection {
    /// Compare sequence number against given sequence range.
    fn compare(sequence_number: u64, min_max: OptionalMinMaxSequence) -> Self {
        // Are we beyond the last seen (at the time when the last partition write happened) sequence already?
        if sequence_number > min_max.max() {
            // Entry appeared after the last partition write.
            Self::Unpersisted
        } else {
            // We have seen this entry already. Now the question is if we can safely assume that it was
            // fully persisted.
            match min_max.min() {
                Some(min) => {
                    // The range of potentially unpersisted sequences is not empty.
                    if sequence_number >= min {
                        // The data might be unpersisted.
                        Self::PartiallyPersisted
                    } else {
                        // The entry was definitely fully persisted.
                        Self::Persisted
                    }
                }
                None => {
                    // The range of potentially unpersisted entries is empty, aka the data up to and including `max`
                    // is fully persisted. We are below or equal to `max` in this branch, so this entry is fully
                    // persisted.
                    Self::Persisted
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        convert::TryFrom,
        num::{NonZeroU32, NonZeroU64, NonZeroUsize},
        sync::Arc,
        time::{Duration, Instant},
    };

    use arrow_util::assert_batches_eq;
    use chrono::{DateTime, Utc};
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
    use query::{exec::ExecutionContextProvider, frontend::sql::SqlQueryPlanner};
    use test_helpers::{assert_contains, assert_not_contains, tracing::TracingCapture};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    use crate::utils::TestDb;
    use crate::write_buffer::WriteBufferConsumer;

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

        /// Skip replay and seek to high watermark instead.
        SkipReplay,

        /// Persist partitions.
        ///
        /// The partitions are by table name and partition key.
        Persist(Vec<(&'static str, &'static str)>),

        /// Drop partitions.
        ///
        /// Note that this only works for fully persisted partitions if
        /// the database is configured for persistence.
        ///
        /// The partitions are by table name and partition key.
        Drop(Vec<(&'static str, &'static str)>),

        /// Advance clock far enough that all ingested entries become persistable.
        MakeWritesPersistable,

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
        ///
        /// # Default
        /// 1
        n_sequencers: NonZeroU32,

        /// How often a catalog checkpoint should be created during persistence.
        ///
        /// # Default
        /// `u64::MAX` aka "never"
        catalog_transactions_until_checkpoint: NonZeroU64,

        /// What to do in which order.
        ///
        /// # Default
        /// No steps.
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

            let registry = metric::Registry::new();
            let write_buffer_state =
                MockBufferSharedState::empty_with_n_sequencers(self.n_sequencers);

            let (mut test_db, mut shutdown, mut join_handle) = Self::create_test_db(
                Arc::clone(&object_store),
                server_id,
                db_name,
                partition_template.clone(),
                self.catalog_transactions_until_checkpoint,
                Utc::now(),
            )
            .await;

            let mut maybe_consumer = Some(WriteBufferConsumer::new(
                Box::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap()),
                Arc::clone(&test_db.db),
                &registry,
            ));

            // This is used to carry the write buffer from replay to await
            let mut maybe_write_buffer = None;

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
                        if let Some(consumer) = maybe_consumer.take() {
                            consumer.shutdown();
                            consumer.join().await.unwrap();
                        }

                        // stop background worker
                        shutdown.cancel();
                        join_handle.await.unwrap();

                        // remember time
                        let now = test_db.db.now_override.lock().unwrap();

                        // drop old DB
                        drop(test_db);

                        // then create new one
                        let (test_db_tmp, shutdown_tmp, join_handle_tmp) = Self::create_test_db(
                            Arc::clone(&object_store),
                            server_id,
                            db_name,
                            partition_template.clone(),
                            self.catalog_transactions_until_checkpoint,
                            now,
                        )
                        .await;
                        test_db = test_db_tmp;
                        shutdown = shutdown_tmp;
                        join_handle = join_handle_tmp;
                    }
                    Step::Replay | Step::SkipReplay => {
                        assert!(maybe_consumer.is_none());
                        assert!(maybe_write_buffer.is_none());

                        let replay_plan = match action_or_check {
                            Step::Replay => Some(&test_db.replay_plan),
                            Step::SkipReplay => None,
                            _ => unreachable!(),
                        };

                        let mut write_buffer =
                            MockBufferForReading::new(write_buffer_state.clone(), None).unwrap();

                        test_db
                            .db
                            .perform_replay(replay_plan, &mut write_buffer)
                            .await
                            .unwrap();

                        maybe_write_buffer = Some(write_buffer);
                    }
                    Step::Persist(partitions) => {
                        let db = &test_db.db;

                        for (table_name, partition_key) in partitions {
                            println!("Persist {}:{}", table_name, partition_key);
                            loop {
                                match db.persist_partition(table_name, partition_key, false).await {
                                    Ok(_) => break,
                                    Err(crate::db::Error::CannotFlushPartition { .. }) => {
                                        // cannot persist right now because of some lifecycle action, so wait a bit
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                        continue;
                                    }
                                    e => {
                                        e.unwrap();
                                        unreachable!();
                                    }
                                }
                            }
                        }
                    }
                    Step::Drop(partitions) => {
                        let db = &test_db.db;

                        for (table_name, partition_key) in partitions {
                            println!("Drop {}:{}", table_name, partition_key);
                            loop {
                                match db.drop_partition(table_name, partition_key).await {
                                    Ok(_) => break,
                                    Err(crate::db::Error::LifecycleError { .. }) => {
                                        // cannot drop right now because of some lifecycle action, so wait a bit
                                        tokio::time::sleep(Duration::from_millis(100)).await;
                                        continue;
                                    }
                                    e => {
                                        e.unwrap();
                                        unreachable!();
                                    }
                                }
                            }
                        }
                    }
                    Step::MakeWritesPersistable => {
                        let mut guard = test_db.db.now_override.lock();
                        *guard = Some(guard.unwrap() + chrono::Duration::seconds(60));
                    }
                    Step::Assert(checks) => {
                        Self::eval_checks(&checks, true, &test_db).await;
                    }
                    Step::Await(checks) => {
                        if maybe_consumer.is_none() {
                            let write_buffer = match maybe_write_buffer.take() {
                                Some(write_buffer) => write_buffer,
                                None => MockBufferForReading::new(write_buffer_state.clone(), None)
                                    .unwrap(),
                            };
                            maybe_consumer = Some(WriteBufferConsumer::new(
                                Box::new(write_buffer),
                                Arc::clone(&test_db.db),
                                &registry,
                            ));
                        }

                        let db = &test_db.db;
                        db.unsuppress_persistence().await;

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
            server_id: ServerId,
            db_name: &'static str,
            partition_template: PartitionTemplate,
            catalog_transactions_until_checkpoint: NonZeroU64,
            now: DateTime<Utc>,
        ) -> (TestDb, CancellationToken, JoinHandle<()>) {
            let test_db = TestDb::builder()
                .object_store(object_store)
                .server_id(server_id)
                .lifecycle_rules(data_types::database_rules::LifecycleRules {
                    buffer_size_hard: Some(NonZeroUsize::new(12_000).unwrap()),
                    late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                    catalog_transactions_until_checkpoint,
                    mub_row_threshold: NonZeroUsize::new(10).unwrap(),
                    ..Default::default()
                })
                .partition_template(partition_template)
                .db_name(db_name)
                .build()
                .await;

            // Mock time
            *test_db.db.now_override.lock() = Some(now);

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

                        if use_assert {
                            assert_eq!(&partitions_actual, partitions);
                            true
                        } else {
                            &partitions_actual == partitions
                        }
                    }
                    Check::Query(query, expected) => {
                        let planner = SqlQueryPlanner::default();
                        let ctx = db.new_query_context(None);

                        let physical_plan = match planner.query(query, &ctx).await {
                            Ok(physical_plan) => physical_plan,
                            err if use_assert => {
                                err.unwrap();
                                unreachable!()
                            }
                            _ => {
                                return false;
                            }
                        };

                        let batches = match ctx.collect(physical_plan).await {
                            Ok(batches) => batches,
                            err if use_assert => {
                                err.unwrap();
                                unreachable!()
                            }
                            _ => {
                                return false;
                            }
                        };

                        if use_assert {
                            assert_batches_eq!(expected, &batches);
                            true
                        } else {
                            let formatted =
                                arrow_util::display::pretty_format_batches(&batches).unwrap();
                            let actual_lines = formatted.trim().split('\n').collect::<Vec<_>>();

                            expected == &actual_lines
                        }
                    }
                };

                if !res {
                    return false;
                }
            }

            true
        }
    }

    impl Default for ReplayTest {
        fn default() -> Self {
            Self {
                n_sequencers: NonZeroU32::try_from(1).unwrap(),
                catalog_transactions_until_checkpoint: NonZeroU64::new(u64::MAX).unwrap(),
                steps: vec![],
            }
        }
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: ")]
    async fn test_framework_fail_assert_partitions() {
        // Test the test framework: checking the partitions should actually fail
        ReplayTest {
            steps: vec![
                // that passes
                Step::Assert(vec![Check::Partitions(vec![])]),
                // that fails
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
            ],
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed: ")]
    async fn test_framework_fail_assert_query() {
        // Test the test framework: checking the query results should actually fail
        ReplayTest {
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
            ..Default::default()
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
                Step::MakeWritesPersistable,
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
            ..Default::default()
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
                Step::MakeWritesPersistable,
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
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_nothing_to_replay() {
        ReplayTest {
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
            ..Default::default()
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
            n_sequencers: NonZeroU32::try_from(3).unwrap(),
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
                Step::MakeWritesPersistable,
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
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_ok_interleaved_writes() {
        ReplayTest {
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
                Step::MakeWritesPersistable,
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
                Step::MakeWritesPersistable,
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
                Step::MakeWritesPersistable,
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
                Step::MakeWritesPersistable,
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
            ..Default::default()
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
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_2", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![(
                    "table_2",
                    "tag_partition_by_a",
                )])]),
                Step::Replay,
            ],
            ..Default::default()
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
    async fn replay_prune_full_partition() {
        // there the following entries:
        //
        // 0. table 2, partition a:
        //    only used to "mark" sequence number 0 as "to be replayed"
        //
        // 1. table 1, partition a:
        //    will be fully persisted
        ReplayTest {
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_2,tag_partition_by=a bar=20 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::Assert(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                ])]),
                Step::Assert(vec![
                    // single chunk for table_1 was restored
                    Check::Query(
                        "select table_name, storage, count(*) as n from system.chunks group by table_name, storage order by table_name, storage",
                        vec![
                            "+------------+-----------------+---+",
                            "| table_name | storage         | n |",
                            "+------------+-----------------+---+",
                            "| table_1    | ObjectStoreOnly | 1 |",
                            "+------------+-----------------+---+",
                        ],
                    ),
                ]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    Check::Query(
                        "select * from table_2 order by bar",
                        vec![
                            "+-----+------------------+----------------------+",
                            "| bar | tag_partition_by | time                 |",
                            "+-----+------------------+----------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00Z |",
                            "+-----+------------------+----------------------+",
                        ],
                    ),
                    // no additional chunk for table_1 was created
                    Check::Query(
                        "select table_name, storage, count(*) as n from system.chunks group by table_name, storage order by table_name, storage",
                        vec![
                            "+------------+-------------------+---+",
                            "| table_name | storage           | n |",
                            "+------------+-------------------+---+",
                            "| table_1    | ObjectStoreOnly   | 1 |",
                            "| table_2    | OpenMutableBuffer | 1 |",
                            "+------------+-------------------+---+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await
    }

    #[tokio::test]
    async fn replay_prune_some_sequences_partition() {
        // there the following entries:
        //
        // 0. table 2, partition a:
        //    only used to "mark" sequence number 0 as "to be replayed"
        //
        // 1. table 1, partition a:
        //    will be persisted
        //
        // 2. table 1, partition a:
        //    will not be persisted
        //
        // 3. table 3, partition a:
        //    persisted, used to "mark" the end of the replay range to sequence number 3
        ReplayTest {
            steps: vec![
                // let's ingest some data for two partitions a and b
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_2,tag_partition_by=a bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=a bar=10 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                ])]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=a bar=20 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 3,
                        lp: "table_3,tag_partition_by=a bar=10 10",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_2", "tag_partition_by_a"),
                    ("table_3", "tag_partition_by_a"),
                ])]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_3", "tag_partition_by_a")]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00Z           |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    // chunks do not overlap
                    Check::Query(
                        "select table_name, storage, min_value, max_value, row_count from system.chunk_columns where column_name = 'time' order by table_name, storage",
                        vec![
                            "+------------+-------------------+-----------+-----------+-----------+",
                            "| table_name | storage           | min_value | max_value | row_count |",
                            "+------------+-------------------+-----------+-----------+-----------+",
                            "| table_1    | ObjectStoreOnly   | 0         | 0         | 1         |",
                            "| table_1    | OpenMutableBuffer | 10        | 10        | 1         |",
                            "| table_2    | OpenMutableBuffer | 0         | 0         | 1         |",
                            "| table_3    | ObjectStoreOnly   | 10        | 10        | 1         |",
                            "+------------+-------------------+-----------+-----------+-----------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await
    }

    #[tokio::test]
    async fn replay_prune_rows() {
        ReplayTest {
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a,tag=1 bar=10 10",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        // same time as first entry in that partition but different tag + some later data
                        lp: "table_1,tag_partition_by=a,tag=2 bar=20 10\ntable_1,tag_partition_by=a,tag=3 bar=30 30",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | 2   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 30  | 3   | a                | 1970-01-01T00:00:00.000000030Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Assert(vec![
                    // chunks do not overlap
                    Check::Query(
                        "select storage, min_value, max_value, row_count from system.chunk_columns where column_name = 'time' order by min_value, storage",
                        vec![
                            "+--------------------------+-----------+-----------+-----------+",
                            "| storage                  | min_value | max_value | row_count |",
                            "+--------------------------+-----------+-----------+-----------+",
                            "| ReadBufferAndObjectStore | 10        | 10        | 2         |",
                            "| ReadBuffer               | 30        | 30        | 1         |",
                            "+--------------------------+-----------+-----------+-----------+",
                        ],
                    ),
                ]),
                Step::Restart,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | 2   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | 2   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 30  | 3   | a                | 1970-01-01T00:00:00.000000030Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                    // chunks do not overlap
                    Check::Query(
                        "select storage, min_value, max_value, row_count from system.chunk_columns where column_name = 'time' order by min_value, storage",
                        vec![
                            "+-------------------+-----------+-----------+-----------+",
                            "| storage           | min_value | max_value | row_count |",
                            "+-------------------+-----------+-----------+-----------+",
                            "| ObjectStoreOnly   | 10        | 10        | 2         |",
                            "| OpenMutableBuffer | 30        | 30        | 1         |",
                            "+-------------------+-----------+-----------+-----------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await
    }

    #[tokio::test]
    async fn replay_works_with_checkpoints_all_full_persisted_1() {
        ReplayTest {
            catalog_transactions_until_checkpoint: NonZeroU64::new(2).unwrap(),
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=b bar=10 0",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=a bar=20 0",
                    },
                ]),
                Step::Await(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                ])]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                ]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![Check::Partitions(vec![
                    ("table_1", "tag_partition_by_a"),
                    ("table_1", "tag_partition_by_b"),
                ])]),
            ],
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_works_with_checkpoints_all_full_persisted_2() {
        // try to provoke an catalog checkpoints that lists database checkpoints in the wrong order
        ReplayTest {
            catalog_transactions_until_checkpoint: NonZeroU64::new(2).unwrap(),
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=b bar=10 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=a bar=20 20",
                    },
                ]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 20  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                // persist partition B
                Step::Persist(vec![
                    ("table_1", "tag_partition_by_b"),
                ]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 3,
                        lp: "table_1,tag_partition_by=b bar=30 30",
                    },
                ]),
                // persist partition A
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 30  |", "+-----+"],
                )]),
                Step::Persist(vec![
                    ("table_1", "tag_partition_by_a"),
                ]),
                // Here we have a catalog checkpoint that orders parquet files by file name, so partition A comes before
                // B. That is the flipped storage order (see persist actions above). If the upcoming replay would only
                // consider the last of the two database checkpoints (as presented by the catalog), it would forget
                // that:
                // 1. sequence number 3 was seen and added to partition B
                // 2. that partition A was fully persisted
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "| 30  | b                | 1970-01-01T00:00:00.000000030Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    // chunks do not overlap
                    Check::Query(
                        "select partition_key, min_value, max_value, row_count from system.chunk_columns where column_name = 'time' order by partition_key, min_value",
                        vec![
                            "+--------------------+-----------+-----------+-----------+",
                            "| partition_key      | min_value | max_value | row_count |",
                            "+--------------------+-----------+-----------+-----------+",
                            "| tag_partition_by_a | 20        | 20        | 1         |",
                            "| tag_partition_by_b | 10        | 10        | 1         |",
                            "| tag_partition_by_b | 30        | 30        | 1         |",
                            "+--------------------+-----------+-----------+-----------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_works_partially_persisted_1() {
        // regression test for https://github.com/influxdata/influxdb_iox/issues/2185
        let tracing_capture = TracingCapture::new();

        ReplayTest {
            steps: vec![
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 1,
                    lp: "table_1,tag_partition_by=a,tag=1 bar=10 10",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 10  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    // same time as first entry
                    lp: "table_1,tag_partition_by=a,tag=2 bar=20 10",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 20  |", "+-----+"],
                )]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 3,
                    lp: "table_1,tag_partition_by=b,tag=3 bar=30 30",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 30  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_b")]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | 2   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 30  | 3   | b                | 1970-01-01T00:00:00.000000030Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await;

        assert_not_contains!(
            tracing_capture.to_string(),
            "What happened to these sequence numbers?"
        );
    }

    #[tokio::test]
    async fn replay_works_partially_persisted_2() {
        // regression test for https://github.com/influxdata/influxdb_iox/issues/2185
        let tracing_capture = TracingCapture::new();

        ReplayTest {
            steps: vec![
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 1,
                    lp: "table_1,tag_partition_by=a,tag=1 bar=10 10",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 10  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    // same time as first entry
                    lp: "table_1,tag_partition_by=a,tag=2 bar=20 10",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 20  |", "+-----+"],
                )]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 3,
                    lp: "table_1,tag_partition_by=b,tag=3 bar=30 30",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 30  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 4,
                    lp: "table_1,tag_partition_by=b,tag=4 bar=40 40",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 40  |", "+-----+"],
                )]),
                Step::Persist(vec![("table_1", "tag_partition_by_b")]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+-----+------------------+--------------------------------+",
                            "| bar | tag | tag_partition_by | time                           |",
                            "+-----+-----+------------------+--------------------------------+",
                            "| 10  | 1   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | 2   | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 30  | 3   | b                | 1970-01-01T00:00:00.000000030Z |",
                            "| 40  | 4   | b                | 1970-01-01T00:00:00.000000040Z |",
                            "+-----+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await;

        assert_not_contains!(
            tracing_capture.to_string(),
            "What happened to these sequence numbers?"
        );
    }

    #[tokio::test]
    async fn replay_works_after_skip() {
        let tracing_capture = TracingCapture::new();

        ReplayTest {
            steps: vec![
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 1,
                    lp: "table_1,tag_partition_by=a bar=10 10",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 10  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    lp: "table_1,tag_partition_by=a bar=20 20",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 20  |", "+-----+"],
                )]),
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::SkipReplay,
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 3,
                    lp: "table_1,tag_partition_by=b bar=30 30",
                }]),
                Step::Await(vec![Check::Query(
                    "select max(bar) as bar from table_1",
                    vec!["+-----+", "| bar |", "+-----+", "| 30  |", "+-----+"],
                )]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_b")]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 30  | b                | 1970-01-01T00:00:00.000000030Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await;

        assert_contains!(
            tracing_capture.to_string(),
            "What happened to these sequence numbers?"
        );
    }

    #[tokio::test]
    async fn replay_initializes_max_seen_sequence_numbers() {
        // Ensures that either replay or the catalog loading initializes the maximum seen sequence numbers (per
        // partition) correctly. Before this test (and its fix), sequence numbers were only written if there was any
        // unpersisted range during replay.
        //
        // This is a regression test for https://github.com/influxdata/influxdb_iox/issues/2215
        ReplayTest {
            n_sequencers: NonZeroU32::try_from(2).unwrap(),
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=b bar=10 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=a bar=20 20",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 1,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=30 30",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=b bar=40 40",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "| 30  | a                | 1970-01-01T00:00:00.000000030Z |",
                            "| 40  | b                | 1970-01-01T00:00:00.000000040Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Assert(vec![
                    // there should be two persisted chunks for partition a
                    Check::Query(
                        "select storage, count(*) as n from system.chunks where partition_key = 'tag_partition_by_a' group by storage order by storage",
                        vec![
                            "+--------------------------+---+",
                            "| storage                  | n |",
                            "+--------------------------+---+",
                            "| ObjectStoreOnly          | 1 |",
                            "| ReadBufferAndObjectStore | 1 |",
                            "+--------------------------+---+",
                        ],
                    ),
                ]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "| 30  | a                | 1970-01-01T00:00:00.000000030Z |",
                            "| 40  | b                | 1970-01-01T00:00:00.000000040Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    // no additional chunk for partition a was created
                    Check::Query(
                        "select storage, count(*) as n from system.chunks where partition_key = 'tag_partition_by_a' group by storage order by storage",
                        vec![
                            "+-----------------+---+",
                            "| storage         | n |",
                            "+-----------------+---+",
                            "| ObjectStoreOnly | 2 |",
                            "+-----------------+---+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await
    }

    #[tokio::test]
    async fn skip_replay_initializes_max_seen_sequence_numbers() {
        // Similar case to `replay_initializes_max_seen_sequence_numbers` but instead of replaying, we skip replay to
        // provoke a similar outcome.
        //
        // This is a regression test for https://github.com/influxdata/influxdb_iox/issues/2215
        ReplayTest {
            n_sequencers: NonZeroU32::try_from(2).unwrap(),
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=b bar=10 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=a bar=20 20",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | b                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Restart,
                Step::SkipReplay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 1,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=30 30",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 2,
                        lp: "table_1,tag_partition_by=b bar=40 40",
                    },
                ]),
                Step::Await(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "| 30  | a                | 1970-01-01T00:00:00.000000030Z |",
                            "| 40  | b                | 1970-01-01T00:00:00.000000040Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_a")]),
                Step::Assert(vec![
                    // there should be two persisted chunks for partition a
                    Check::Query(
                        "select storage, count(*) as n from system.chunks where partition_key = 'tag_partition_by_a' group by storage order by storage",
                        vec![
                            "+--------------------------+---+",
                            "| storage                  | n |",
                            "+--------------------------+---+",
                            "| ObjectStoreOnly          | 1 |",
                            "| ReadBufferAndObjectStore | 1 |",
                            "+--------------------------+---+",
                        ],
                    ),
                ]),
                Step::Restart,
                Step::Replay,
                Step::Assert(vec![
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 20  | a                | 1970-01-01T00:00:00.000000020Z |",
                            "| 30  | a                | 1970-01-01T00:00:00.000000030Z |",
                            "| 40  | b                | 1970-01-01T00:00:00.000000040Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                    // no additional chunk for partition a was created
                    Check::Query(
                        "select storage, count(*) as n from system.chunks where partition_key = 'tag_partition_by_a' group by storage order by storage",
                        vec![
                            "+-----------------+---+",
                            "| storage         | n |",
                            "+-----------------+---+",
                            "| ObjectStoreOnly | 2 |",
                            "+-----------------+---+",
                        ],
                    ),
                ]),
            ],
            ..Default::default()
        }
        .run()
        .await
    }

    #[tokio::test]
    async fn replay_after_drop() {
        ReplayTest {
            steps: vec![
                Step::Ingest(vec![
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 0,
                        lp: "table_1,tag_partition_by=a bar=10 10",
                    },
                    TestSequencedEntry {
                        sequencer_id: 0,
                        sequence_number: 1,
                        lp: "table_1,tag_partition_by=b bar=20 20",
                    },
                ]),
                Step::Await(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "| 20  | b                | 1970-01-01T00:00:00.000000020Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::MakeWritesPersistable,
                Step::Persist(vec![("table_1", "tag_partition_by_b")]),
                Step::Drop(vec![("table_1", "tag_partition_by_b")]),
                Step::Assert(vec![
                    // note that at the moment the dropped partition still exists but has no chunks
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                Step::Restart,
                Step::Assert(vec![
                    // partition also exists after restart but still has no data
                    Check::Partitions(vec![("table_1", "tag_partition_by_b")]),
                    // Note that currently this edge case also leads to "no columns" because the query executor does not
                    // return any record batches.
                    Check::Query("select * from table_1 order by bar", vec!["++", "++"]),
                ]),
                Step::Ingest(vec![TestSequencedEntry {
                    sequencer_id: 0,
                    sequence_number: 2,
                    lp: "table_1,tag_partition_by=b bar=30 30",
                }]),
                Step::Replay,
                Step::Assert(vec![
                    Check::Partitions(vec![
                        ("table_1", "tag_partition_by_a"),
                        ("table_1", "tag_partition_by_b"),
                    ]),
                    Check::Query(
                        "select * from table_1 order by bar",
                        vec![
                            "+-----+------------------+--------------------------------+",
                            "| bar | tag_partition_by | time                           |",
                            "+-----+------------------+--------------------------------+",
                            "| 10  | a                | 1970-01-01T00:00:00.000000010Z |",
                            "+-----+------------------+--------------------------------+",
                        ],
                    ),
                ]),
                // new data can still be ingested into the dropped partition
                Step::Await(vec![Check::Query(
                    "select * from table_1 order by bar",
                    vec![
                        "+-----+------------------+--------------------------------+",
                        "| bar | tag_partition_by | time                           |",
                        "+-----+------------------+--------------------------------+",
                        "| 10  | a                | 1970-01-01T00:00:00.000000010Z |",
                        "| 30  | b                | 1970-01-01T00:00:00.000000030Z |",
                        "+-----+------------------+--------------------------------+",
                    ],
                )]),
            ],
            ..Default::default()
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn replay_fail_sequencers_change() {
        // create write buffer w/ sequencer 0 and 1
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(2).unwrap());
        let mut write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        // create DB
        let db = TestDb::builder().build().await.db;

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
        replay_planner
            .register_checkpoints(&partition_checkpoint, &database_checkpoint)
            .unwrap();
        let replay_plan = replay_planner.build().unwrap();

        // replay fails
        let res = db
            .perform_replay(Some(&replay_plan), &mut write_buffer)
            .await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Replay plan references unknown sequencer"
        );
    }

    #[tokio::test]
    async fn replay_fail_lost_entry() {
        // create write buffer state with sequence number 0 and 2, 1 is missing
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
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
        let mut write_buffer = MockBufferForReading::new(write_buffer_state, None).unwrap();

        // create DB
        let db = TestDb::builder().build().await.db;

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
        replay_planner
            .register_checkpoints(&partition_checkpoint, &database_checkpoint)
            .unwrap();
        let replay_plan = replay_planner.build().unwrap();

        // replay fails
        let res = db
            .perform_replay(Some(&replay_plan), &mut write_buffer)
            .await;
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
        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(3).unwrap());
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
        let mut write_buffer = MockBufferForReading::new(write_buffer_state.clone(), None).unwrap();

        // create DB
        let test_db = TestDb::builder().build().await;
        let db = &test_db.db;

        // seek
        db.perform_replay(None, &mut write_buffer).await.unwrap();

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

        let consumer =
            WriteBufferConsumer::new(Box::new(write_buffer), Arc::clone(db), &Default::default());

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

        consumer.shutdown();
        consumer.join().await.unwrap();
    }

    #[test]
    fn sequence_number_section() {
        let min_max = OptionalMinMaxSequence::new(None, 0);
        assert_eq!(
            SequenceNumberSection::compare(0, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(1, min_max),
            SequenceNumberSection::Unpersisted
        );

        let min_max = OptionalMinMaxSequence::new(None, 1);
        assert_eq!(
            SequenceNumberSection::compare(0, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(1, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(2, min_max),
            SequenceNumberSection::Unpersisted
        );

        let min_max = OptionalMinMaxSequence::new(Some(0), 0);
        assert_eq!(
            SequenceNumberSection::compare(0, min_max),
            SequenceNumberSection::PartiallyPersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(1, min_max),
            SequenceNumberSection::Unpersisted
        );

        let min_max = OptionalMinMaxSequence::new(Some(1), 1);
        assert_eq!(
            SequenceNumberSection::compare(0, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(1, min_max),
            SequenceNumberSection::PartiallyPersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(2, min_max),
            SequenceNumberSection::Unpersisted
        );

        let min_max = OptionalMinMaxSequence::new(Some(2), 4);
        assert_eq!(
            SequenceNumberSection::compare(0, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(1, min_max),
            SequenceNumberSection::Persisted
        );
        assert_eq!(
            SequenceNumberSection::compare(2, min_max),
            SequenceNumberSection::PartiallyPersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(3, min_max),
            SequenceNumberSection::PartiallyPersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(4, min_max),
            SequenceNumberSection::PartiallyPersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(5, min_max),
            SequenceNumberSection::Unpersisted
        );
        assert_eq!(
            SequenceNumberSection::compare(6, min_max),
            SequenceNumberSection::Unpersisted
        );
    }
}
