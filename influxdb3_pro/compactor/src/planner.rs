//! Planner contains logic for organizing compaction within a table and creating compaction plans.

use hashbrown::HashMap;
use influxdb3_pro_data_layout::{
    CompactedData, CompactionConfig, Gen1, Generation, GenerationId, GenerationLevel,
    HostSnapshotMarker,
};
use influxdb3_write::PersistedSnapshot;
use object_store::ObjectStore;
use observability_deps::tracing::warn;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("host {} is not getting tracked", .0)]
    NotTrackingHost(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The `SnapshotTracker` keeps the parquet files and snapshot markers for each host. Once
/// there are enough snapshots to warrant a compaction run, all parquet files must be
/// organized into compactions. Once all those compactions are complete, the
/// `CompactionSummary` can be updated with markers of what snapshot sequence each host is up to.
#[derive(Debug)]
pub(crate) struct SnapshotTracker {
    state: Arc<Mutex<TrackerState>>,
}

#[derive(Debug)]
struct TrackerState {
    /// Map of host to snapshot marker and snapshot count
    host_snapshot_markers: HashMap<String, HostSnapshotCounter>,
    /// Map of database name to table name to gen1 files
    gen1_files: DatabaseToTables,
}

impl TrackerState {
    fn reset(&mut self) -> (HashMap<String, HostSnapshotCounter>, DatabaseToTables) {
        let reset_markers = self
            .host_snapshot_markers
            .keys()
            .cloned()
            .map(|host| (host, HostSnapshotCounter::default()))
            .collect();

        let host_snapshot_markers =
            std::mem::replace(&mut self.host_snapshot_markers, reset_markers);

        let mut gen1_files = HashMap::new();
        std::mem::swap(&mut self.gen1_files, &mut gen1_files);

        (host_snapshot_markers, gen1_files)
    }
}

type DatabaseToTables = HashMap<Arc<str>, HashMap<Arc<str>, Vec<Arc<dyn Generation>>>>;

#[derive(Debug, Default)]
pub(crate) struct HostSnapshotCounter {
    pub marker: Option<HostSnapshotMarker>,
    pub snapshot_count: usize,
}

impl SnapshotTracker {
    /// Create a new tracker with all of the hosts that will be getting compacted together
    pub(crate) fn new(hosts: Vec<String>) -> Self {
        let host_snapshot_markers = hosts
            .into_iter()
            .map(|host| (host, HostSnapshotCounter::default()))
            .collect();
        Self {
            state: Arc::new(Mutex::new(TrackerState {
                host_snapshot_markers,
                gen1_files: HashMap::new(),
            })),
        }
    }

    pub(crate) fn hosts(&self) -> Vec<String> {
        self.state
            .lock()
            .host_snapshot_markers
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn add_snapshot(&self, snapshot: &PersistedSnapshot) -> Result<()> {
        let mut state = self.state.lock();

        // set the snapshot marker for the host
        let counter = state
            .host_snapshot_markers
            .get_mut(&snapshot.host_id)
            .ok_or_else(|| Error::NotTrackingHost(snapshot.host_id.clone()))?;
        counter.snapshot_count += 1;
        if let Some(marker) = counter.marker.as_mut() {
            marker.snapshot_sequence_number = marker
                .snapshot_sequence_number
                .max(snapshot.snapshot_sequence_number);
        } else {
            counter.marker = Some(HostSnapshotMarker {
                host_id: snapshot.host_id.clone(),
                snapshot_sequence_number: snapshot.snapshot_sequence_number,
            });
        }

        // add the parquet files to the gen1_files map
        for (db, tables) in &snapshot.databases {
            for (table, gen1_files) in &tables.tables {
                let files = state
                    .gen1_files
                    .entry(Arc::clone(db))
                    .or_default()
                    .entry(Arc::clone(table))
                    .or_default();
                let mut gen1_files: Vec<Arc<dyn Generation>> = gen1_files
                    .iter()
                    .map(|f| Arc::new(Gen1::new(Arc::new(f.clone()))) as _)
                    .collect();
                files.append(&mut gen1_files);
            }
        }

        Ok(())
    }

    /// We only want to run compactions when we have at least 2 snapshots for every host. However,
    /// if we have 3 snapshots from any host, we should run a compaction to advance things.
    pub(crate) fn should_compact(&self) -> bool {
        let state = self.state.lock();

        // if we have any host with 3 snapshots, we should compact
        let must_compact = state
            .host_snapshot_markers
            .values()
            .any(|marker| marker.snapshot_count >= 3);
        if must_compact {
            warn!("Compacting because at least one host has 3 snapshots");
            return true;
        }

        // otherwise, we should compact if we have at least 2 snapshots for every host
        state
            .host_snapshot_markers
            .values()
            .all(|marker| marker.snapshot_count >= 2)
    }

    /// Generate compaction plans based on the tracker and the existing compacted state. Once
    /// all of these plans have been run and the resulting compaction detail files have been written,
    /// we can write a compaction summary that contains all the details.
    pub(crate) fn to_plan_and_reset(
        &self,
        compaction_config: &CompactionConfig,
        compacted_data: &CompactedData,
        object_store: Arc<dyn ObjectStore>,
    ) -> SnapshotAdvancePlan {
        let mut state = self.state.lock();

        let (host_snapshot_markers, gen1_files) = state.reset();

        let mut compaction_plans = HashMap::new();
        let mut generation_map = HashMap::new();

        for (db, tables) in gen1_files {
            let table_plans: &mut Vec<CompactionPlan> =
                compaction_plans.entry(Arc::clone(&db)).or_default();

            for (table, mut gen1_files) in tables {
                // if this table has been compacted before, get its generations
                let generations = compacted_data.get_generations(
                    db.as_ref(),
                    table.as_ref(),
                    Arc::clone(&object_store),
                );
                if let Some(mut generations) = generations {
                    gen1_files.append(&mut generations);
                }

                let plan = create_gen1_plan(
                    compaction_config,
                    Arc::clone(&db),
                    Arc::clone(&table),
                    &gen1_files,
                );
                table_plans.push(plan);
                for g in gen1_files {
                    generation_map.insert(g.id(), g);
                }
            }
        }

        SnapshotAdvancePlan {
            host_snapshot_markers,
            compaction_plans,
            generations: generation_map,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct SnapshotAdvancePlan {
    /// Map of host to snapshot marker and snapshot count
    pub(crate) host_snapshot_markers: HashMap<String, HostSnapshotCounter>,
    /// The compaction plans that must be run to advance the snapshot summary beyond these snapshots
    pub(crate) compaction_plans: HashMap<Arc<str>, Vec<CompactionPlan>>,
    /// Map of `GenerationId` to the `Generation`
    pub(crate) generations: HashMap<GenerationId, Arc<dyn Generation>>,
}

/// Creates a plan to do a gen1 compaction on the newest gen1 files. If no gen1 compaction is
/// needed, it returns the leftover gen1 files if any exist (either because there are historical
/// backfill that will require a later generation compaction or there aren't enough gen1 files to
/// compact yet). These will have to be tracked in the `CompactionDetail` for the table.
fn create_gen1_plan(
    compaction_config: &CompactionConfig,
    db_name: Arc<str>,
    table_name: Arc<str>,
    generations: &[Arc<dyn Generation>],
) -> CompactionPlan {
    // grab a slice of the leading gen1
    let leading_gen1 = generations
        .iter()
        .take_while(|g| g.level().is_under_two())
        .collect::<Vec<_>>();
    // if there are fewer than 2 gen1 files, we're not going to be compacting
    if leading_gen1.len() < 2 {
        let leftover_ids = generations
            .iter()
            .filter(|g| g.level().is_under_two())
            .map(|g| g.id())
            .collect::<Vec<_>>();
        return CompactionPlan::LeftoverOnly(LeftoverPlan {
            db_name,
            table_name,
            leftover_ids,
        });
    }

    let mut new_block_times_to_gens = BTreeMap::new();
    for gen in leading_gen1 {
        let level_start_time =
            compaction_config.generation_start_time(GenerationLevel::two(), gen.start_time_secs());
        let gens = new_block_times_to_gens
            .entry(level_start_time)
            .or_insert_with(Vec::new);
        gens.push(gen);
    }

    // build a plan to compact the newest generation group with at least 2
    for (gen_time, gens) in new_block_times_to_gens.into_iter().rev() {
        if gens.len() >= 2 {
            let mut input_ids = gens.iter().map(|g| g.id()).collect::<Vec<_>>();
            input_ids.sort();
            let mut leftover_ids = generations
                .iter()
                .filter(|g| g.level().is_under_two() && !input_ids.contains(&g.id()))
                .map(|g| g.id())
                .collect::<Vec<_>>();
            leftover_ids.sort();
            let compaction_plan = CompactionPlan::Compaction(NextCompactionPlan {
                db_name,
                table_name,
                output_level: GenerationLevel::two(),
                output_id: GenerationId::new(),
                output_gen_time: gen_time,
                input_ids,
                leftover_ids,
            });

            return compaction_plan;
        }
    }

    let leftover_ids = generations
        .iter()
        .filter(|g| g.level().is_under_two())
        .map(|g| g.id())
        .collect::<Vec<_>>();
    CompactionPlan::LeftoverOnly(LeftoverPlan {
        db_name,
        table_name,
        leftover_ids,
    })
}

/// Creates a compaction plan for whatever the next compaction is that should be run. If the
/// table is fully compacted, None will be returned.
#[allow(dead_code)]
fn create_next_gen_plan(
    _db_name: Arc<str>,
    _table_name: Arc<str>,
    _generations: &[Arc<dyn Generation>],
) -> Option<CompactionPlan> {
    None
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum CompactionPlan {
    LeftoverOnly(LeftoverPlan),
    Compaction(NextCompactionPlan),
}

impl CompactionPlan {
    #[allow(dead_code)]
    pub(crate) fn db_name(&self) -> &str {
        match self {
            Self::LeftoverOnly(plan) => &plan.db_name,
            Self::Compaction(plan) => &plan.db_name,
        }
    }

    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::LeftoverOnly(plan) => &plan.table_name,
            Self::Compaction(plan) => &plan.table_name,
        }
    }
}

/// This plan is what gets created when the only compaction to be done is with gen1 files
/// that overlap with older generations (3+) or there aren't enough gen1 files to compact into a larger gen2 generation. In that case, we'll want to just update the
/// `CompactionDetail` for the table with this information so that the historical compaction
/// can be run later. For now, we want to advance the snapshot trackers of the upstream gen1 hosts.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct LeftoverPlan {
    pub(crate) db_name: Arc<str>,
    pub(crate) table_name: Arc<str>,
    pub(crate) leftover_ids: Vec<GenerationId>,
}

/// When the planner gets called to plan a compaction on a table, this contains all the detail
/// to run whatever the next highest priority compaction is. The returned information from that
/// compaction combined with the leftover_ids will give us enough detail to write a new
/// `CompactionDetail` file for the table.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct NextCompactionPlan {
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
    pub output_level: GenerationLevel,
    pub output_id: GenerationId,
    pub output_gen_time: i64,
    /// The input generations from both gen1 and existing generations for this compaction
    pub input_ids: Vec<GenerationId>,
    /// The ids for the gen1 files that will be left over after this compaction plan runs
    pub leftover_ids: Vec<GenerationId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_pro_data_layout::{gen_time_string, gen_time_string_to_start_time_secs};
    use influxdb3_write::ParquetFile;

    #[test]
    fn gen1_plans() {
        let compaction_config = CompactionConfig::default();

        struct TestCase<'a> {
            // description of what the test case is for
            description: &'a str,
            // input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            // the expected output level of the compaction
            output_level: u8,
            // the expected output gen_time of the compaction
            output_time: &'a str,
            // the expected ids from the input that will be used for the compaction
            compact_ids: Vec<u64>,
            // any gen1 ids that would be leftover that have yet to land in a compacted generation
            leftover_ids: Vec<u64>,
        }

        let test_cases = vec![
            TestCase {
                description: "two gen1 into a gen2",
                input: vec![(1, 1, "2024-09-05/12-00"), (2, 1, "2024-09-05/12-10")],
                output_level: 2,
                output_time: "2024-09-05/12-00",
                compact_ids: vec![1, 2],
                leftover_ids: vec![],
            },
            TestCase {
                description: "one gen1 not ready with 2 older ready",
                input: vec![
                    (5, 1, "2024-09-10/11-40"),
                    (3, 1, "2024-09-10/11-30"),
                    (2, 1, "2024-09-10/11-20"),
                ],
                output_level: 2,
                output_time: "2024-09-10/11-20",
                compact_ids: vec![2, 3],
                leftover_ids: vec![5],
            },
            TestCase {
                description: "three leading gen1 and trailing 2 gen1s to be leftover",
                input: vec![
                    (5, 1, "2024-09-10/11-30"),
                    (3, 1, "2024-09-10/11-20"),
                    (2, 1, "2024-09-10/11-10"),
                    (4, 1, "2024-09-10/11-25"),
                    (1, 1, "2024-09-10/11-00"),
                ],
                output_level: 2,
                output_time: "2024-09-10/11-20",
                compact_ids: vec![3, 4, 5],
                leftover_ids: vec![1, 2],
            },
        ];

        for tc in test_cases {
            let gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| {
                    Arc::new(TestGen {
                        start_time: gen_time_string_to_start_time_secs(time).unwrap(),
                        id: GenerationId::from(*id),
                        level: GenerationLevel::new(*level),
                    }) as Arc<dyn Generation>
                })
                .collect();
            let plan = create_gen1_plan(&compaction_config, "db".into(), "table".into(), &gens);
            match plan {
                CompactionPlan::Compaction(NextCompactionPlan {
                    output_level,
                    output_gen_time,
                    input_ids,
                    leftover_ids,
                    ..
                }) => {
                    assert_eq!(
                        output_level,
                        GenerationLevel::new(tc.output_level),
                        "{}: expected level {} but got {}",
                        tc.description,
                        tc.output_level,
                        output_level
                    );
                    assert_eq!(
                        output_gen_time,
                        gen_time_string_to_start_time_secs(tc.output_time).unwrap(),
                        "{}: expected gen time {} but got {}",
                        tc.description,
                        tc.output_time,
                        gen_time_string(output_gen_time)
                    );
                    let ids_to_compact = input_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    assert_eq!(
                        tc.compact_ids, ids_to_compact,
                        "{}: expected ids {:?} but got {:?}",
                        tc.description, tc.compact_ids, ids_to_compact
                    );
                    let leftover_ids = leftover_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    assert_eq!(
                        tc.leftover_ids, leftover_ids,
                        "{}: expected leftover ids {:?} but got {:?}",
                        tc.description, tc.leftover_ids, leftover_ids
                    );
                }
                _ => panic!(
                    "expected a compaction plan for test case '{}'",
                    tc.description
                ),
            }
        }
    }

    #[test]
    fn gen1_leftover_plas() {
        let compaction_config = CompactionConfig::default();

        struct TestCase<'a> {
            // description of what the test case is for
            description: &'a str,
            // input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            // the expected leftover ids from the input
            leftover_ids: Vec<u64>,
        }

        let test_cases = vec![
            TestCase {
                description: "one gen1 leftover",
                input: vec![(23, 1, "2024-09-05/12-00")],
                leftover_ids: vec![23],
            },
            TestCase {
                description: "two gen1 leftovers in different gen2 blocks",
                input: vec![(23, 1, "2024-09-05/12-00"), (24, 1, "2024-09-05/12-40")],
                leftover_ids: vec![23, 24],
            },
        ];

        for tc in test_cases {
            let gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| {
                    Arc::new(TestGen {
                        start_time: gen_time_string_to_start_time_secs(time).unwrap(),
                        id: GenerationId::from(*id),
                        level: GenerationLevel::new(*level),
                    }) as Arc<dyn Generation>
                })
                .collect();
            let plan = create_gen1_plan(&compaction_config, "db".into(), "table".into(), &gens);
            match plan {
                CompactionPlan::LeftoverOnly(LeftoverPlan { leftover_ids, .. }) => {
                    let leftover_ids = leftover_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    assert_eq!(
                        tc.leftover_ids, leftover_ids,
                        "{}: expected leftover ids {:?} but got {:?}",
                        tc.description, tc.leftover_ids, leftover_ids
                    );
                }
                _ => panic!(
                    "expected a leftover compaction plan for test case '{}'",
                    tc.description
                ),
            }
        }
    }

    struct TestGen {
        start_time: i64,
        id: GenerationId,
        level: GenerationLevel,
    }

    #[async_trait::async_trait]
    impl Generation for TestGen {
        fn id(&self) -> GenerationId {
            self.id
        }

        fn start_time_secs(&self) -> i64 {
            self.start_time
        }

        fn gen_path(&self) -> String {
            "some_path".to_string()
        }

        fn level(&self) -> GenerationLevel {
            self.level
        }

        async fn files(&self) -> Vec<Arc<ParquetFile>> {
            todo!()
        }
    }
}
