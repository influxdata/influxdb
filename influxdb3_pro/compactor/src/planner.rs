//! Planner contains logic for organizing compaction within a table and creating compaction plans.

use hashbrown::{HashMap, HashSet};
use influxdb3_id::{DbId, TableId};
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::{
    CompactionConfig, Generation, GenerationId, GenerationLevel, HostSnapshotMarker,
};
use influxdb3_write::{ParquetFile, PersistedSnapshot};
use observability_deps::tracing::{debug, warn};
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

type DatabaseToTables = HashMap<DbId, HashMap<TableId, Vec<ParquetFile>>>;

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
            marker.next_file_id = marker.next_file_id.max(snapshot.next_file_id);
        } else {
            counter.marker = Some(HostSnapshotMarker {
                host_id: snapshot.host_id.clone(),
                snapshot_sequence_number: snapshot.snapshot_sequence_number,
                next_file_id: snapshot.next_file_id,
            });
        }

        // add the parquet files to the gen1_files map
        for (db_id, tables) in &snapshot.databases {
            for (table_id, gen1_files) in &tables.tables {
                let files = state
                    .gen1_files
                    .entry(*db_id)
                    .or_default()
                    .entry(*table_id)
                    .or_default();
                let mut gen1_files: Vec<_> = gen1_files.to_vec();
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
    pub(crate) fn to_plan_and_reset(&self, compacted_data: &CompactedData) -> SnapshotAdvancePlan {
        let mut state = self.state.lock();

        let (host_snapshot_markers, gen1_files) = state.reset();

        let mut compaction_plans = HashMap::new();

        for (db_id, tables) in gen1_files {
            let table_plans: &mut Vec<CompactionPlan> = compaction_plans.entry(db_id).or_default();

            for (table_id, gen1_files) in tables {
                // if this table has been compacted before, get its generations
                let mut generations = compacted_data.get_generations(db_id, table_id);

                // add the gen1 files to the compacted data structure
                let mut gen1 =
                    compacted_data.add_compacting_gen1_files(db_id, table_id, gen1_files);
                generations.append(&mut gen1);

                generations.sort();

                let plan = create_gen1_plan(
                    &compacted_data.compaction_config,
                    db_id,
                    table_id,
                    &generations,
                );
                debug!(plan = ?plan, "Plan");
                table_plans.push(plan);
            }
        }

        SnapshotAdvancePlan {
            host_snapshot_markers,
            compaction_plans,
        }
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotAdvancePlan {
    /// Map of host to snapshot marker and snapshot count
    pub(crate) host_snapshot_markers: HashMap<String, HostSnapshotCounter>,
    /// The compaction plans that must be run to advance the snapshot summary beyond these snapshots
    pub(crate) compaction_plans: HashMap<DbId, Vec<CompactionPlan>>,
}

/// This is a group of compaction plans, generally at the same level that can be run that will
/// be used to output a new summary when complete
#[derive(Debug)]
pub(crate) struct CompactionPlanGroup {
    /// The compaction plans that must be run to advance the compaction summary
    pub(crate) compaction_plans: Vec<NextCompactionPlan>,
}

impl CompactionPlanGroup {
    pub(crate) fn plans_for_level(
        compacted_data: &CompactedData,
        output_level: GenerationLevel,
    ) -> Option<Self> {
        let mut compaction_plans = Vec::new();
        for db_id in compacted_data.databases() {
            for table_id in compacted_data.tables(db_id) {
                let generations = compacted_data.get_generations(db_id, table_id);
                if let Some(plan) = create_next_plan(
                    &compacted_data.compaction_config,
                    db_id,
                    table_id,
                    &generations,
                    output_level,
                ) {
                    compaction_plans.push(plan);
                }
            }
        }

        if compaction_plans.is_empty() {
            None
        } else {
            Some(Self { compaction_plans })
        }
    }
}

/// Creates a plan to do a gen1 compaction on the newest gen1 files. If no gen1 compaction is
/// needed, it returns the leftover gen1 files if any exist (either because there are historical
/// backfill that will require a later generation compaction or there aren't enough gen1 files to
/// compact yet). These will have to be tracked in the `CompactionDetail` for the table.
fn create_gen1_plan(
    compaction_config: &CompactionConfig,
    db_id: DbId,
    table_id: TableId,
    generations: &[Generation],
) -> CompactionPlan {
    // grab a slice of the leading gen1
    let leading_gen1 = generations
        .iter()
        .take_while(|g| g.level.is_under_two())
        .collect::<Vec<_>>();
    // if there are fewer than 2 gen1 files, we're not going to be compacting
    if leading_gen1.len() < 2 {
        let leftover_gen1_ids = generations
            .iter()
            .filter(|g| g.level.is_under_two())
            .map(|g| g.id)
            .collect::<Vec<_>>();
        return CompactionPlan::LeftoverOnly(LeftoverPlan {
            db_id,
            table_id,
            leftover_gen1_ids,
        });
    }

    let mut new_block_times_to_gens = BTreeMap::new();
    for gen in leading_gen1 {
        let level_start_time =
            compaction_config.generation_start_time(GenerationLevel::two(), gen.start_time_secs);
        let gens = new_block_times_to_gens
            .entry(level_start_time)
            .or_insert_with(Vec::new);
        gens.push(gen);
    }

    let gen2_duration = compaction_config.generation_duration(GenerationLevel::two());

    // build a plan to compact the newest generation group with at least 2
    for (gen_time, gens) in new_block_times_to_gens.into_iter().rev() {
        // we only want to compact if we have at least 2 gen1 files of different times (i.e. we can create a longer block for gen2)
        let mut gen_times = HashSet::new();
        for gen in &gens {
            gen_times.insert(gen.start_time_secs);
        }

        if gens.len() >= 2 && gen_times.len() >= 2 {
            let mut input_ids = gens.iter().map(|g| g.id).collect::<Vec<_>>();
            input_ids.sort();
            let mut leftover_ids = generations
                .iter()
                .filter(|g| g.level.is_under_two() && !input_ids.contains(&g.id))
                .map(|g| g.id)
                .collect::<Vec<_>>();
            leftover_ids.sort();
            let compaction_plan = CompactionPlan::Compaction(NextCompactionPlan {
                db_id,
                table_id,
                output_generation: Generation {
                    id: GenerationId::new(),
                    level: GenerationLevel::two(),
                    start_time_secs: gen_time,
                    max_time: gen2_duration
                        .map(|d| (gen_time + d.as_secs() as i64) * 1_000_000_000)
                        .unwrap_or_else(|| gen_time * 1_000_000_000),
                },
                input_ids,
                leftover_ids,
            });

            return compaction_plan;
        }
    }

    let leftover_gen1_ids = generations
        .iter()
        .filter(|g| g.level.is_under_two())
        .map(|g| g.id)
        .collect::<Vec<_>>();
    CompactionPlan::LeftoverOnly(LeftoverPlan {
        db_id,
        table_id,
        leftover_gen1_ids,
    })
}

/// Given a list of generations, returns the next compaction plan of the passed in generation to
/// run. If there are no compactions to run at that level, it will return None.
pub(crate) fn create_next_plan(
    compaction_config: &CompactionConfig,
    db_id: DbId,
    table_id: TableId,
    generations: &[Generation],
    output_level: GenerationLevel,
) -> Option<NextCompactionPlan> {
    // first, group the generations together into what their start time would be at the
    // chosen output level. Only inlude generations that are less than the output level.
    let mut new_block_times_to_gens = BTreeMap::new();
    for gen in generations {
        // only include generations that are less than the desired output level
        if gen.level < output_level {
            let start_time =
                compaction_config.generation_start_time(output_level, gen.start_time_secs);
            let gens = new_block_times_to_gens
                .entry(start_time)
                .or_insert_with(Vec::new);
            gens.push(gen);
        }
    }

    // Loop through newest to oldest group.
    // For the output level, we want N generations of the previous level to compact into. The
    // generations don't have to strictly be of that previous level (they can be any number
    // below), but we want to make sure we have enough blocks of time to equal the output generation.
    // For example, if we have gen2 of 20m duration, and gen3 is 3x gen2. We will want to make sure
    // we have data for start times of 00:00, 00:20, and 00:40 to compact into a gen3 of 60m duration.
    // The data for those start times can come from gen1 or gen2.
    let target_count = compaction_config.number_of_previous_generations_to_compact(output_level);
    for (gen_time, gens) in new_block_times_to_gens.into_iter().rev() {
        let mut prev_times = HashSet::new();
        for g in &gens {
            prev_times.insert(
                compaction_config.previous_generation_start_time(g.start_time_secs, output_level),
            );
        }
        if prev_times.len() >= target_count as usize {
            let output_duration = compaction_config
                .generation_duration(output_level)
                .expect("output level should have a duration");
            let output_generation =
                Generation::new_with_start(output_level, gen_time, output_duration);
            let input_ids = gens.iter().map(|g| g.id).collect::<Vec<_>>();
            let leftover_ids = generations
                .iter()
                .filter(|g| g.level.is_one() && !input_ids.contains(&g.id))
                .map(|g| g.id)
                .collect::<Vec<_>>();
            let compaction_plan = NextCompactionPlan {
                db_id,
                table_id,
                output_generation,
                input_ids,
                leftover_ids,
            };

            return Some(compaction_plan);
        }
    }

    None
}

#[derive(Debug)]
pub(crate) enum CompactionPlan {
    LeftoverOnly(LeftoverPlan),
    Compaction(NextCompactionPlan),
}

impl CompactionPlan {
    pub(crate) fn db_id(&self) -> DbId {
        match self {
            Self::LeftoverOnly(plan) => plan.db_id,
            Self::Compaction(plan) => plan.db_id,
        }
    }

    pub(crate) fn table_id(&self) -> TableId {
        match self {
            Self::LeftoverOnly(plan) => plan.table_id,
            Self::Compaction(plan) => plan.table_id,
        }
    }
}

/// This plan is what gets created when the only compaction to be done is with gen1 files
/// that overlap with older generations (3+) or there aren't enough gen1 files to compact into a larger gen2 generation. In that case, we'll want to just update the
/// `CompactionDetail` for the table with this information so that the historical compaction
/// can be run later. For now, we want to advance the snapshot trackers of the upstream gen1 hosts.
#[derive(Debug)]
pub(crate) struct LeftoverPlan {
    pub(crate) db_id: DbId,
    pub(crate) table_id: TableId,
    pub(crate) leftover_gen1_ids: Vec<GenerationId>,
}

/// When the planner gets called to plan a compaction on a table, this contains all the detail
/// to run whatever the next highest priority compaction is. The returned information from that
/// compaction combined with the leftover_ids will give us enough detail to write a new
/// `CompactionDetail` file for the table.
#[derive(Debug)]
pub(crate) struct NextCompactionPlan {
    pub db_id: DbId,
    pub table_id: TableId,
    pub output_generation: Generation,
    /// The input generations for this compaction. Could be empty if there are only gen1 files
    /// getting compacted.
    pub input_ids: Vec<GenerationId>,
    /// The ids for the gen1 files that will be left over after this compaction plan runs
    pub leftover_ids: Vec<GenerationId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_pro_data_layout::{gen_time_string, gen_time_string_to_start_time_secs};

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
            TestCase {
                description: "four gen 1, older gen2",
                input: vec![
                    (5, 1, "2024-10-13/15-10"),
                    (4, 1, "2024-10-13/15-10"),
                    (3, 1, "2024-10-13/15-00"),
                    (2, 1, "2024-10-13/15-00"),
                    (1, 2, "2024-10-13/14-40"),
                ],
                output_level: 2,
                output_time: "2024-10-13/15-00",
                compact_ids: vec![2, 3, 4, 5],
                leftover_ids: vec![],
            },
        ];

        for tc in test_cases {
            let mut gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| Generation {
                    id: GenerationId::from(*id),
                    level: GenerationLevel::new(*level),
                    start_time_secs: gen_time_string_to_start_time_secs(time).unwrap(),
                    max_time: 0,
                })
                .collect();
            gens.sort();
            let plan = create_gen1_plan(&compaction_config, "db".into(), "table".into(), &gens);
            match plan {
                CompactionPlan::Compaction(NextCompactionPlan {
                    output_generation,
                    input_ids,
                    leftover_ids,
                    ..
                }) => {
                    assert_eq!(
                        output_generation.level,
                        GenerationLevel::new(tc.output_level),
                        "{}: expected level {} but got {}",
                        tc.description,
                        tc.output_level,
                        output_generation.level,
                    );
                    assert_eq!(
                        output_generation.start_time_secs,
                        gen_time_string_to_start_time_secs(tc.output_time).unwrap(),
                        "{}: expected gen time {} but got {}",
                        tc.description,
                        tc.output_time,
                        gen_time_string(output_generation.start_time_secs)
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
    fn gen1_leftover_plans() {
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
            TestCase {
                description: "two gen1 at the same time (i.e. from two different hosts)",
                input: vec![(23, 1, "2024-09-05/12-00"), (24, 1, "2024-09-05/12-00")],
                leftover_ids: vec![23, 24],
            },
        ];

        for tc in test_cases {
            let gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| Generation {
                    id: GenerationId::from(*id),
                    level: GenerationLevel::new(*level),
                    start_time_secs: gen_time_string_to_start_time_secs(time).unwrap(),
                    max_time: 0,
                })
                .collect();
            let plan = create_gen1_plan(&compaction_config, "db".into(), "table".into(), &gens);
            match plan {
                CompactionPlan::LeftoverOnly(LeftoverPlan {
                    leftover_gen1_ids, ..
                }) => {
                    let leftover_ids = leftover_gen1_ids
                        .iter()
                        .map(|g| g.as_u64())
                        .collect::<Vec<_>>();
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

    #[test]
    fn next_plan_cases() {
        struct TestCase<'a> {
            // description of what the test case is for
            description: &'a str,
            // input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            // the output level we're testing for
            output_level: u8,
            // the expected output gen_time of the compaction
            output_time: &'a str,
            // the expected ids from the input that will be used for the compaction
            compact_ids: Vec<u64>,
        }

        let compaction_config = CompactionConfig::default();
        let test_cases = vec![
            TestCase {
                description: "level 2 to 3 compaction",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/12-40"),
                    (12, 3, "2024-10-14/11-00"),
                    (15, 3, "2024-10-14/10-00"),
                ],
                output_level: 3,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![3, 6, 9],
            },
            TestCase {
                description: "level 2 to 3 with some gen1 blocks coming along for the ride",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/12-40"),
                    (12, 3, "2024-10-14/11-00"),
                    (15, 3, "2024-10-14/10-00"),
                    (7, 1, "2024-10-14/12-10"),
                    (11, 1, "2024-10-14/12-40"),
                ],
                output_level: 3,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![3, 6, 7, 9, 11],
            },
        ];

        for tc in test_cases {
            let mut gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| Generation {
                    id: GenerationId::from(*id),
                    level: GenerationLevel::new(*level),
                    start_time_secs: gen_time_string_to_start_time_secs(time).unwrap(),
                    max_time: 0,
                })
                .collect();
            gens.sort();
            let plan = create_next_plan(
                &compaction_config,
                "db".into(),
                "table".into(),
                &gens,
                GenerationLevel::new(tc.output_level),
            );
            match plan {
                Some(NextCompactionPlan {
                    output_generation,
                    input_ids,
                    ..
                }) => {
                    assert_eq!(
                        output_generation.level,
                        GenerationLevel::new(tc.output_level),
                        "{}: expected level {} but got {}",
                        tc.description,
                        tc.output_level,
                        output_generation.level,
                    );
                    assert_eq!(
                        output_generation.start_time_secs,
                        gen_time_string_to_start_time_secs(tc.output_time).unwrap(),
                        "{}: expected gen time {} but got {}",
                        tc.description,
                        tc.output_time,
                        gen_time_string(output_generation.start_time_secs)
                    );
                    let mut ids_to_compact =
                        input_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    ids_to_compact.sort();
                    assert_eq!(
                        tc.compact_ids, ids_to_compact,
                        "{}: expected ids {:?} but got {:?}",
                        tc.description, tc.compact_ids, ids_to_compact
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
    fn next_plan_no_cases() {
        struct TestCase<'a> {
            // description of what the test case is for
            description: &'a str,
            // input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            // the output level we're testing for
            output_level: u8,
        }

        let compaction_config = CompactionConfig::default();
        let test_cases = vec![
            TestCase {
                description: "level 2 to 3 compaction, but not enough gen2 blocks",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (11, 3, "2024-10-14/11-00"),
                ],
                output_level: 3,
            },
            TestCase {
                description: "level 2 to 3 compaction, but gen2 blocks in different gen3 times",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/13-20"),
                    (12, 2, "2024-10-14/13-40"),
                    (15, 2, "2024-10-14/14-00"),
                ],
                output_level: 3,
            },
        ];

        for tc in test_cases {
            let mut gens: Vec<_> = tc
                .input
                .iter()
                .map(|(id, level, time)| Generation {
                    id: GenerationId::from(*id),
                    level: GenerationLevel::new(*level),
                    start_time_secs: gen_time_string_to_start_time_secs(time).unwrap(),
                    max_time: 0,
                })
                .collect();
            gens.sort();
            let plan = create_next_plan(
                &compaction_config,
                "db".into(),
                "table".into(),
                &gens,
                GenerationLevel::new(tc.output_level),
            );

            assert!(
                plan.is_none(),
                "expected no compaction plan for test case '{}'",
                tc.description
            );
        }
    }
}
