//! Planner contains logic for organizing compaction within a table and creating compaction plans.

use hashbrown::HashMap;
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_pro_data_layout::{
    CompactionConfig, Generation, GenerationId, GenerationLevel, HostSnapshotMarker,
};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("host {} is not getting tracked", .0)]
    NotTrackingHost(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub(crate) struct SnapshotAdvancePlan {
    /// The host snapshot markers that will be saved at the end of this advance plan being run
    pub(crate) host_snapshot_markers: Vec<Arc<HostSnapshotMarker>>,
    /// The compaction plans that must be run to advance the snapshot summary beyond these snapshots
    pub(crate) compaction_plans: HashMap<Arc<str>, Vec<CompactionPlan>>,
}

impl SnapshotAdvancePlan {
    /// Evaluates the snapshots sitting in compacted data along with the existing
    /// generations to determine if the snapshots should be advance. If so, it will
    /// return a plan. After running all compactions, the compaction summary will
    /// be updated to mark the new snapshot position per host.
    pub(crate) fn should_advance(compacted_data: &CompactedData) -> Option<Self> {
        let must_advance = compacted_data.should_compact_and_advance_snapshots();
        let mut next_compaction_present = false;
        let mut compaction_plans = HashMap::new();

        let (gen1_tables, host_snapshot_markers) =
            compacted_data.tables_with_gen1_awaiting_compaction_and_markers();

        for (db_name, table_map) in gen1_tables {
            let plans = compaction_plans
                .entry(Arc::clone(&db_name))
                .or_insert_with(Vec::new);
            for (table_name, generations) in table_map {
                if let Some(plan) = create_next_plan(
                    &compacted_data.compaction_config,
                    Arc::clone(&db_name),
                    Arc::clone(&table_name),
                    &generations,
                    GenerationLevel::two(),
                    host_snapshot_markers.len(),
                    must_advance,
                ) {
                    next_compaction_present = true;
                    plans.push(CompactionPlan::Compaction(plan));
                } else {
                    let plan = CompactionPlan::LeftoverOnly(LeftoverPlan {
                        db_name: Arc::clone(&db_name),
                        table_name: Arc::clone(&table_name),
                        leftover_gen1_ids: generations
                            .iter()
                            .filter_map(|g| {
                                if g.level.is_under_two() {
                                    Some(g.id)
                                } else {
                                    None
                                }
                            })
                            .collect(),
                    });
                    plans.push(plan);
                }
            }
        }

        if must_advance || next_compaction_present {
            Some(SnapshotAdvancePlan {
                host_snapshot_markers,
                compaction_plans,
            })
        } else {
            None
        }
    }
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
        let host_count = compacted_data.last_snapshot_marker_per_host().len();
        let mut compaction_plans = Vec::new();
        for db in compacted_data.databases() {
            for table in compacted_data.tables(db.as_ref()) {
                let generations = compacted_data.get_generations(db.as_ref(), table.as_ref());
                if let Some(plan) = create_next_plan(
                    &compacted_data.compaction_config,
                    Arc::clone(&db),
                    Arc::clone(&table),
                    &generations,
                    output_level,
                    host_count,
                    false, // this plan is created in between snapshots, so it's not about advancing
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

/// Given a list of generations, returns the next compaction plan of the passed in generation to
/// run. If there are no compactions to run at that level, it will return None.
pub(crate) fn create_next_plan(
    compaction_config: &CompactionConfig,
    db_name: Arc<str>,
    table_name: Arc<str>,
    generations: &[Generation],
    output_level: GenerationLevel,
    host_count: usize,
    must_advance: bool,
) -> Option<NextCompactionPlan> {
    // first, group the generations together into what their start time would be at the
    // chosen output level. Only include generations that are less than the output level.
    let mut new_block_times_to_gens = BTreeMap::new();
    for gen in generations {
        // only include generations that are less than the desired output level, unless we're
        // trying to get to gen2, in which case we should also include those
        if (gen.level < output_level) || (output_level.is_two() && gen.level.is_two()) {
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
        let mut prev_times_counts = HashMap::new();
        for g in &gens {
            let count = prev_times_counts
                .entry(
                    compaction_config
                        .previous_generation_start_time(g.start_time_secs, output_level),
                )
                .or_insert(0);
            *count += 1;
        }

        // ensure we have at least one generation newer than this block to keep around
        let gen_end_time = gen_time
            + compaction_config
                .generation_duration(output_level)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
        let has_one_newer = generations
            .iter()
            .any(|g| g.start_time_secs >= gen_end_time);
        // only enforce this if we don't have to advance the snapshot
        if !has_one_newer && !must_advance {
            continue;
        }

        // check that all counts are at least equal to the host count
        let all_over_host_count = prev_times_counts
            .values()
            .filter(|c| **c < host_count)
            .count()
            == 0;
        // we relax this constraint if we have to advance the snapshot
        let over_host_count_or_must_advance = all_over_host_count || must_advance;
        if over_host_count_or_must_advance && prev_times_counts.len() >= target_count as usize {
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
                db_name,
                table_name,
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
#[derive(Debug)]
pub(crate) struct LeftoverPlan {
    pub(crate) db_name: Arc<str>,
    pub(crate) table_name: Arc<str>,
    pub(crate) leftover_gen1_ids: Vec<GenerationId>,
}

/// When the planner gets called to plan a compaction on a table, this contains all the detail
/// to run whatever the next highest priority compaction is. The returned information from that
/// compaction combined with the leftover_ids will give us enough detail to write a new
/// `CompactionDetail` file for the table.
#[derive(Debug)]
pub(crate) struct NextCompactionPlan {
    pub db_name: Arc<str>,
    pub table_name: Arc<str>,
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
            // the expected leftover gen1 ids after the compaction
            leftover_gen1_ids: Vec<u64>,
        }

        let compaction_config = CompactionConfig::default();
        let test_cases = vec![
            TestCase {
                description: "gen1 to 2 compaction, but only if an gen1 left behind",
                input: vec![
                    (5, 1, "2024-10-14/12-00"),
                    (6, 1, "2024-10-14/12-10"),
                    (7, 1, "2024-10-14/12-20"),
                ],
                output_level: 2,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![5, 6],
                leftover_gen1_ids: vec![7],
            },
            TestCase {
                description: "gen1 to 2 as a compaction into an existing gen2",
                input: vec![
                    (7, 1, "2024-10-14/12-20"),
                    (6, 1, "2024-10-14/12-10"),
                    (8, 2, "2024-10-14/12-00"),
                ],
                output_level: 2,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![6, 8],
                leftover_gen1_ids: vec![7],
            },
            TestCase {
                description: "level 2 to 3 compaction",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/12-40"),
                    (10, 2, "2024-10-14/13-00"),
                    (12, 3, "2024-10-14/11-00"),
                    (15, 3, "2024-10-14/10-00"),
                ],
                output_level: 3,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![3, 6, 9],
                leftover_gen1_ids: vec![],
            },
            TestCase {
                description: "level 2 to 3 with some gen1 blocks coming along for the ride",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/12-40"),
                    (10, 2, "2024-10-14/13-00"),
                    (12, 3, "2024-10-14/11-00"),
                    (15, 3, "2024-10-14/10-00"),
                    (7, 1, "2024-10-14/12-10"),
                    (11, 1, "2024-10-14/12-40"),
                ],
                output_level: 3,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![3, 6, 7, 9, 11],
                leftover_gen1_ids: vec![],
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
                1,
                false,
            );
            match plan {
                Some(NextCompactionPlan {
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
                    let mut ids_to_compact =
                        input_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    ids_to_compact.sort();
                    assert_eq!(
                        tc.compact_ids, ids_to_compact,
                        "{}: expected ids {:?} but got {:?}",
                        tc.description, tc.compact_ids, ids_to_compact
                    );
                    let mut leftover_gen1_ids =
                        leftover_ids.iter().map(|g| g.as_u64()).collect::<Vec<_>>();
                    leftover_gen1_ids.sort();
                    assert_eq!(
                        tc.leftover_gen1_ids, leftover_gen1_ids,
                        "{}: expected leftover gen1 ids {:?} but got {:?}",
                        tc.description, tc.leftover_gen1_ids, leftover_gen1_ids
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
                description: "gen1 to 2 doesn't happen if we don't have a gen1 to leave behind",
                input: vec![
                    (7, 1, "2024-10-14/12-00"),
                    (6, 1, "2024-10-14/12-10"),
                    (9, 2, "2024-10-14/11-40"),
                ],
                output_level: 2,
            },
            TestCase {
                description:
                    "gen1 to 2 doesn't happen if we don't have a gen1 to leave behind (odd split)",
                input: vec![
                    (7, 1, "2024-10-14/12-10"),
                    (6, 1, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/11-40"),
                ],
                output_level: 2,
            },
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
                description: "level 2 to 3 compaction, but don't have a gen2 block to leave behind",
                input: vec![
                    (3, 2, "2024-10-14/12-00"),
                    (6, 2, "2024-10-14/12-20"),
                    (9, 2, "2024-10-14/12-40"),
                    (12, 3, "2024-10-14/11-00"),
                    (15, 3, "2024-10-14/10-00"),
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
                1,
                false,
            );

            assert!(
                plan.is_none(),
                "expected no compaction plan for test case '{}'",
                tc.description
            );
        }
    }
}
