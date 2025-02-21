//! Planner contains logic for organizing compaction within a table and creating compaction plans.

use crate::compacted_data::CompactedData;
use crate::producer::Gen1FileMap;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{DatabaseSchema, TableDefinition};
use influxdb3_enterprise_data_layout::{
    CompactionConfig, CompactionDetail, Gen1File, Generation, GenerationLevel,
};
use object_store::path::Path;
use observability_deps::tracing::warn;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("writer id {} is not getting tracked", .0)]
    NotTrackingWriterId(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This is a group of compaction plans, generally at the same level that can be run that will
/// be used to output a new summary when complete
#[derive(Debug)]
pub(crate) struct CompactionPlanGroup {
    /// The compaction plans that must be run to advance the compaction summary
    pub(crate) next_compaction_plans: Vec<NextCompactionPlan>,
    pub(crate) leftover_plans: Vec<LeftoverPlan>,
}

impl CompactionPlanGroup {
    pub(crate) fn plans_for_level(
        compaction_config: &CompactionConfig,
        compacted_data: &CompactedData,
        new_files_to_compact: &Gen1FileMap,
        output_level: GenerationLevel,
    ) -> Result<Option<Self>, String> {
        let mut next_compaction_plans = Vec::new();
        let mut leftover_plans = Vec::new();

        let mut db_ids = compacted_data.db_ids();
        db_ids.extend(new_files_to_compact.keys());
        db_ids.sort_unstable();
        db_ids.dedup();

        for db_id in db_ids {
            let mut table_ids = compacted_data.table_ids(db_id);
            if let Some(new_table_ids) = new_files_to_compact
                .get(&db_id)
                .map(|t| t.keys().copied().collect::<Vec<_>>())
            {
                table_ids.extend(new_table_ids);
            }

            table_ids.sort_unstable();
            table_ids.dedup();

            for table_id in table_ids {
                let compaction_detail = compacted_data.compaction_detail(db_id, table_id);
                let gen1_files = new_files_to_compact
                    .get(&db_id)
                    .and_then(|t| t.get(&table_id));

                let db_schema = compacted_data
                    .compacted_catalog
                    .db_schema_by_id(&db_id)
                    .ok_or("database schema is missing")?;

                let table_definition = db_schema
                    .table_definition_by_id(&table_id)
                    .ok_or("table definition is missing")?;

                let plan = create_next_plan(
                    compacted_data,
                    compaction_config,
                    Arc::clone(&db_schema),
                    Arc::clone(&table_definition),
                    output_level,
                    compaction_detail,
                    gen1_files,
                );

                if let Some(plan) = plan {
                    next_compaction_plans.push(plan);
                } else if let Some(gen1_files) = gen1_files {
                    leftover_plans.push(LeftoverPlan {
                        db_schema: Arc::clone(&db_schema),
                        table_definition: Arc::clone(&table_definition),
                        leftover_gen1_files: gen1_files.to_vec(),
                    });
                }
            }
        }

        if next_compaction_plans.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Self {
                next_compaction_plans,
                leftover_plans,
            }))
        }
    }
}

/// Given a list of generations, returns the next compaction plan of the passed in generation to
/// run. If there are no compactions to run at that level, it will return None.
fn create_next_plan(
    compacted_data: &CompactedData,
    compaction_config: &CompactionConfig,
    db_schema: Arc<DatabaseSchema>,
    table_definition: Arc<TableDefinition>,
    output_level: GenerationLevel,
    last_compaction_detail: Option<Arc<CompactionDetail>>,
    new_gen1_files: Option<&Vec<Gen1File>>,
) -> Option<NextCompactionPlan> {
    let mut generations = last_compaction_detail
        .as_ref()
        .map(|d| {
            let mut generations = d.compacted_generations.clone();
            generations.extend(d.leftover_gen1_files.iter().map(|g| g.generation()));
            generations
        })
        .unwrap_or_default();

    if let Some(gen1_files) = new_gen1_files {
        generations.extend(gen1_files.iter().map(|f| f.generation()));
    }

    generations.sort();

    let (output_generation, input_generations) =
        compaction_for_level(&generations, output_level, compaction_config)?;

    // combine new gen1 files with those leftover from the last compaction to form the leftover
    // gen1 files for this plan:
    let mut leftover_gen1_files = vec![];

    // put any gen1 files that aren't in the input ids into the leftover
    if let Some(new_gen1_files) = new_gen1_files {
        for f in new_gen1_files {
            if !input_generations.iter().any(|g| g.id == f.id) {
                leftover_gen1_files.push(f.clone());
            }
        }
    }
    if let Some(last_compaction_detail) = last_compaction_detail.as_ref() {
        for f in last_compaction_detail.leftover_gen1_files.iter() {
            if !input_generations.iter().any(|g| g.id == f.id) {
                leftover_gen1_files.push(f.clone());
            }
        }
    }

    let mut input_paths = vec![];

    for genr in &input_generations {
        if genr.level.is_one() {
            let f = new_gen1_files.and_then(|files| files.iter().find(|f| f.id == genr.id));
            if let Some(f) = f {
                input_paths.push(Path::from(f.file.path.as_str()));
            } else if let Some(f) = last_compaction_detail
                .as_ref()
                .and_then(|d| d.leftover_gen1_files.iter().find(|f| f.id == genr.id))
            {
                input_paths.push(Path::from(f.file.path.as_str()));
            } else {
                warn!(id = genr.id.as_u64(), "gen1 file not found for compaction");
            }
        } else {
            let gen_detail =
                compacted_data.parquet_files(db_schema.id, table_definition.table_id, genr.id);
            for file in gen_detail {
                input_paths.push(Path::from(file.path.as_str()));
            }
        }
    }

    // To avoid using too much memory, skip this plan if it would attempt to compact more
    // than the maximum configured number of files.
    if input_paths.len() > compaction_config.max_num_files_per_compaction {
        return None;
    }

    Some(NextCompactionPlan {
        db_schema,
        table_definition,
        output_generation,
        input_generations,
        input_paths,
        leftover_gen1_files,
    })
}

/// If there is a compaction to be done with the input generations at the given level, the output
/// generation and a vec of the generation ids to compact is returned.
fn compaction_for_level(
    generations: &[Generation],
    output_level: GenerationLevel,
    compaction_config: &CompactionConfig,
) -> Option<(Generation, Vec<Generation>)> {
    // first, group the generations together into what their start time would be at the
    // chosen output level. Only include generations that are less than the output level.
    let mut new_block_times_to_gens = BTreeMap::new();
    for genr in generations {
        // only include generations that are less than the desired output level, unless we're
        // trying to get to gen2, in which case we should also include those
        if (genr.level < output_level) || (output_level.is_two() && genr.level.is_two()) {
            let start_time =
                compaction_config.generation_start_time(output_level, genr.start_time_secs);
            let gens = new_block_times_to_gens
                .entry(start_time)
                .or_insert_with(Vec::new);
            gens.push(*genr);
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

        // ensure we have at least one generation newer than this block to keep around. We want
        // to do this because we could have lagged data coming in, or another gen1 file coming
        // in from another writer. We don't want to compact too aggressively, otherwise we'll
        // end up redoing all that work.
        let gen_end_time = gen_time
            + compaction_config
                .generation_duration(output_level)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
        let has_one_newer = generations
            .iter()
            .any(|g| g.start_time_secs >= gen_end_time);
        if !has_one_newer {
            continue;
        }

        // only do a compaction if we have enough generations to compact into the output level
        if prev_times_counts.len() >= target_count as usize {
            // NOTE: we can expect an output duration here because the `output_level` passed in is
            // based on the compaction configuration's set of generation durations, so we wouldn't
            // pass an `output_level` that would yeild a `None` here...
            let output_duration = compaction_config
                .generation_duration(output_level)
                .expect("output level should have a duration");
            let output_generation =
                Generation::new_with_start(output_level, gen_time, output_duration);

            return Some((output_generation, gens));
        }
    }

    None
}

/// This plan is what gets created when the only compaction to be done is with gen1 files
/// that overlap with older generations (3+) or there aren't enough gen1 files to compact into a larger gen2 generation. In that case, we'll want to just update the
/// `CompactionDetail` for the table with this information so that the historical compaction
/// can be run later. For now, we want to advance the snapshot trackers of the upstream gen1 writers.
#[derive(Debug)]
pub(crate) struct LeftoverPlan {
    pub(crate) db_schema: Arc<DatabaseSchema>,
    pub(crate) table_definition: Arc<TableDefinition>,
    pub(crate) leftover_gen1_files: Vec<Gen1File>,
}

/// When the planner gets called to plan a compaction on a table, this contains all the detail
/// to run whatever the next highest priority compaction is. The returned information from that
/// compaction combined with the leftover_ids will give us enough detail to write a new
/// `CompactionDetail` file for the table.
#[derive(Debug)]
pub(crate) struct NextCompactionPlan {
    pub db_schema: Arc<DatabaseSchema>,
    pub table_definition: Arc<TableDefinition>,
    pub output_generation: Generation,
    pub input_generations: Vec<Generation>,
    pub input_paths: Vec<Path>,
    pub leftover_gen1_files: Vec<Gen1File>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_enterprise_data_layout::{
        gen_time_string, gen_time_string_to_start_time_secs, GenerationId,
    };

    #[test]
    fn next_plan_cases() {
        struct TestCase<'a> {
            /// description of what the test case is for
            description: &'a str,
            /// input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            /// the output level we're testing for
            output_level: u8,
            /// the expected output gen_time of the compaction
            output_time: &'a str,
            /// the expected ids from the input that will be used for the compaction
            compact_ids: Vec<u64>,
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
            },
            TestCase {
                description: "level 3 to 4 compaction, with some gen1's",
                input: vec![
                    (4, 3, "2024-10-14/12-00"),
                    (5, 3, "2024-10-14/13-00"),
                    (10, 1, "2024-10-14/13-30"),
                    (6, 3, "2024-10-14/14-00"),
                    (7, 3, "2024-10-14/15-00"),
                    (9, 1, "2024-10-14/15-10"),
                    (8, 3, "2024-10-14/16-00"),
                ],
                output_level: 4,
                output_time: "2024-10-14/12-00",
                compact_ids: vec![4, 5, 6, 7, 9, 10],
            },
            TestCase {
                description: "level 4 to 5 compaction, with some gen1's",
                input: vec![
                    (4, 4, "2024-10-14/00-00"),
                    (5, 4, "2024-10-14/04-00"),
                    (6, 4, "2024-10-14/08-00"),
                    (11, 1, "2024-10-14/09-20"),
                    (7, 4, "2024-10-14/12-00"),
                    (8, 4, "2024-10-14/16-00"),
                    (12, 1, "2024-10-14/17-30"),
                    (9, 4, "2024-10-14/20-00"),
                    (10, 4, "2024-10-15/00-00"),
                ],
                output_level: 5,
                output_time: "2024-10-14/00-00",
                compact_ids: vec![4, 5, 6, 7, 8, 9, 11, 12],
            },
            TestCase {
                description: "level 5 to 6 compaction, with some gen1's",
                input: vec![
                    (4, 5, "2024-10-14/00-00"),
                    (5, 5, "2024-10-15/00-00"),
                    (10, 1, "2024-10-15/12-20"),
                    (6, 5, "2024-10-16/00-00"),
                    (7, 5, "2024-10-17/00-00"),
                    (11, 1, "2024-10-17/13-40"),
                    (8, 5, "2024-10-18/00-00"),
                    (9, 5, "2024-10-19/00-00"),
                ],
                output_level: 6,
                output_time: "2024-10-14/00-00",
                compact_ids: vec![4, 5, 6, 7, 8, 10, 11],
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
            if let Some((output_generation, input_generations)) = compaction_for_level(
                &gens,
                GenerationLevel::new(tc.output_level),
                &compaction_config,
            ) {
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
                let mut ids_to_compact = input_generations
                    .iter()
                    .map(|g| g.id.as_u64())
                    .collect::<Vec<_>>();
                ids_to_compact.sort();
                assert_eq!(
                    tc.compact_ids, ids_to_compact,
                    "{}: expected ids {:?} but got {:?}",
                    tc.description, tc.compact_ids, ids_to_compact
                );
            } else {
                panic!(
                    "expected a compaction plan for test case '{}'",
                    tc.description
                );
            }
        }
    }

    #[test]
    fn next_plan_no_cases() {
        struct TestCase<'a> {
            /// description of what the test case is for
            description: &'a str,
            /// input is a list of (generation_id, level, gen_time)
            input: Vec<(u64, u8, &'a str)>,
            /// the output level we're testing for
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
            let plan = compaction_for_level(
                &gens,
                GenerationLevel::new(tc.output_level),
                &compaction_config,
            );

            assert!(
                plan.is_none(),
                "expected no compaction plan for test case '{}'",
                tc.description
            );
        }
    }
}
