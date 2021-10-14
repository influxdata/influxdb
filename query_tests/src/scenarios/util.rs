//! This module contains util functions for testing scenarios

use predicate::delete_predicate::DeletePredicate;
use query::{QueryChunk, QueryDatabase};
use std::fmt::Display;
use std::sync::Arc;

use server::db::test_helpers::write_lp;
use server::utils::make_db;

use super::DbScenario;

// Structs, enums, and functions used to exhaust all test scenarios of chunk life cycle
// & when delete predicates are applied

// STRUCTs & ENUMs
#[derive(Debug, Clone)]
pub struct ChunkData<'a> {
    /// Line protocol data of this chunk
    pub lp_lines: Vec<&'a str>,
    /// which stage this chunk will be created
    pub chunk_stage: ChunkStage,
}

#[derive(Debug, Clone)]
pub enum ChunkStage {
    /// Open MUB
    Mubo,
    /// Frozen MUB
    Mubf,
    /// RUB without OS
    Rub,
    /// both RUB and OS of the chunk exist
    RubOs,
    /// OS only
    Os,
}

impl Display for ChunkStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkStage::Mubo => write!(f, "Open MUB"),
            ChunkStage::Mubf => write!(f, "Frozen MUB"),
            ChunkStage::Rub => write!(f, "RUB"),
            ChunkStage::RubOs => write!(f, "RUB & OS"),
            ChunkStage::Os => write!(f, "OS"),
        }
    }
}

impl ChunkStage {
    /// return the list of all chunk types
    pub fn all() -> Vec<Self> {
        vec![Self::Mubo, Self::Mubf, Self::Rub, Self::RubOs, Self::Os]
    }
}

#[derive(Debug, Clone)]
pub struct Pred<'a> {
    /// Delete predicate
    predicate: &'a DeletePredicate,
    /// At which chunk stage this predicate is applied
    delete_time: DeleteTime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeleteTime {
    /// Delete predicate happens after all chunks created
    /// and moved to their corresponding stages
    End,
    /// Delete predicate is added to chunks at their Mub Open stage
    Mubo,
    /// Delete predicate is added to chunks at their Mub Frozen stage
    Mubf,
    /// Delete predicate is added to chunks at their Rub stage
    Rub,
    /// Delete predicate is added to chunks at their Rub & Os stage
    RubOs,
    /// Delete predicate is added to chunks at their Os stage
    Os,
}

impl DeleteTime {
    /// Return all DeleteTime at and after the given chunk stage
    pub fn all_from_and_before(chunk_stage: ChunkStage) -> Vec<DeleteTime> {
        match chunk_stage {
            ChunkStage::Mubo => vec![DeleteTime::Mubo],
            ChunkStage::Mubf => vec![DeleteTime::Mubo, DeleteTime::Mubf],
            ChunkStage::Rub => vec![DeleteTime::Mubo, DeleteTime::Mubf, DeleteTime::Rub],
            ChunkStage::RubOs => vec![
                DeleteTime::Mubo,
                DeleteTime::Mubf,
                DeleteTime::Rub,
                DeleteTime::RubOs,
            ],
            ChunkStage::Os => vec![
                DeleteTime::Mubo,
                DeleteTime::Mubf,
                DeleteTime::Rub,
                DeleteTime::RubOs,
                DeleteTime::Os,
            ],
        }
    }
}

// --------------------------------------------------------------------------------------------

/// All scenarios chunk stages and their life cycle moves for given set of delete predicates
/// If the delete predicates are empty, all scenarios of different chunk stages will return
pub async fn all_scenarios_for_one_chunk(
    // These delete predicates are applied at all stages of the chunk life cycle
    chunk_stage_preds: Vec<&DeletePredicate>,
    // These delete predicates are applied to all chunks at their final stages
    at_end_preds: Vec<&DeletePredicate>,
    // Single chunk data
    lp_lines: Vec<&str>,
    // Table of the chunk
    table_name: &str,
    // Partition of the chunk
    partition_key: &str,
) -> Vec<DbScenario> {
    // Make delete predicates that happen when all chunks in their final stages
    let end_preds: Vec<Pred> = at_end_preds
        .iter()
        .map(|p| Pred {
            predicate: *p,
            delete_time: DeleteTime::End,
        })
        .collect();

    let mut scenarios = vec![];
    // Go over chunk stages
    for chunk_stage in ChunkStage::all() {
        // Apply delete chunk_stage_preds to this chunk stage at
        // all stages at and before that in the life cycle to the chunk
        // But only need to get all delete times if chunk_stage_preds is not empty,
        // otherwise, produce only one scenario of each chunk stage
        let mut delete_times = vec![DeleteTime::Mubo];
        if !chunk_stage_preds.is_empty() {
            delete_times = DeleteTime::all_from_and_before(chunk_stage.clone())
        };

        for delete_time in delete_times {
            // make delete predicate with time it happens
            let mut preds: Vec<Pred> = chunk_stage_preds
                .iter()
                .map(|p| Pred {
                    predicate: *p,
                    delete_time: delete_time.clone(),
                })
                .collect();
            // extend at-end predicates
            preds.extend(end_preds.clone());

            // make this specific chunk stage & delete predicates scenario
            scenarios.push(
                make_chunk_with_deletes_at_different_stages(
                    lp_lines.clone(),
                    chunk_stage.clone(),
                    preds,
                    table_name,
                    partition_key,
                )
                .await,
            );
        }
    }

    scenarios
}

/// Build a chunk that may move with life cycle before/after deletes
/// Note that the only chunk in this function can be moved to different stages and delete predicates
/// can be applied at different stages when the chunk is moved.
pub async fn make_chunk_with_deletes_at_different_stages(
    lp_lines: Vec<&str>,
    chunk_stage: ChunkStage,
    preds: Vec<Pred<'_>>,
    delete_table_name: &str, // table of deleting data
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;

    // ----------------------
    // Make an open MUB
    //
    // There may be more than one tables in the lp data
    let tables = write_lp(&db, &lp_lines.join("\n")).await;
    for table in &tables {
        let num_mubs = db.num_mub_table_chunks(table.as_str(), partition_key);
        // must be one MUB per table
        assert_eq!(num_mubs, 1);
    }
    // Apply delete predicate
    let mut deleted = false;
    let mut display = "".to_string();
    let mut count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Mubo {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            deleted = true;
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from open MUB", count).as_str());
    }

    // ----------------------
    // Freeze MUB if requested
    match chunk_stage {
        ChunkStage::Mubf | ChunkStage::Rub | ChunkStage::RubOs | ChunkStage::Os => {
            // Since mub are frozen at delete, no need to do it in that case for table of deleted data
            if !deleted {
                db.rollover_partition(delete_table_name, partition_key)
                    .await
                    .unwrap()
                    .unwrap();
            }

            // Freeze MUBs of tables that not have deleted/deleting data
            for table in &tables {
                if table != delete_table_name {
                    db.rollover_partition(table.as_str(), partition_key)
                        .await
                        .unwrap()
                        .unwrap();
                }
            }

            // Verify still one MUB and no RUB for each table
            for table in &tables {
                let num_mubs = db.num_mub_table_chunks(table.as_str(), partition_key);
                let num_rubs = db.num_rub_table_chunks(table.as_str(), partition_key);
                assert_eq!(num_mubs, 1);
                assert_eq!(num_rubs, 0);
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Mubf {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from frozen MUB", count).as_str());
    }

    // ----------------------
    // Move MUB to RUB if requested
    match chunk_stage {
        ChunkStage::Rub | ChunkStage::RubOs | ChunkStage::Os => {
            let mut no_more_data = false;
            for table in &tables {
                // Compact this MUB of this table
                let chunk_result = db.compact_partition(table, partition_key).await.unwrap();

                // Verify no MUB and one RUB if not all data was soft deleted
                let num_mubs = db.num_mub_table_chunks(table.as_str(), partition_key);
                let num_rubs = db.num_rub_table_chunks(table.as_str(), partition_key);
                assert_eq!(num_mubs, 0);

                // Stop if compaction result is nothing which means MUB has
                // all soft deleted data and no RUB is created. No more data
                // to affect further delete
                if table == delete_table_name {
                    match chunk_result {
                        Some(_chunk) => {
                            assert_eq!(num_rubs, 1);
                        }
                        None => {
                            assert_eq!(num_rubs, 0);
                            no_more_data = true;
                        }
                    }
                } else {
                    assert_eq!(num_rubs, 1);
                }
            }

            if no_more_data {
                let scenario_name =
                    format!("Deleted data from one {} chunk{}", chunk_stage, display);
                return DbScenario { scenario_name, db };
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Rub {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from RUB", count).as_str());
    }

    // ----------------------
    // Persist RUB to OS if requested
    match chunk_stage {
        ChunkStage::RubOs | ChunkStage::Os => {
            let mut no_more_data = false;
            for table in &tables {
                // Persist RUB of this table
                let chunk_result = db
                    .persist_partition(table, partition_key, true)
                    .await
                    .unwrap();

                // Verify no MUB and one RUB if not all data was soft deleted
                let num_mubs = db.num_mub_table_chunks(table.as_str(), partition_key);
                let num_rubs = db.num_rub_table_chunks(table.as_str(), partition_key);
                let num_os = db.num_os_table_chunks(table.as_str(), partition_key);
                assert_eq!(num_mubs, 0);

                // Stop if persistent result is nothing which means RUB has
                // all soft deleted data and no OS is created. No more data
                // to affect further delete
                if table == delete_table_name {
                    match chunk_result {
                        Some(_chunk) => {
                            assert_eq!(num_rubs, 1); // still have RUB with the persisted OS
                            assert_eq!(num_os, 1);
                        }
                        None => {
                            assert_eq!(num_rubs, 0); //  ask Raphael if we also empty RUB of all soft deleted rows at persistence
                            assert_eq!(num_os, 0);
                            no_more_data = true;
                        }
                    }
                } else {
                    assert_eq!(num_rubs, 1);
                    assert_eq!(num_os, 1);
                }
            }

            if no_more_data {
                let scenario_name =
                    format!("Deleted data from one {} chunk{}", chunk_stage, display);
                return DbScenario { scenario_name, db };
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::RubOs {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count = 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from RUB & OS", count).as_str());
    }

    // ----------
    // Unload RUB
    if let ChunkStage::Os = chunk_stage {
        for table in &tables {
            // retrieve its chunk_id first
            let rub_chunk_ids = db.read_buffer_table_chunk_ids(table.as_str(), partition_key);
            assert_eq!(rub_chunk_ids.len(), 1);
            db.unload_read_buffer(table.as_str(), partition_key, rub_chunk_ids[0])
                .unwrap();

            // verify chunk stages
            let num_mubs = db.num_mub_table_chunks(table.as_str(), partition_key);
            let num_rubs = db.num_rub_table_chunks(table.as_str(), partition_key);
            let num_os = db.num_os_table_chunks(table.as_str(), partition_key);
            assert_eq!(num_mubs, 0);
            assert_eq!(num_rubs, 0);
            assert_eq!(num_os, 1);
        }
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Os || pred.delete_time == DeleteTime::End {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(
            format!(
                ", with {} deletes from OS or after all chunks are created",
                count
            )
            .as_str(),
        );
    }

    let scenario_name = format!("Deleted data from one {} chunk{}", chunk_stage, display);
    DbScenario { scenario_name, db }
}

/// Build many chunks which are in different stages
//  Note that, after a lot of thoughts, I decided to have 2 separated functions, this one and the one above.
//  The above tests delete predicates before and/or after a chunk is moved to different stages, while
//  this function tests different-stage chunks in various stages when one or many deletes happen.
//  Even though these 2 functions have some overlapped code, merging them in one
//  function will created a much more complicated cases to handle
pub async fn make_different_stage_chunks_with_deletes_scenario(
    data: Vec<ChunkData<'_>>,
    preds: Vec<&DeletePredicate>,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    let mut display = "".to_string();

    // Build chunks
    for chunk_data in &data {
        display.push_str(" - ");
        display.push_str(&chunk_data.chunk_stage.to_string());

        // ----------
        // Make an open MUB
        write_lp(&db, &chunk_data.lp_lines.join("\n")).await;
        // 0 does not represent the real chunk id. It is here just to initialize the chunk_id  variable for later assignment
        let mut chunk_id = db.chunk_summaries().unwrap()[0].id;

        // ----------
        // freeze MUB
        match chunk_data.chunk_stage {
            ChunkStage::Mubf | ChunkStage::Rub | ChunkStage::RubOs | ChunkStage::Os => {
                let chunk = db
                    .rollover_partition(table_name, partition_key)
                    .await
                    .unwrap()
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // ----------
        // Move MUB to RUB
        match chunk_data.chunk_stage {
            ChunkStage::Rub | ChunkStage::RubOs | ChunkStage::Os => {
                let chunk = db
                    .compact_chunks(table_name, partition_key, |chunk| chunk.id() == chunk_id)
                    .await
                    .unwrap()
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // ----------
        // Move RUB to OS
        match chunk_data.chunk_stage {
            ChunkStage::RubOs | ChunkStage::Os => {
                let chunk = db
                    .persist_partition(table_name, partition_key, true)
                    .await
                    .unwrap()
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // ----------
        // Unload RUB
        if let ChunkStage::Os = chunk_data.chunk_stage {
            db.unload_read_buffer(table_name, partition_key, chunk_id)
                .unwrap();
        }
    }

    // ----------
    // Apply all delete predicates
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone()))
            .await
            .unwrap();
    }

    // Scenario of the input chunks and delete predicates
    let scenario_name = format!(
        "Deleted data from {} chunks, {}, with {} deletes after all chunks are created",
        data.len(),
        display,
        preds.len()
    );
    DbScenario { scenario_name, db }
}
