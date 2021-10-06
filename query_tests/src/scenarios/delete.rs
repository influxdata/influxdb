//! This module contains testing scenarios for Delete

use data_types::chunk_metadata::ChunkId;
use data_types::timestamp::TimestampRange;
use predicate::delete_expr::DeleteExpr;
use predicate::delete_predicate::DeletePredicate;

use async_trait::async_trait;
use query::QueryChunk;
use std::fmt::Display;
use std::sync::Arc;

use server::db::test_helpers::write_lp;
use server::utils::make_db;

use super::{DbScenario, DbSetup};

// =========================================================================================================================
// DELETE TEST SETUPS: chunk lp data, how many chunks, their types, how many delete predicates and when they happen

#[derive(Debug)]
/// Setup for delete query test with one table and one chunk moved from MUB to RUB to OS
/// All data will be soft deleted in this setup
pub struct OneDeleteSimpleExprOneChunkDeleteAll {}
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunkDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        // chunk data
        let lp_lines = vec!["cpu bar=1 10", "cpu bar=2 20"];

        // delete predicate
        let pred = DeletePredicate {
            range: TimestampRange { start: 10, end: 20 },
            exprs: vec![],
        };

        // this returns 15 scenarios
        all_delete_scenarios_for_one_chunk(vec![&pred], vec![], lp_lines, table_name, partition_key)
            .await
    }
}

#[derive(Debug)]
/// Setup for delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct OneDeleteSimpleExprOneChunk {}
#[async_trait]
impl DbSetup for OneDeleteSimpleExprOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        // chunk data
        let lp_lines = vec!["cpu bar=1 10", "cpu bar=2 20"];

        // delete predicate
        let pred = DeletePredicate {
            range: TimestampRange { start: 0, end: 15 },
            exprs: vec![DeleteExpr::new(
                "bar".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::F64((1.0).into()),
            )],
        };

        // this returns 15 scenarios
        all_delete_scenarios_for_one_chunk(vec![&pred], vec![], lp_lines, table_name, partition_key)
            .await
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct OneDeleteMultiExprsOneChunk {}
#[async_trait]
impl DbSetup for OneDeleteMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        // chunk data
        let lp_lines = vec![
            "cpu,foo=me bar=1 10",
            "cpu,foo=you bar=2 20",
            "cpu,foo=me bar=1 30",
            "cpu,foo=me bar=1 40",
        ];
        // delete predicate
        let pred = DeletePredicate {
            range: TimestampRange { start: 0, end: 30 },
            exprs: vec![
                DeleteExpr::new(
                    "bar".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::F64((1.0).into()),
                ),
                DeleteExpr::new(
                    "foo".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::String("me".to_string()),
                ),
            ],
        };

        // this returns 15 scenarios
        all_delete_scenarios_for_one_chunk(vec![&pred], vec![], lp_lines, table_name, partition_key)
            .await
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
/// Two deletes at different chunk stages
pub struct TwoDeletesMultiExprsOneChunk {}
#[async_trait]
impl DbSetup for TwoDeletesMultiExprsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in MUB and
        // is moved with chunk moving. Then one more delete after moving

        // General setup for all scenarios
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        // chunk data
        let lp_lines = vec![
            "cpu,foo=me bar=1 10",
            "cpu,foo=you bar=2 20",
            "cpu,foo=me bar=1 30",
            "cpu,foo=me bar=1 40",
        ];
        // delete predicate
        // pred1: delete from cpu where 0 <= time <= 32 and bar = 1 and foo = 'me'
        let pred1 = DeletePredicate {
            range: TimestampRange { start: 0, end: 32 },
            exprs: vec![
                DeleteExpr::new(
                    "bar".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::F64((1.0).into()),
                ),
                DeleteExpr::new(
                    "foo".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::String("me".to_string()),
                ),
            ],
        };

        // pred2: delete from cpu where 10 <= time <= 40 and bar != 1
        let pred2 = DeletePredicate {
            range: TimestampRange { start: 10, end: 40 },
            exprs: vec![DeleteExpr::new(
                "bar".to_string(),
                predicate::delete_expr::Op::Ne,
                predicate::delete_expr::Scalar::F64((1.0).into()),
            )],
        };

        // build scenarios
        all_delete_scenarios_for_one_chunk(
            vec![&pred1],
            vec![&pred2],
            lp_lines,
            table_name,
            partition_key,
        )
        .await
    }
}

// Three different delete on three different chunks
#[derive(Debug)]
/// Setup for three different delete on three different chunks
pub struct ThreeDeleteThreeChunks {}
#[async_trait]
impl DbSetup for ThreeDeleteThreeChunks {
    async fn make(&self) -> Vec<DbScenario> {
        // General setup for all scenarios
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        // chunk1 data
        let lp_lines_1 = vec![
            "cpu,foo=me bar=1 10",  // deleted by pred1
            "cpu,foo=you bar=2 20", // deleted by pred2
            "cpu,foo=me bar=1 30",  // deleted by pred1
            "cpu,foo=me bar=1 40",
        ];
        // delete predicate on chunk 1
        let pred1 = DeletePredicate {
            range: TimestampRange { start: 0, end: 30 },
            exprs: vec![
                DeleteExpr::new(
                    "bar".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::F64((1.0).into()),
                ),
                DeleteExpr::new(
                    "foo".to_string(),
                    predicate::delete_expr::Op::Eq,
                    predicate::delete_expr::Scalar::String("me".to_string()),
                ),
            ],
        };

        //chunk 2 data
        let lp_lines_2 = vec![
            "cpu,foo=me bar=1 42",
            "cpu,foo=you bar=3 42", // deleted by pred2
            "cpu,foo=me bar=4 50",
            "cpu,foo=me bar=5 60",
        ];
        // delete predicate on chunk 1 & chunk 2
        let pred2 = DeletePredicate {
            range: TimestampRange { start: 20, end: 45 },
            exprs: vec![DeleteExpr::new(
                "foo".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::String("you".to_string()),
            )],
        };

        // chunk 3 data
        let lp_lines_3 = vec![
            "cpu,foo=me bar=1 62",
            "cpu,foo=you bar=3 70",
            "cpu,foo=me bar=7 80",
            "cpu,foo=me bar=8 90", // deleted by pred3
        ];
        // delete predicate on chunk 3
        let pred3 = DeletePredicate {
            range: TimestampRange { start: 75, end: 95 },
            exprs: vec![DeleteExpr::new(
                "bar".to_string(),
                predicate::delete_expr::Op::Ne,
                predicate::delete_expr::Scalar::F64((7.0).into()),
            )],
        };

        // ----------------------
        // 3 chunks: MUB, RUB, OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_stage: ChunkStage::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_stage: ChunkStage::Mubo,
            },
        ];
        let preds = vec![&pred1, &pred2, &pred3];
        let scenario_mub_rub_os = make_different_stage_chunks_with_deletes_scenario(
            lp,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // ----------------------
        // 3 chunks: 1 MUB open, 1 MUB frozen, 1 RUB
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_stage: ChunkStage::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_stage: ChunkStage::Mubf,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_stage: ChunkStage::Mubo,
            },
        ];
        let scenario_2mub_rub = make_different_stage_chunks_with_deletes_scenario(
            lp,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // ----------------------
        // 3 chunks: 2 MUB, 1 OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_stage: ChunkStage::Mubf,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_stage: ChunkStage::Mubo,
            },
        ];
        let scenario_2mub_os = make_different_stage_chunks_with_deletes_scenario(
            lp,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // ----------------------
        // 3 chunks: 2 RUB, 1 OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_stage: ChunkStage::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_stage: ChunkStage::Rub,
            },
        ];
        let scenario_2rub_os = make_different_stage_chunks_with_deletes_scenario(
            lp,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // ----------------------
        // 3 chunks:  RUB, 2 OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_stage: ChunkStage::Rub,
            },
        ];
        let scenario_rub_2os = make_different_stage_chunks_with_deletes_scenario(
            lp,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // ----------------------
        // 3 chunks:  3 OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1,
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2,
                chunk_stage: ChunkStage::Os,
            },
            ChunkData {
                lp_lines: lp_lines_3,
                chunk_stage: ChunkStage::Os,
            },
        ];
        let scenario_3os =
            make_different_stage_chunks_with_deletes_scenario(lp, preds, table_name, partition_key)
                .await;

        // return scenarios to run queries
        vec![
            scenario_mub_rub_os,
            scenario_2mub_rub,
            scenario_2mub_os,
            scenario_2rub_os,
            scenario_rub_2os,
            scenario_3os,
        ]
    }
}

// =========================================================================================================================
// Structs, enums, and functions used to exhaust all test scenarios of chunk life cycle & when delete predicates are applied

// STRUCTs & ENUMs
#[derive(Debug, Clone)]
pub struct ChunkData<'a> {
    /// Line protocol data of this chunk
    lp_lines: Vec<&'a str>,
    /// which stage this chunk will be created
    chunk_stage: ChunkStage,
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

// ------------------------------------------------------------------------------------------------------------------------
// MAJOR FUNCTIONS TO GET INVOKED IN TEST SETUPS

/// Exhaust tests of chunk stages and their life cycle moves for given set of delete predicates
async fn all_delete_scenarios_for_one_chunk(
    // These delete predicates are applied at all stages of the chunk life cycle
    chunk_stage_preds: Vec<&DeletePredicate>,
    // These delete predicates are applied all chunks at their final stages
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
        for delete_time in DeleteTime::all_from_and_before(chunk_stage.clone()) {
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
async fn make_chunk_with_deletes_at_different_stages(
    lp_lines: Vec<&str>,
    chunk_stage: ChunkStage,
    preds: Vec<Pred<'_>>,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;

    // ----------
    // Make an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // 0 does not represent the real chunk id. It is here just to initialize the chunk_id  variable for later assignment
    let mut chunk_id = ChunkId::new(0);
    // Apply delete predicate
    let mut deleted = false;
    let mut display = "".to_string();
    let mut count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Mubo {
            db.delete(table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            deleted = true;
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from open MUB", count).as_str());
    }

    // ----------
    // Freeze MUB if requested
    // Since mub are frozen at delete, no need to do it in that case
    if !deleted {
        match chunk_stage {
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
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Mubf {
            db.delete(table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from frozen MUB", count).as_str());
    }

    // ----------
    // Move MUB to RUB
    match chunk_stage {
        ChunkStage::Rub | ChunkStage::RubOs | ChunkStage::Os => {
            let chunk_result = db
                .compact_chunks(table_name, partition_key, |chunk| chunk.id() == chunk_id)
                .await
                .unwrap();

            match chunk_result {
                Some(chunk) => {
                    chunk_id = chunk.id();
                }
                None => {
                    // MUB has all soft deleted data, no RUB will be created
                    // no more data to affect further delete
                    let scenario_name =
                        format!("Deleted data from one {} chunk{}", chunk_stage, display);
                    return DbScenario { scenario_name, db };
                }
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Rub {
            db.delete(table_name, Arc::new(pred.predicate.clone()))
                .await
                .unwrap();
            count += 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from RUB", count).as_str());
    }

    // ----------
    // Move RUB to OS
    match chunk_stage {
        ChunkStage::RubOs | ChunkStage::Os => {
            let chunk_result = db
                .persist_partition(table_name, partition_key, true)
                .await
                .unwrap();

            match chunk_result {
                Some(chunk) => {
                    chunk_id = chunk.id();
                }
                None => {
                    // RUB has all soft deleted data, no OS will be created
                    // no more data to affect further delete
                    let scenario_name =
                        format!("Deleted data from one {} chunk{}", chunk_stage, display);
                    return DbScenario { scenario_name, db };
                }
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::RubOs {
            db.delete(table_name, Arc::new(pred.predicate.clone()))
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
        db.unload_read_buffer(table_name, partition_key, chunk_id)
            .unwrap();
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTime::Os || pred.delete_time == DeleteTime::End {
            db.delete(table_name, Arc::new(pred.predicate.clone()))
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
async fn make_different_stage_chunks_with_deletes_scenario(
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
        let mut chunk_id = ChunkId::new(0);

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
