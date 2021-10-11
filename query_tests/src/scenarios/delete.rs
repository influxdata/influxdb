//! This module contains testing scenarios for Delete

use data_types::timestamp::TimestampRange;
use predicate::delete_expr::DeleteExpr;
use predicate::delete_predicate::DeletePredicate;

use async_trait::async_trait;

use crate::scenarios::util::{ChunkData, ChunkStage, all_delete_scenarios_for_one_chunk, make_different_stage_chunks_with_deletes_scenario};

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
/// Setup for many scenario move chunk from from MUB to RUB to OS
/// No delete in this case
pub struct NoDeleteOneChunk {}
#[async_trait]
impl DbSetup for NoDeleteOneChunk {
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

        // this returns 15 scenarios
        all_delete_scenarios_for_one_chunk(vec![], vec![], lp_lines, table_name, partition_key)
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

        // build all possible scenarios
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

