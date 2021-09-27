//! This module contains testing scenarios for Delete

use data_types::chunk_metadata::ChunkId;
use datafusion::logical_plan::{col, lit};
use predicate::predicate::{Predicate, PredicateBuilder};

use async_trait::async_trait;
use query::QueryChunk;
use std::fmt::Display;
//use server::Db;
//use core::slice::SlicePattern;
use std::sync::Arc;
use std::time::{Duration, Instant};

use server::db::test_helpers::write_lp;
use server::utils::make_db;

use super::{DbScenario, DbSetup};

#[derive(Debug)]
/// Setup for delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct DeleteFromMubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteFromMubOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the delete predicate is added in MUB and
        // is moved with chunk moving

        // General setup for all scenarios
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        // chunk data
        let lp_lines = vec!["cpu bar=1 10", "cpu bar=2 20"];
        // delete predicate
        let i: f64 = 1.0;
        let expr = col("bar").eq(lit(i));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 15)
            .add_expr(expr)
            .build();

        // delete happens when data in open MUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Mubo,
        }];
        let scenario_mub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Mubo,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in open MUB then moved to RUB
        let scenario_mub_to_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in open MUB then moved to RUB and then persisted
        let scenario_mub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB, then persisted, and then RUB is unloaded
        let scenario_mub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![
            scenario_mub,
            scenario_mub_to_rub,
            scenario_mub_to_rub_os,
            scenario_mub_to_os,
        ]
    }
}

#[derive(Debug)]
/// Setup for delete query test with one table and one chunk moved from RUB to OS
pub struct DeleteFromRubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteFromRubOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the delete predicate is added in RUB
        // and is moved with chunk moving

        // General setup for all scenarios
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        // chunk data
        let lp_lines = vec!["cpu bar=1 10", "cpu bar=2 20"];
        // delete predicate
        let i: f64 = 1.0;
        let expr = col("bar").eq(lit(i));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 15)
            .add_expr(expr)
            .build();

        // delete happens to data in RUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Rub,
        }];
        let scenario_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens to data in RUB then persisted
        let scenario_rub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens to data in RUB then persisted then RUB unloaded
        let scenario_rub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub, scenario_rub_to_rub_os, scenario_rub_to_os]
    }
}

#[derive(Debug)]
/// Setup for delete query test with one table and one chunk in both RUB and OS
pub struct DeleteFromOsOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteFromOsOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the delete predicate is added to persisted chunks

        // General setup for all scenarios
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";
        // chunk data
        let lp_lines = vec!["cpu bar=1 10", "cpu bar=2 20"];
        // delete predicate
        let i: f64 = 1.0;
        let expr = col("bar").eq(lit(i));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 15)
            .add_expr(expr)
            .build();

        // delete happens after data is persisted but still in RUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::RubOs,
        }];
        let scenario_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted but still in RUB and then unload RUB
        let scenario_rub_os_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted and RUB is unloaded
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Os,
        }];
        let scenario_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub_os, scenario_rub_os_to_os, scenario_os]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct DeleteMultiExprsFromMubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteMultiExprsFromMubOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in MUB and
        // is moved with chunk moving

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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        // delete happens when data in open MUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Mubo,
        }];
        let scenario_mub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Mubo,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in open MUB then moved to RUB
        let scenario_mub_to_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in open MUB then moved to RUB and then persisted
        let scenario_mub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB, then persisted, and then RUB is unloaded
        let scenario_mub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![
            scenario_mub,
            scenario_mub_to_rub,
            scenario_mub_to_rub_os,
            scenario_mub_to_os,
        ]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct DeleteMultiExprsFromRubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteMultiExprsFromRubOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in MUB and
        // is moved with chunk moving

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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        // delete happens to data in RUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Rub,
        }];
        let scenario_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens to data in RUB then persisted
        let scenario_rub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens to data in RUB then persisted then RUB unloaded
        let scenario_rub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub, scenario_rub_to_rub_os, scenario_rub_to_os]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
pub struct DeleteMultiExprsFromOsOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for DeleteMultiExprsFromOsOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in MUB and
        // is moved with chunk moving

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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        // delete happens after data is persisted but still in RUB
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::RubOs,
        }];
        let scenario_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted but still in RUB and then unload RUB
        let scenario_rub_os_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted and RUB is unloaded
        let preds = vec![Pred {
            predicate: &pred,
            delete_time: DeleteTime::Os,
        }];
        let scenario_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub_os, scenario_rub_os_to_os, scenario_os]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from MUB to RUB to OS
/// Two deletes at different chunk stages
pub struct TwoDeleteMultiExprsFromMubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for TwoDeleteMultiExprsFromMubOneMeasurementOneChunk {
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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred1 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        let expr3 = col("bar").not_eq(lit(i));
        let pred2 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(10, 45)
            .add_expr(expr3)
            .build();

        // delete happens when data in MUB
        let preds = vec![
            Pred {
                predicate: &pred1,
                delete_time: DeleteTime::Mubo,
            },
            Pred {
                predicate: &pred2,
                delete_time: DeleteTime::End,
            },
        ];
        let scenario_mub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Mubo,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB
        let scenario_mub_to_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB and then persisted
        let scenario_mub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB, then persisted, and then RUB is unloaded
        let scenario_mub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![
            scenario_mub,
            scenario_mub_to_rub,
            scenario_mub_to_rub_os,
            scenario_mub_to_os,
        ]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk moved from RUB to OS
/// Two deletes at different chunk stages
pub struct TwoDeleteMultiExprsFromRubOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for TwoDeleteMultiExprsFromRubOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in RUB and
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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred1 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        let expr3 = col("bar").not_eq(lit(i));
        let pred2 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(10, 45)
            .add_expr(expr3)
            .build();

        // delete happens when data in RUB
        let preds = vec![
            Pred {
                predicate: &pred1,
                delete_time: DeleteTime::Rub,
            },
            Pred {
                predicate: &pred2,
                delete_time: DeleteTime::End,
            },
        ];
        let scenario_rub = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Rub,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in RUB then moved to OS but still in RUB
        let scenario_rub_to_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in RUB then moved to RUB and then persisted, then unload RUB
        let scenario_rub_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub, scenario_rub_to_rub_os, scenario_rub_to_os]
    }
}

#[derive(Debug)]
/// Setup for multi-expression delete query test with one table and one chunk in OS
pub struct TwoDeleteMultiExprsFromOsOneMeasurementOneChunk {}
#[async_trait]
impl DbSetup for TwoDeleteMultiExprsFromOsOneMeasurementOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        // The main purpose of these scenarios is the multi-expression delete predicate is added in OS twice

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
        let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(i));
        let expr2 = col("foo").eq(lit("me"));
        let pred1 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        let expr3 = col("bar").not_eq(lit(i));
        let pred2 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(10, 45)
            .add_expr(expr3)
            .build();

        // delete happens after data is persisted but still in RUB
        let preds = vec![
            Pred {
                predicate: &pred1,
                delete_time: DeleteTime::RubOs,
            },
            Pred {
                predicate: &pred2,
                delete_time: DeleteTime::End,
            },
        ];
        let scenario_rub_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::RubOs,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted but still in RUB and then unload RUB
        let scenario_rub_os_to_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted and unload RUB
        let preds = vec![
            Pred {
                predicate: &pred1,
                delete_time: DeleteTime::End,
            },
            Pred {
                predicate: &pred2,
                delete_time: DeleteTime::End,
            },
        ];
        let scenario_os = make_chunk_with_deletes_at_different_stages(
            lp_lines.clone(),
            ChunkType::Os,
            preds.clone(),
            table_name,
            partition_key,
        )
        .await;

        // return scenarios to run queries
        vec![scenario_rub_os, scenario_rub_os_to_os, scenario_os]
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
        //let i: f64 = 1.0;
        let expr1 = col("bar").eq(lit(1f64));
        let expr2 = col("foo").eq(lit("me"));
        let pred1 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(0, 32)
            .add_expr(expr1)
            .add_expr(expr2)
            .build();

        //chunk 2 data
        let lp_lines_2 = vec![
            "cpu,foo=me bar=1 42",
            "cpu,foo=you bar=3 42", // deleted by pred2
            "cpu,foo=me bar=4 50",
            "cpu,foo=me bar=5 60",
        ];
        // delete predicate on chunk 1 & chunk 2
        let expr = col("foo").eq(lit("you"));
        let pred2 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(20, 45)
            .add_expr(expr)
            .build();

        // chunk 3 data
        let lp_lines_3 = vec![
            "cpu,foo=me bar=1 62",
            "cpu,foo=you bar=3 70",
            "cpu,foo=me bar=7 80",
            "cpu,foo=me bar=8 90", // deleted by pred3
        ];
        // delete predicate on chunk 3
        let i: f64 = 7.0;
        let expr = col("bar").not_eq(lit(i));
        let pred3 = PredicateBuilder::new()
            .table("cpu")
            .timestamp_range(75, 95)
            .add_expr(expr)
            .build();

        // ----------------------
        // 3 chunks: MUB, RUB, OS
        let lp = vec![
            ChunkData {
                lp_lines: lp_lines_1.clone(),
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_type: ChunkType::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_type: ChunkType::Mubo,
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
                chunk_type: ChunkType::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_type: ChunkType::Mubf,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_type: ChunkType::Mubo,
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
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_type: ChunkType::Mubf,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_type: ChunkType::Mubo,
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
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_type: ChunkType::Rub,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_type: ChunkType::Rub,
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
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2.clone(),
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_3.clone(),
                chunk_type: ChunkType::Rub,
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
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_2,
                chunk_type: ChunkType::Os,
            },
            ChunkData {
                lp_lines: lp_lines_3,
                chunk_type: ChunkType::Os,
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

// -----------------------------------------------------------------------------
// Helper structs and functions

#[derive(Debug, Clone)]
pub struct ChunkData<'a> {
    lp_lines: Vec<&'a str>,
    chunk_type: ChunkType,
}

#[derive(Debug, Clone)]
pub enum ChunkType {
    Mubo, // open MUB
    Mubf, // frozen MUB
    Rub,
    RubOs,
    Os,
}

impl Display for ChunkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkType::Mubo => write!(f, "Open MUB"),
            ChunkType::Mubf => write!(f, "Frozen MUB"),
            ChunkType::Rub => write!(f, "RUB"),
            ChunkType::RubOs => write!(f, "RUB & OS"),
            ChunkType::Os => write!(f, "OS"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Pred<'a> {
    predicate: &'a Predicate,
    delete_time: DeleteTime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeleteTime {
    /// delete predicate happens after all chunks created
    /// and moved to their corresponding stages
    End,

    /// delete predicate applies to chunks at their Mub Open stage
    Mubo,

    /// delete predicate applies to chunks at their Mub Frozen stage
    Mubf,

    /// delete predicate applies to chunks at their Rub stage
    Rub,

    /// delete predicate applies to chunks at their Rub & Os stage
    RubOs,

    /// delete predicate applies to chunks at their Os stage
    Os,
}

/// Build a chunk that may move with life cycle before/after deletes
///  Note the chunk in this function can be moved to different stages and delete predicates
///  can be applied at different stages when the chunk is moved.
async fn make_chunk_with_deletes_at_different_stages(
    lp_lines: Vec<&str>,
    chunk_type: ChunkType,
    preds: Vec<Pred<'_>>,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;

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

    // Freeze MUB is requested
    // Since mub are frozen at delete, no need to do it in that case
    if !deleted {
        match chunk_type {
            ChunkType::Mubf | ChunkType::Rub | ChunkType::RubOs | ChunkType::Os => {
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

    // Move MUB to RUB
    match chunk_type {
        ChunkType::Rub | ChunkType::RubOs | ChunkType::Os => {
            let chunk = db
                .move_chunk_to_read_buffer(table_name, partition_key, chunk_id)
                .await
                .unwrap();
            chunk_id = chunk.id();
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

    // Move RUB to OS
    match chunk_type {
        ChunkType::RubOs | ChunkType::Os => {
            let chunk = db
                .persist_partition(
                    table_name,
                    partition_key,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
                .unwrap();
            chunk_id = chunk.id();
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

    // Unload RUB
    if let ChunkType::Os = chunk_type {
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
                ", with {} deletes from OS or after all chunks created",
                count
            )
            .as_str(),
        );
    }

    let scenario_name = format!("Deleted data from one chunk{}", display);
    DbScenario { scenario_name, db }
}

/// Build many chunks which are in different stages
// Note that, after a lot of thoughts, I decided to have 2 separated functionsn this one and the one above.
// The above tests delete predicates before and/or after a chunk is moved to different stages, while
// this function tests different-stage chunks in different stages when one or many deletes happen.
// Even though these 2 functions have some overlapped code, merging them in one
// function will created a much more complicated cases to handle
async fn make_different_stage_chunks_with_deletes_scenario(
    data: Vec<ChunkData<'_>>,
    preds: Vec<&Predicate>,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    let mut display = "".to_string();

    //build_different_stage_chunks(&db, data, ChunkType::Mubo, table_name, partition_key).await;
    for chunk_data in &data {
        display.push_str(" - ");
        display.push_str(&chunk_data.chunk_type.to_string());
        // Make an open MUB
        write_lp(&db, &chunk_data.lp_lines.join("\n")).await;
        // 0 does not represent the real chunk id. It is here just to initialize the chunk_id  variable for later assignment
        let mut chunk_id = ChunkId::new(0);

        // freeze MUB
        match chunk_data.chunk_type {
            ChunkType::Mubf | ChunkType::Rub | ChunkType::RubOs | ChunkType::Os => {
                let chunk = db
                    .rollover_partition(table_name, partition_key)
                    .await
                    .unwrap()
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // Move MUB to RUB
        match chunk_data.chunk_type {
            ChunkType::Rub | ChunkType::RubOs | ChunkType::Os => {
                let chunk = db
                    .move_chunk_to_read_buffer(table_name, partition_key, chunk_id)
                    .await
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // Move RUB to OS
        match chunk_data.chunk_type {
            ChunkType::RubOs | ChunkType::Os => {
                let chunk = db
                    .persist_partition(
                        table_name,
                        partition_key,
                        Instant::now() + Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                chunk_id = chunk.id();
            }
            _ => {}
        }

        // Unload RUB
        if let ChunkType::Os = chunk_data.chunk_type {
            db.unload_read_buffer(table_name, partition_key, chunk_id)
                .unwrap();
        }
    }

    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone()))
            .await
            .unwrap();
    }

    let scenario_name = format!(
        "Deleted data from {} chunks, {}, with {} deletes after all chunks are created",
        data.len(),
        display,
        preds.len()
    );
    DbScenario { scenario_name, db }
}
