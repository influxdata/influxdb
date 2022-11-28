//! Library of test scenarios that can be used in query_tests

use super::{
    util::{
        all_scenarios_for_one_chunk, make_n_chunks_scenario_with_retention,
        make_two_chunk_scenarios, ChunkStage,
    },
    DbScenario, DbSetup,
};
use crate::scenarios::util::{make_n_chunks_scenario, ChunkData};
use async_trait::async_trait;
use iox_query::frontend::sql::SqlQueryPlanner;
use iox_time::{MockProvider, Time, TimeProvider};

#[derive(Debug)]
pub struct MeasurementWithMaxTime {}
#[async_trait]
impl DbSetup for MeasurementWithMaxTime {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2262-04-11T23";

        // This is the maximum timestamp that can be represented in the InfluxDB data model:
        // https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34
        let max_nano_time = i64::MAX - 1; // 9223372036854775806

        let lp_lines = vec![format!("cpu,host=server01 value=100 {}", max_nano_time)];
        let lp_lines = lp_lines.iter().map(|s| s.as_str()).collect();

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

/// a measurement with timestamps in 2021
#[derive(Debug)]
pub struct OneMeasurementRealisticTimes {}
#[async_trait]
impl DbSetup for OneMeasurementRealisticTimes {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-07-20T19";

        let lp_lines = vec![
            "cpu,region=west user=23.2 1626809330000000000",
            "cpu,region=west user=21.0 1626809430000000000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

#[derive(Debug)]
pub struct OneMeasurementNoTags {}
#[async_trait]
impl DbSetup for OneMeasurementNoTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o temp=70.4 100",
            "h2o temp=72.4 250",
            "h2o temp=50.4 200",
            "h2o level=200.0 300",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyNullTags {}
#[async_trait]
impl DbSetup for OneMeasurementManyNullTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=CA,city=LA,county=LA temp=70.4 100",
            "h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250",
            "h2o,state=MA,city=Boston temp=50.4 200",
            "h2o,state=CA temp=79.0 300",
            "h2o,state=NY temp=60.8 400",
            "h2o,state=NY,city=NYC temp=61.0 500",
            "h2o,state=NY,city=NYC,borough=Brooklyn temp=61.0 600",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

/// Two measurements data in different chunk scenarios
#[derive(Debug)]
pub struct TwoMeasurements {}
#[async_trait]
impl DbSetup for TwoMeasurements {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsUnsignedType {}
#[async_trait]
impl DbSetup for TwoMeasurementsUnsignedType {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "restaurant,town=andover count=40000u 100",
            "restaurant,town=reading count=632u 120",
            "school,town=reading count=17u 150",
            "school,town=andover count=25u 160",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "restaurant", partition_key).await
    }
}

#[derive(Debug)]
pub struct AllTypes {}
#[async_trait]
impl DbSetup for AllTypes {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // min and max times are purposely not the first or last in the input
        let lp_lines = vec![
            // ensure all rows have at least one null
            "m,tag=row1 float_field=64.0 450",
            "m,tag=row1 int_field=64 550",
            "m,tag=row1 float_field=61.0,int_field=22,uint_field=25u,string_field=\"foo\",bool_field=t 500",
            "m,tag=row1 float_field=62.0,int_field=21,uint_field=30u,string_field=\"ba\",bool_field=f 200",
            "m,tag=row1 float_field=63.0,int_field=20,uint_field=35u,string_field=\"baz\",bool_field=f 300",
            "m,tag=row1 float_field=64.0,int_field=19,uint_field=20u,string_field=\"bar\",bool_field=t 400",
            "m,tag=row1 float_field=65.0,int_field=18,uint_field=40u,string_field=\"fruz\",bool_field=f 100",
            "m,tag=row1 float_field=66.0,int_field=17,uint_field=10u,string_field=\"faa\",bool_field=t 600",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "m", partition_key).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsPredicatePushDown {}
#[async_trait]
impl DbSetup for TwoMeasurementsPredicatePushDown {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "restaurant,town=andover count=40000u,system=5.0 100",
            "restaurant,town=reading count=632u,system=5.0 120",
            "restaurant,town=bedford count=189u,system=7.0 110",
            "restaurant,town=tewsbury count=471u,system=6.0 110",
            "restaurant,town=lexington count=372u,system=5.0 100",
            "restaurant,town=lawrence count=872u,system=6.0 110",
            "restaurant,town=reading count=632u,system=6.0 130",
        ];
        let lp_lines2 = vec![
            "school,town=reading count=17u,system=6.0 150",
            "school,town=andover count=25u,system=6.0 160",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

/// Single measurement that has several different chunks with
/// different (but compatible) schema
#[derive(Debug)]
pub struct MultiChunkSchemaMerge {}
#[async_trait]
impl DbSetup for MultiChunkSchemaMerge {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "cpu,region=west user=23.2,system=5.0 100",
            "cpu,region=west user=21.0,system=6.0 150",
        ];
        let lp_lines2 = [
            "cpu,region=east,host=foo user=23.2 100",
            "cpu,region=west,host=bar user=21.0 250",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

/// Two measurements data with many null values
#[derive(Debug)]
pub struct TwoMeasurementsManyNulls {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyNulls {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=CA,city=LA,county=LA temp=70.4 100",
            "h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250",
            "o2,state=MA,city=Boston temp=50.4 200",
            "o2,state=CA temp=79.0 300",
        ];
        let lp_lines2 = [
            "o2,state=NY temp=60.8 400",
            "o2,state=NY,city=NYC temp=61.0 500",
            "o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsManyFields {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFields {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];
        let lp_lines2 = vec!["h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000"];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

#[derive(Debug)]
/// This has a single chunk for queries that check the state of the system
pub struct TwoMeasurementsManyFieldsOneChunk {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsOneChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[derive(Debug)]
/// This has two chunks for queries that check the state of the system
///
/// This scenario can be used for `EXPLAIN` plans and system tables.
pub struct TwoMeasurementsManyFieldsTwoChunks {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsTwoChunks {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
        ];

        let lp_lines2 = vec![
            "h2o,state=CA,city=Boston other_temp=72.4 150",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];

        let scenarios = make_n_chunks_scenario(&[
            ChunkData {
                lp_lines: lp_lines1,
                partition_key,
                chunk_stage: Some(ChunkStage::Parquet),
                ..Default::default()
            },
            ChunkData {
                lp_lines: lp_lines2,
                partition_key,
                chunk_stage: Some(ChunkStage::Ingester),
                ..Default::default()
            },
        ])
        .await;

        // run a test query on each DB
        for scenario in &scenarios {
            let query = "SELECT 1;";
            let planner = SqlQueryPlanner::default();
            let ctx = scenario.db.new_query_context(None);
            let physical_plan = planner
                .query(query, &ctx)
                .await
                .expect("built plan successfully");
            let mut query_completed_token =
                scenario
                    .db
                    .record_query(&ctx, "sql", Box::new(String::from(query)));
            ctx.collect(physical_plan).await.expect("Running plan");
            query_completed_token.set_success()
        }

        scenarios
    }
}

#[derive(Debug)]
/// This has several chunks that represent differnt chunk stage and overlaps for EXPLAIN plans
pub struct ManyFieldsSeveralChunks {}
#[async_trait]
impl DbSetup for ManyFieldsSeveralChunks {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // c1: parquet stage
        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250", // duplicate with a row in c4 and will be removed
        ];
        let c1 = ChunkData {
            lp_lines: lp_lines1,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };

        // c2: parquet stage & overlaps with c1
        let lp_lines2 = vec!["h2o,state=CA,city=Andover other_temp=72.4 150"];
        let c2 = ChunkData {
            lp_lines: lp_lines2,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };

        // c3: parquet stage & not overlap with any
        let lp_lines3 = vec![
            "h2o,state=MA,city=Boston temp=80.7 350",
            "h2o,state=MA,city=Boston other_temp=68.2 450",
        ];
        let c3 = ChunkData {
            lp_lines: lp_lines3,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };

        // c4: parquet stage & overlap with c1
        let lp_lines4 = vec![
            "h2o,state=MA,city=Boston temp=88.6 230",
            "h2o,state=MA,city=Boston other_temp=80 250", // duplicate with a row in c1 but more
                                                          // recent => this row is kept
        ];
        let c4 = ChunkData {
            lp_lines: lp_lines4,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };

        // c5: ingester stage & not overlaps with any
        let lp_lines5 = vec!["h2o,state=CA,city=Andover temp=67.3 500"];
        let c5 = ChunkData {
            lp_lines: lp_lines5,
            partition_key,
            chunk_stage: Some(ChunkStage::Ingester),
            ..Default::default()
        };

        make_n_chunks_scenario(&[c1, c2, c3, c4, c5]).await
    }
}

#[derive(Debug)]
/// This has two chunks with different tag/key sets for queries whose columns not include keys
pub struct OneMeasurementTwoChunksDifferentTagSet {}
#[async_trait]
impl DbSetup for OneMeasurementTwoChunksDifferentTagSet {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // tag: state
        let lp_lines1 = vec![
            "h2o,state=MA temp=70.4 50",
            "h2o,state=MA other_temp=70.4 250",
        ];

        // tag: city
        let lp_lines2 = vec![
            "h2o,city=Boston other_temp=72.4 350",
            "h2o,city=Boston temp=53.4,reading=51 50",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

fn one_measurement_four_chunks_with_duplicates() -> Vec<ChunkData<'static, 'static>> {
    let partition_key = "1970-01-01T00";

    // Chunk 1:
    //  . time range: 50-250
    //  . no duplicates in its own chunk
    let lp_lines1 = vec![
        "h2o,state=MA,city=Boston min_temp=70.4 50",
        "h2o,state=MA,city=Bedford min_temp=71.59 150",
        "h2o,state=MA,city=Boston max_temp=75.4 250",
        "h2o,state=MA,city=Andover max_temp=69.2, 250",
    ];

    // Chunk 2: overlaps with chunk 1
    //  . time range: 150 - 300
    //  . no duplicates in its own chunk
    let lp_lines2 = vec![
        // new field (area) and update available NULL (max_temp)
        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150",
        "h2o,state=MA,city=Boston min_temp=65.4 250", // update min_temp from NULL
        "h2o,state=MA,city=Reading min_temp=53.4, 250",
        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
    ];

    // Chunk 3: no overlap
    //  . time range: 400 - 500
    //  . duplicates in its own chunk
    let lp_lines3 = vec![
        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
        "h2o,state=MA,city=Boston min_temp=68.4 400",
        "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
    ];

    // Chunk 4: no overlap
    //  . time range: 600 - 700
    //  . no duplicates
    let lp_lines4 = vec![
        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
        "h2o,state=MA,city=Boston min_temp=67.4 600",
        "h2o,state=MA,city=Reading min_temp=60.4, 600",
        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
    ];

    vec![
        ChunkData {
            lp_lines: lp_lines1,
            partition_key,
            ..Default::default()
        },
        ChunkData {
            lp_lines: lp_lines2,
            partition_key,
            ..Default::default()
        },
        ChunkData {
            lp_lines: lp_lines3,
            partition_key,
            ..Default::default()
        },
        ChunkData {
            lp_lines: lp_lines4,
            partition_key,
            ..Default::default()
        },
    ]
}

/// Setup for four chunks with duplicates for deduplicate tests
#[derive(Debug)]
pub struct OneMeasurementFourChunksWithDuplicates {}
#[async_trait]
impl DbSetup for OneMeasurementFourChunksWithDuplicates {
    async fn make(&self) -> Vec<DbScenario> {
        make_n_chunks_scenario(&one_measurement_four_chunks_with_duplicates()).await
    }
}

/// Setup for four chunks with duplicates for deduplicate tests.
///
/// This is identical to [`OneMeasurementFourChunksWithDuplicates`] but only uses parquet files so it can be used for
/// `EXPLAIN` plans.
#[derive(Debug)]
pub struct OneMeasurementFourChunksWithDuplicatesParquetOnly {}
#[async_trait]
impl DbSetup for OneMeasurementFourChunksWithDuplicatesParquetOnly {
    async fn make(&self) -> Vec<DbScenario> {
        let chunk_data: Vec<_> = one_measurement_four_chunks_with_duplicates()
            .into_iter()
            .map(|cd| ChunkData {
                chunk_stage: Some(ChunkStage::Parquet),
                ..cd
            })
            .collect();
        make_n_chunks_scenario(&chunk_data).await
    }
}

/// Setup for four chunks with duplicates for deduplicate tests.
///
/// This is identical to [`OneMeasurementFourChunksWithDuplicates`] but uses ingester data as well so it can be used for
/// `EXPLAIN` plans.
#[derive(Debug)]
pub struct OneMeasurementFourChunksWithDuplicatesWithIngester {}
#[async_trait]
impl DbSetup for OneMeasurementFourChunksWithDuplicatesWithIngester {
    async fn make(&self) -> Vec<DbScenario> {
        let chunks = one_measurement_four_chunks_with_duplicates();
        let n = chunks.len();

        let chunk_data: Vec<_> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, cd)| ChunkData {
                chunk_stage: Some(if i == (n - 1) {
                    ChunkStage::Ingester
                } else {
                    ChunkStage::Parquet
                }),
                ..cd
            })
            .collect();
        make_n_chunks_scenario(&chunk_data).await
    }
}

/// Setup with 20 parquet files, some with duplicated and some without
/// duplicated tags. The idea here is to verify that merging them
/// together produces the correct values
#[derive(Debug)]
pub struct TwentySortedParquetFiles {}
#[async_trait]
impl DbSetup for TwentySortedParquetFiles {
    async fn make(&self) -> Vec<DbScenario> {
        let lp_data: Vec<_> = (0..20)
            .map(|i| {
                if i % 2 == 0 {
                    vec![
                        format!("m,tag=A f=1 {}", 1000 - i), // unique in this chunk
                        format!("m,tab=B f=2 {}", 1000 - i), // unique in this chunk (not plus i!)
                    ]
                } else {
                    vec![
                        format!("m,tag=A f=3 2001"), // duplicated across all chunks
                    ]
                }
            })
            .collect();

        let partition_key = "1970-01-01T00";
        let chunk_data: Vec<_> = lp_data
            .iter()
            .map(|lp_lines| ChunkData {
                lp_lines: lp_lines.iter().map(|s| s.as_str()).collect(),
                partition_key,
                chunk_stage: Some(ChunkStage::Parquet),
                ..Default::default()
            })
            .collect();

        make_n_chunks_scenario(&chunk_data).await
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyFields {}
#[async_trait]
impl DbSetup for OneMeasurementManyFields {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // Order this so field3 comes before field2
        // (and thus the columns need to get reordered)
        let lp_lines = vec![
            "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
            "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
            "h2o,tag1=foo,tag2=bar field1=70.3,field5=false 3000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}
/// This data (from end to end test)
#[derive(Debug)]
pub struct EndToEndTest {}
#[async_trait]
impl DbSetup for EndToEndTest {
    async fn make(&self) -> Vec<DbScenario> {
        let lp_lines = vec![
            "cpu_load_short,host=server01,region=us-west value=0.64 0000",
            "cpu_load_short,host=server01 value=27.99 1000",
            "cpu_load_short,host=server02,region=us-west value=3.89 2000",
            "cpu_load_short,host=server01,region=us-east value=1234567.891011 3000",
            "cpu_load_short,host=server01,region=us-west value=0.000003 4000",
            "system,host=server03 uptime=1303385 5000",
            "swap,host=server01,name=disk0 in=3,out=4 6000",
            "status active=t 7000",
            "attributes color=\"blue\" 8000",
        ];

        let partition_key = "1970-01-01T00";

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu_load_short", partition_key).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsMultiSeries {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiSeries {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let mut lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100", // to row 2
            "h2o,state=MA,city=Boston temp=72.4 250", // to row 1
            "h2o,state=CA,city=LA temp=90.0 200",     // to row 0
            "h2o,state=CA,city=LA temp=90.0 350",     // to row 3
            "o2,state=MA,city=Boston temp=50.4,reading=50 100", // to row 5
            "o2,state=MA,city=Boston temp=53.4,reading=51 250", // to row 4
        ];

        // Swap around data is not inserted in series order
        lp_lines.swap(0, 2);
        lp_lines.swap(4, 5);

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsMultiTagValue {}
#[async_trait]
impl DbSetup for TwoMeasurementsMultiTagValue {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Lowell temp=75.4 100",
            "h2o,state=CA,city=LA temp=90.0 200",
            "o2,state=MA,city=Boston temp=50.4,reading=50 100",
            "o2,state=KS,city=Topeka temp=60.4,reading=60 100",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

pub struct MeasurementStatusCode {}
#[async_trait]
impl DbSetup for MeasurementStatusCode {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2018-05-22T19";

        let lp = vec![
            "status_code,url=http://www.example.com value=404 1527018806000000000",
            "status_code,url=https://influxdb.com value=418 1527018816000000000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "status_code", partition_key).await
    }
}

#[derive(Debug)]
pub struct MeasurementsSortableTags {}
#[async_trait]
impl DbSetup for MeasurementsSortableTags {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
            "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
            "h2o,state=CA,city=Boston temp=70.3 250",
            "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
            "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

// See issue: https://github.com/influxdata/influxdb_iox/issues/2845
#[derive(Debug)]
pub struct MeasurementsForDefect2845 {}
#[async_trait]
impl DbSetup for MeasurementsForDefect2845 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2018-05-22T19";

        let lp_lines = vec![
            "system,host=host.local load1=1.83 1527018806000000000",
            "system,host=host.local load1=1.63 1527018816000000000",
            "system,host=host.local load3=1.72 1527018806000000000",
            "system,host=host.local load4=1.77 1527018806000000000",
            "system,host=host.local load4=1.78 1527018816000000000",
            "system,host=host.local load4=1.77 1527018826000000000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "system", partition_key).await
    }
}

pub struct OneMeasurementNoTags2 {}
#[async_trait]
impl DbSetup for OneMeasurementNoTags2 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let lp_lines = vec!["m0 foo=1.0 1", "m0 foo=2.0 2"];
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "m0", partition_key).await
    }
}

pub struct OneMeasurementForAggs {}
#[async_trait]
impl DbSetup for OneMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        let lp_lines2 = vec![
            "h2o,state=CA,city=LA temp=90.0 200",
            "h2o,state=CA,city=LA temp=90.0 350",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct AnotherMeasurementForAggs {}
#[async_trait]
impl DbSetup for AnotherMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
            "h2o,state=MA,city=Boston temp=70 300",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct TwoMeasurementForAggs {}
#[async_trait]
impl DbSetup for TwoMeasurementForAggs {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        let lp_lines2 = vec![
            "o2,state=CA,city=LA temp=90.0 200",
            "o2,state=CA,city=LA temp=90.0 350",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct MeasurementForSelectors {}
#[async_trait]
impl DbSetup for MeasurementForSelectors {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec!["h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000"];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct MeasurementForMin {}
#[async_trait]
impl DbSetup for MeasurementForMin {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"a\" 2000",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
            "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"c\" 4000",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct MeasurementForMax {}
#[async_trait]
impl DbSetup for MeasurementForMax {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
            "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
            "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
        ];
        let lp_lines2 = vec!["h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"z\" 4000"];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct MeasurementForGroupKeys {}
#[async_trait]
impl DbSetup for MeasurementForGroupKeys {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Cambridge temp=80 50",
            "h2o,state=MA,city=Cambridge temp=81 100",
            "h2o,state=MA,city=Cambridge temp=82 200",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Boston temp=70 300",
            "h2o,state=MA,city=Boston temp=71 400",
            "h2o,state=CA,city=LA temp=90,humidity=10 500",
            "h2o,state=CA,city=LA temp=91,humidity=11 600",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

pub struct MeasurementForGroupByField {}
#[async_trait]
impl DbSetup for MeasurementForGroupByField {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "system,host=local,region=A load1=1.1,load2=2.1 100",
            "system,host=local,region=A load1=1.2,load2=2.2 200",
            "system,host=remote,region=B load1=10.1,load2=2.1 100",
        ];

        let lp_lines2 = vec![
            "system,host=remote,region=B load1=10.2,load2=20.2 200",
            "system,host=local,region=C load1=100.1,load2=200.1 100",
            "aa_system,host=local,region=C load1=100.1,load2=200.1 100",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2691
pub struct MeasurementForDefect2691 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2691 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2018-05-22T19";

        let lp = vec![
            "system,host=host.local load1=1.83 1527018806000000000",
            "system,host=host.local load1=1.63 1527018816000000000",
            "system,host=host.local load3=1.72 1527018806000000000",
            "system,host=host.local load4=1.77 1527018806000000000",
            "system,host=host.local load4=1.78 1527018816000000000",
            "system,host=host.local load4=1.77 1527018826000000000",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "system", partition_key).await
    }
}

pub struct MeasurementForWindowAggregate {}
#[async_trait]
impl DbSetup for MeasurementForWindowAggregate {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines1 = vec![
            "h2o,state=MA,city=Boston temp=70.0 100",
            "h2o,state=MA,city=Boston temp=71.0 200",
            "h2o,state=MA,city=Boston temp=72.0 300",
            "h2o,state=MA,city=Boston temp=73.0 400",
            "h2o,state=MA,city=Boston temp=74.0 500",
            "h2o,state=MA,city=Cambridge temp=80.0 100",
            "h2o,state=MA,city=Cambridge temp=81.0 200",
        ];
        let lp_lines2 = vec![
            "h2o,state=MA,city=Cambridge temp=82.0 300",
            "h2o,state=MA,city=Cambridge temp=83.0 400",
            "h2o,state=MA,city=Cambridge temp=84.0 500",
            "h2o,state=CA,city=LA temp=90.0 100",
            "h2o,state=CA,city=LA temp=91.0 200",
            "h2o,state=CA,city=LA temp=92.0 300",
            "h2o,state=CA,city=LA temp=93.0 400",
            "h2o,state=CA,city=LA temp=94.0 500",
        ];

        make_two_chunk_scenarios(partition_key, &lp_lines1.join("\n"), &lp_lines2.join("\n")).await
    }
}

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2697
pub struct MeasurementForDefect2697 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2697 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm,section=1a bar=5.0 1609459201000000011",
            "mm,section=1a bar=0.28 1609459201000000031",
            "mm,section=2b bar=4.0 1609459201000000009",
            "mm,section=2b bar=6.0 1609459201000000015",
            "mm,section=2b bar=1.2 1609459201000000022",
            "mm,section=1a foo=1.0 1609459201000000001",
            "mm,section=1a foo=3.0 1609459201000000005",
            "mm,section=1a foo=11.24 1609459201000000024",
            "mm,section=2b foo=2.0 1609459201000000002",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "mm", partition_key).await
    }
}

// Test data to validate fix for:
// https://github.com/influxdata/influxdb_iox/issues/2890
pub struct MeasurementForDefect2890 {}
#[async_trait]
impl DbSetup for MeasurementForDefect2890 {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "mm foo=2.0 1609459201000000001",
            "mm foo=2.0 1609459201000000002",
            "mm foo=3.0 1609459201000000005",
            "mm foo=11.24 1609459201000000024",
            "mm bar=4.0 1609459201000000009",
            "mm bar=5.0 1609459201000000011",
            "mm bar=6.0 1609459201000000015",
            "mm bar=1.2 1609459201000000022",
            "mm bar=2.8 1609459201000000031",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "mm", partition_key).await
    }
}

// Test data for retention policy
#[derive(Debug)]
pub struct ThreeChunksWithRetention {}
#[async_trait]
impl DbSetup for ThreeChunksWithRetention {
    async fn make(&self) -> Vec<DbScenario> {
        // Same time provider as the one used in n_chunks scenarios
        let time_provider = MockProvider::new(Time::from_timestamp(0, 0).unwrap());
        let retention_period_1_hour_ns = 3600 * 1_000_000_000;
        let inside_retention = time_provider.now().timestamp_nanos(); // now
        let outside_retention = inside_retention - retention_period_1_hour_ns - 10; // over one hour ago

        let partition_key = "test_partition";

        let l1 = format!("cpu,host=a load=1 {}", inside_retention);
        let l2 = format!("cpu,host=aa load=11 {}", outside_retention);
        let lp_partially_inside = vec![l1.as_str(), l2.as_str()];

        let l3 = format!("cpu,host=b load=2 {}", inside_retention);
        let l4 = format!("cpu,host=bb load=21 {}", inside_retention);
        let lp_fully_inside = vec![l3.as_str(), l4.as_str()];

        let l5 = format!("cpu,host=z load=3 {}", outside_retention);
        let l6 = format!("cpu,host=zz load=31 {}", outside_retention);
        let lp_fully_outside = vec![l5.as_str(), l6.as_str()];

        let c_partially_inside = ChunkData {
            lp_lines: lp_partially_inside,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };
        let c_fully_inside = ChunkData {
            lp_lines: lp_fully_inside,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };
        let c_fully_outside = ChunkData {
            lp_lines: lp_fully_outside,
            partition_key,
            chunk_stage: Some(ChunkStage::Parquet),
            ..Default::default()
        };

        make_n_chunks_scenario_with_retention(
            &[c_partially_inside, c_fully_inside, c_fully_outside],
            Some(retention_period_1_hour_ns),
        )
        .await
    }
}

#[derive(Debug)]
pub struct TwoChunksMissingColumns {}
#[async_trait]
impl DbSetup for TwoChunksMissingColumns {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key1 = "a";
        let partition_key2 = "b";

        let lp_lines1 = vec!["table,tag1=a,tag2=b field1=10,field2=11 100"];
        let lp_lines2 = vec!["table,tag1=a,tag3=c field1=20,field3=22 200"];

        make_n_chunks_scenario(&[
            ChunkData {
                lp_lines: lp_lines1,
                partition_key: partition_key1,
                ..Default::default()
            },
            ChunkData {
                lp_lines: lp_lines2,
                partition_key: partition_key2,
                ..Default::default()
            },
        ])
        .await
    }
}

// Test data for periods (`.`) in names which SQL treats as identifiers
//
pub struct PeriodsInNames {}
#[async_trait]
impl DbSetup for PeriodsInNames {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec![
            "measurement.one,tag.one=value,tag.two=other field.one=1.0,field.two=t 1609459201000000001",
            "measurement.one,tag.one=value2,tag.two=other2 field.one=1.0,field.two=f 1609459201000000002",
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "measurement.one", partition_key).await
    }
}

/// This re-creates <https://github.com/influxdata/influxdb_iox/issues/6066>.
///
/// Namely it sets up two chunks to which certain filters MUST NOT be applied prior to deduplication.
fn two_chunks_dedup_weirdness() -> Vec<ChunkData<'static, 'static>> {
    let partition_key = "1970-01-01T00";

    let lp_lines1 = vec!["table,tag=A foo=1,bar=1 0"];

    let lp_lines2 = vec!["table,tag=A bar=2 0", "table,tag=B foo=1 0"];

    vec![
        ChunkData {
            lp_lines: lp_lines1,
            partition_key,
            ..Default::default()
        },
        ChunkData {
            lp_lines: lp_lines2,
            partition_key,
            ..Default::default()
        },
    ]
}

#[derive(Debug)]
pub struct TwoChunksDedupWeirdnessParquet {}

#[async_trait]
impl DbSetup for TwoChunksDedupWeirdnessParquet {
    async fn make(&self) -> Vec<DbScenario> {
        let chunk_data: Vec<_> = two_chunks_dedup_weirdness()
            .into_iter()
            .map(|cd| ChunkData {
                chunk_stage: Some(ChunkStage::Parquet),
                ..cd
            })
            .collect();

        make_n_chunks_scenario(&chunk_data).await
    }
}

#[derive(Debug)]
pub struct TwoChunksDedupWeirdnessParquetIngester {}

#[async_trait]
impl DbSetup for TwoChunksDedupWeirdnessParquetIngester {
    async fn make(&self) -> Vec<DbScenario> {
        let chunk_data = two_chunks_dedup_weirdness();
        assert_eq!(chunk_data.len(), 2);

        make_n_chunks_scenario(&[
            ChunkData {
                chunk_stage: Some(ChunkStage::Parquet),
                ..chunk_data[0].clone()
            },
            ChunkData {
                chunk_stage: Some(ChunkStage::Ingester),
                ..chunk_data[1].clone()
            },
        ])
        .await
    }
}

/// This recreates the test case for
/// <https://github.com/influxdata/idpe/issues/16238>.
pub struct StringFieldWithNumericValue {}
#[async_trait]
impl DbSetup for StringFieldWithNumericValue {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "2021-01-01T00";

        let lp = vec!["m,tag0=foo fld=\"200\" 1000", "m,tag0=foo fld=\"404\" 1050"];

        all_scenarios_for_one_chunk(vec![], vec![], lp, "m", partition_key).await
    }
}
