//! This module contains testing scenarios for Db

pub mod delete;
pub mod util;

use std::collections::HashMap;
use std::sync::Arc;

use data_types::timestamp::TimestampRange;
use once_cell::sync::OnceCell;

use predicate::delete_expr::DeleteExpr;
use predicate::delete_predicate::DeletePredicate;
use query::QueryChunk;

use async_trait::async_trait;

use delete::{
    OneDeleteMultiExprsOneChunk, OneDeleteSimpleExprOneChunk, OneDeleteSimpleExprOneChunkDeleteAll,
    ThreeDeleteThreeChunks, TwoDeletesMultiExprsOneChunk,
};
use server::db::{LockableChunk, LockablePartition};
use server::utils::{
    count_mutable_buffer_chunks, count_object_store_chunks, count_read_buffer_chunks, make_db,
};
use server::{db::test_helpers::write_lp, Db};

use crate::scenarios::util::all_scenarios_for_one_chunk;

/// Holds a database and a description of how its data was configured
#[derive(Debug)]
pub struct DbScenario {
    pub scenario_name: String,
    pub db: Arc<Db>,
}

#[async_trait]
pub trait DbSetup: Send + Sync {
    // Create several scenarios, scenario has the same data, but
    // different physical arrangements (e.g.  the data is in different chunks)
    async fn make(&self) -> Vec<DbScenario>;
}

// registry of setups that can be referred to by name
static SETUPS: OnceCell<HashMap<String, Arc<dyn DbSetup>>> = OnceCell::new();

/// Creates a pair of (setup_name, Arc(setup))
/// assumes that the setup is constructed via DB_SETUP {} type constructor
macro_rules! register_setup {
    ($DB_SETUP_NAME:ident) => {
        (
            stringify!($DB_SETUP_NAME),
            Arc::new($DB_SETUP_NAME {}) as Arc<dyn DbSetup>,
        )
    };
}

pub fn get_all_setups() -> &'static HashMap<String, Arc<dyn DbSetup>> {
    SETUPS.get_or_init(|| {
        vec![
            register_setup!(TwoMeasurements),
            register_setup!(TwoMeasurementsPredicatePushDown),
            register_setup!(TwoMeasurementsManyFieldsOneChunk),
            register_setup!(TwoMeasurementsManyFieldsOneRubChunk),
            register_setup!(OneMeasurementFourChunksWithDuplicates),
            register_setup!(OneMeasurementAllChunksDropped),
            register_setup!(ChunkOrder),
            register_setup!(ThreeDeleteThreeChunks),
            register_setup!(OneDeleteSimpleExprOneChunkDeleteAll),
            register_setup!(OneDeleteSimpleExprOneChunk),
            register_setup!(OneDeleteMultiExprsOneChunk),
            register_setup!(TwoDeletesMultiExprsOneChunk),
            register_setup!(OneMeasurementRealisticTimes),
        ]
        .into_iter()
        .map(|(name, setup)| (name.to_string(), setup as Arc<dyn DbSetup>))
        .collect()
    })
}

/// Return a reference to the specified scenario
pub fn get_db_setup(setup_name: impl AsRef<str>) -> Option<Arc<dyn DbSetup>> {
    get_all_setups()
        .get(setup_name.as_ref())
        .map(|setup| Arc::clone(setup))
}

/// No data
#[derive(Debug)]
pub struct NoData {}
#[async_trait]
impl DbSetup for NoData {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        // Scenario 1: No data in the DB yet
        //
        let db = make_db().await.db;
        let scenario1 = DbScenario {
            scenario_name: "New, Empty Database".into(),
            db,
        };

        // Scenario 2: listing partitions (which may create an entry in a map)
        // in an empty database
        //
        let db = make_db().await.db;
        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);
        let scenario2 = DbScenario {
            scenario_name: "New, Empty Database after partitions are listed".into(),
            db,
        };

        // Scenario 3: the database has had data loaded into RB and then deleted
        //
        let db = make_db().await.db;
        let data = "cpu,region=west user=23.2 100";
        write_lp(&db, data).await;
        // move data out of open chunk
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 1); //
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        let chunk_id = db
            .compact_partition(table_name, partition_key)
            .await
            .unwrap()
            .unwrap()
            .id();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // drop chunk
        db.drop_chunk(table_name, partition_key, chunk_id)
            .await
            .unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing after dropping chunk 0
        assert_eq!(count_object_store_chunks(&db), 0); // still nothing

        let scenario3 = DbScenario {
            scenario_name: "Empty Database after drop chunk that is in read buffer".into(),
            db,
        };

        // Scenario 4: the database has had data loaded into RB & Object Store and then deleted
        //
        let db = make_db().await.db;
        let data = "cpu,region=west user=23.2 100";
        write_lp(&db, data).await;
        // move data out of open chunk
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 1); // 1 open chunk
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now write the data in RB to object store but keep it in RB
        let chunk_id = db
            .persist_partition("cpu", partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // closed chunk only
        assert_eq!(count_object_store_chunks(&db), 1); // close chunk only

        // drop chunk
        db.drop_chunk(table_name, partition_key, chunk_id)
            .await
            .unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);

        let scenario4 = DbScenario {
            scenario_name:
                "Empty Database after drop chunk that is in both read buffer and object store"
                    .into(),
            db,
        };

        vec![scenario1, scenario2, scenario3, scenario4]
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

        // return all possible scenarios a chunk: MUB open, MUB frozen, RUB, RUB & OS, OS
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
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

        // return all possible scenarios a chunk: MUB open, MUB frozen, RUB, RUB & OS, OS
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyNullTagsWithDelete {}
#[async_trait]
impl DbSetup for OneMeasurementManyNullTagsWithDelete {
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

        // pred: delete from h2o where 400 <= time <= 602 and state=NY
        // 3 rows of h2o & NY state will be deleted
        let delete_table_name = "h2o";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 400,
                end: 602,
            },
            exprs: vec![DeleteExpr::new(
                "state".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::String(("NY").to_string()),
            )],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyNullTagsWithDeleteAll {}
#[async_trait]
impl DbSetup for OneMeasurementManyNullTagsWithDeleteAll {
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

        // pred: delete from h2o where 100 <= time <= 602
        // all rows of h2o  will be deleted
        let delete_table_name = "h2o";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 100,
                end: 602,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

/// Two measurements data in a single mutable buffer chunk
#[derive(Debug)]
pub struct TwoMeasurementsMubScenario {}
#[async_trait]
impl DbSetup for TwoMeasurementsMubScenario {
    async fn make(&self) -> Vec<DbScenario> {
        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        make_one_chunk_mub_scenario(&lp_lines.join("\n")).await
    }
}

/// Two measurements data in a single read buffer chunk
#[derive(Debug)]
pub struct TwoMeasurementsRubScenario {}
#[async_trait]
impl DbSetup for TwoMeasurementsRubScenario {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        make_one_chunk_rub_scenario(partition_key, &lp_lines.join("\n")).await
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

        // return all possible scenarios a chunk: MUB open, MUB frozen, RUB, RUB & OS, OS
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu", partition_key).await
    }
}

/// Two measurements data in different chunk scenarios
/// with one delete applied at different stages of the chunk
#[derive(Debug)]
pub struct TwoMeasurementsWithDelete {}
#[async_trait]
impl DbSetup for TwoMeasurementsWithDelete {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        // pred: delete from cpu where 120 <= time <= 160 and region="west"
        // delete 1 row from cpu with timestamp 150
        let table_name = "cpu";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 120,
                end: 160,
            },
            exprs: vec![DeleteExpr::new(
                "region".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::String("west".to_string()),
            )],
        };

        // return all possible combination scenarios of a chunk stage and when the delete predicates are applied
        all_scenarios_for_one_chunk(vec![&pred], vec![], lp_lines, table_name, partition_key).await
    }
}

/// Two measurements data in different chunk scenarios
/// with 2 deletes that remove all data from one table
#[derive(Debug)]
pub struct TwoMeasurementsWithDeleteAll {}
#[async_trait]
impl DbSetup for TwoMeasurementsWithDeleteAll {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "cpu,region=west user=23.2 100",
            "cpu,region=west user=21.0 150",
            "disk,region=east bytes=99i 200",
        ];

        // pred: delete from cpu where 120 <= time <= 160 and region="west"
        // which will delete second row of the cpu
        let table_name = "cpu";
        let pred1 = DeletePredicate {
            range: TimestampRange {
                start: 120,
                end: 160,
            },
            exprs: vec![DeleteExpr::new(
                "region".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::String("west".to_string()),
            )],
        };

        // delete the first row of the cpu
        let pred2 = DeletePredicate {
            range: TimestampRange { start: 0, end: 110 },
            exprs: vec![],
        };

        // return all possible combination scenarios of a chunk stage and when the delete predicates are applied
        all_scenarios_for_one_chunk(
            vec![&pred1],
            vec![&pred2],
            lp_lines,
            table_name,
            partition_key,
        )
        .await
    }
}

#[derive(Debug)]
pub struct TwoMeasurementsUnsignedTypeMubScenario {}
#[async_trait]
impl DbSetup for TwoMeasurementsUnsignedTypeMubScenario {
    async fn make(&self) -> Vec<DbScenario> {
        let lp_lines = vec![
            "restaurant,town=andover count=40000u 100",
            "restaurant,town=reading count=632u 120",
            "school,town=reading count=17u 150",
            "school,town=andover count=25u 160",
        ];

        make_one_chunk_mub_scenario(&lp_lines.join("\n")).await
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
pub struct TwoMeasurementsPredicatePushDown {}
#[async_trait]
impl DbSetup for TwoMeasurementsPredicatePushDown {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "restaurant,town=andover count=40000u,system=5.0 100",
            "restaurant,town=reading count=632u,system=5.0 120",
            "restaurant,town=bedford count=189u,system=7.0 110",
            "restaurant,town=tewsbury count=471u,system=6.0 110",
            "restaurant,town=lexington count=372u,system=5.0 100",
            "restaurant,town=lawrence count=872u,system=6.0 110",
            "restaurant,town=reading count=632u,system=6.0 130",
            "school,town=reading count=17u,system=6.0 150",
            "school,town=andover count=25u,system=6.0 160",
        ];

        make_one_rub_or_parquet_chunk_scenario(partition_key, &lp_lines.join("\n")).await
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
            "o2,state=CA temp=79.0 300\n",
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
        let db = make_db().await.db;

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];

        write_lp(&db, &lp_lines.join("\n")).await;
        vec![DbScenario {
            scenario_name: "Data in open chunk of mutable buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has a single chunk for queries that check the state of the system
pub struct TwoMeasurementsManyFieldsOneRubChunk {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsOneRubChunk {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;
        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];

        write_lp(&db, &lp_lines.join("\n")).await;

        // move all data to RUB
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        vec![DbScenario {
            scenario_name: "Data in single chunk of read buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has two chunks for queries that check the state of the system
pub struct TwoMeasurementsManyFieldsTwoChunks {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsTwoChunks {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let partition_key = "1970-01-01T00";

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 50",
            "h2o,state=MA,city=Boston other_temp=70.4 250",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_partition("h2o", partition_key).await.unwrap();

        let lp_lines = vec![
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;

        assert_eq!(count_mutable_buffer_chunks(&db), 2);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 0);

        vec![DbScenario {
            scenario_name: "Data in two open mutable buffer chunks and read buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has two chunks with different tag/key sets for queries whose columns not include keys
pub struct OneMeasurementTwoChunksDifferentTagSet {}
#[async_trait]
impl DbSetup for OneMeasurementTwoChunksDifferentTagSet {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let partition_key = "1970-01-01T00";

        // tag: state
        let lp_lines = vec![
            "h2o,state=MA temp=70.4 50",
            "h2o,state=MA other_temp=70.4 250",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_partition("h2o", partition_key).await.unwrap();

        // tag: city
        let lp_lines = vec![
            "h2o,city=Boston other_temp=72.4 350",
            "h2o,city=Boston temp=53.4,reading=51 50",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 2);
        assert_eq!(count_object_store_chunks(&db), 0);

        vec![DbScenario {
            scenario_name: "2 chunks in read buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// Setup for four chunks with duplicates for deduplicate tests
pub struct OneMeasurementFourChunksWithDuplicates {}
#[async_trait]
impl DbSetup for OneMeasurementFourChunksWithDuplicates {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let partition_key = "1970-01-01T00";

        // Chunk 1:
        //  . time range: 50-250
        //  . no duplicates in its own chunk
        let lp_lines = vec![
            "h2o,state=MA,city=Boston min_temp=70.4 50",
            "h2o,state=MA,city=Bedford min_temp=71.59 150",
            "h2o,state=MA,city=Boston max_temp=75.4 250",
            "h2o,state=MA,city=Andover max_temp=69.2, 250",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        // Chunk 2: overlaps with chunk 1
        //  . time range: 150 - 300
        //  . no duplicates in its own chunk
        let lp_lines = vec![
            "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150", // new field (area) and update available NULL (max_temp)
            "h2o,state=MA,city=Boston min_temp=65.4 250",             // update min_temp from NULL
            "h2o,state=MA,city=Reading min_temp=53.4, 250",
            "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
            "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
            "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        // Chunk 3: no overlap
        //  . time range: 400 - 500
        //  . duplicates in its own chunk
        let lp_lines = vec![
            "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
            "h2o,state=MA,city=Boston min_temp=68.4 400",
            "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
            "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
            "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
            "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        // Chunk 4: no overlap
        //  . time range: 600 - 700
        //  . no duplicates
        let lp_lines = vec![
            "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
            "h2o,state=MA,city=Boston min_temp=67.4 600",
            "h2o,state=MA,city=Reading min_temp=60.4, 600",
            "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
            "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
            "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 4);
        assert_eq!(count_object_store_chunks(&db), 0);

        vec![DbScenario {
            scenario_name: "Data in four chunks with duplicates".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// This has a single scenario with all the life cycle operations to
/// test queries that depend on that
pub struct TwoMeasurementsManyFieldsLifecycle {}
#[async_trait]
impl DbSetup for TwoMeasurementsManyFieldsLifecycle {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        let db = make_db().await.db;

        write_lp(
            &db,
            &vec![
                "h2o,state=MA,city=Boston temp=70.4 50",
                "h2o,state=MA,city=Boston other_temp=70.4 250",
            ]
            .join("\n"),
        )
        .await;

        db.compact_open_chunk("h2o", partition_key).await.unwrap();

        db.persist_partition("h2o", partition_key, true)
            .await
            .unwrap();

        write_lp(
            &db,
            &vec!["h2o,state=CA,city=Boston other_temp=72.4 350"].join("\n"),
        )
        .await;

        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 1);

        vec![DbScenario {
            scenario_name: "Data in parquet, RUB, and MUB".into(),
            db,
        }]
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
        ];

        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "h2o", partition_key).await
    }
}

#[derive(Debug)]
pub struct OneMeasurementManyFieldsWithDelete {}
#[async_trait]
impl DbSetup for OneMeasurementManyFieldsWithDelete {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";

        // Order this so field3 comes before field2
        // (and thus the columns need to get reordered)
        let lp_lines = vec![
            "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
            "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
            "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
        ];

        // pred: delete from h2o where 1000 <= time <= 1000
        // 1 rows of h2o with timestamp 1000 will be deleted which means
        // field4 no longer available
        let delete_table_name = "h2o";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 1000,
                end: 3000,
            },
            exprs: vec![],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
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
        // return all possible scenarios a chunk: MUB open, MUB frozen, RUB, RUB & OS, OS
        all_scenarios_for_one_chunk(vec![], vec![], lp_lines, "cpu_load_short", partition_key).await
    }
}

#[derive(Debug)]
pub struct EndToEndTestWithDelete {}
#[async_trait]
impl DbSetup for EndToEndTestWithDelete {
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

        // pred: delete from swap where 6000 <= time <= 6000 and name=disk0
        // 1 rows of h2o with timestamp 250 will be deleted
        let delete_table_name = "swap";
        let pred = DeletePredicate {
            range: TimestampRange {
                start: 6000,
                end: 6000,
            },
            exprs: vec![DeleteExpr::new(
                "name".to_string(),
                predicate::delete_expr::Op::Eq,
                predicate::delete_expr::Scalar::String(("disk0").to_string()),
            )],
        };

        all_scenarios_for_one_chunk(
            vec![&pred],
            vec![],
            lp_lines,
            delete_table_name,
            partition_key,
        )
        .await
    }
}

/// This function loads one chunk of lp data into MUB only
///
pub(crate) async fn make_one_chunk_mub_scenario(data: &str) -> Vec<DbScenario> {
    // Scenario 1: One open chunk in MUB
    let db = make_db().await.db;
    write_lp(&db, data).await;
    let scenario = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer".into(),
        db,
    };

    vec![scenario]
}

/// This function loads one chunk of lp data into RUB only
///
pub(crate) async fn make_one_chunk_rub_scenario(
    partition_key: &str,
    data: &str,
) -> Vec<DbScenario> {
    // Scenario 1: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let scenario = DbScenario {
        scenario_name: "Data in read buffer".into(),
        db,
    };

    vec![scenario]
}

/// This creates two chunks but then drops them all. This should keep the tables.
#[derive(Debug)]
pub struct OneMeasurementAllChunksDropped {}
#[async_trait]
impl DbSetup for OneMeasurementAllChunksDropped {
    async fn make(&self) -> Vec<DbScenario> {
        let db = make_db().await.db;

        let partition_key = "1970-01-01T00";
        let table_name = "h2o";

        let lp_lines = vec!["h2o,state=MA temp=70.4 50"];
        write_lp(&db, &lp_lines.join("\n")).await;
        let chunk_id = db
            .compact_open_chunk(table_name, partition_key)
            .await
            .unwrap()
            .unwrap()
            .id();
        db.drop_chunk(table_name, partition_key, chunk_id)
            .await
            .unwrap();

        vec![DbScenario {
            scenario_name: "one measurement but all chunks are dropped".into(),
            db,
        }]
    }
}

/// This function loads two chunks of lp data into 4 different scenarios
///
/// Data in single open mutable buffer chunk
/// Data in one open mutable buffer chunk, one closed mutable chunk
/// Data in one open mutable buffer chunk, one read buffer chunk
/// Data in one two read buffer chunks,
pub async fn make_two_chunk_scenarios(
    partition_key: &str,
    data1: &str,
    data2: &str,
) -> Vec<DbScenario> {
    let db = make_db().await.db;
    write_lp(&db, data1).await;
    write_lp(&db, data2).await;
    let scenario1 = DbScenario {
        scenario_name: "Data in single open chunk of mutable buffer".into(),
        db,
    };

    // spread across 2 mutable buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    write_lp(&db, data2).await;
    let scenario2 = DbScenario {
        scenario_name: "Data in one open chunk and one closed chunk of mutable buffer".into(),
        db,
    };

    // spread across 1 mutable buffer, 1 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    write_lp(&db, data2).await;
    let scenario3 = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer, and one chunk of read buffer".into(),
        db,
    };

    // in 2 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        // Compact just the last chunk
        db.compact_open_chunk(table_name, partition_key)
            .await
            .unwrap();
    }
    let scenario4 = DbScenario {
        scenario_name: "Data in two read buffer chunks".into(),
        db,
    };

    // in 2 read buffer chunks that also loaded into object store
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        db.persist_partition(table_name, partition_key, true)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        db.persist_partition(table_name, partition_key, true)
            .await
            .unwrap();
    }
    let scenario5 = DbScenario {
        scenario_name: "Data in two read buffer chunks and two parquet file chunks".into(),
        db,
    };

    // Scenario 6: Two closed chunk in OS only
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        let id = db
            .persist_partition(table_name, partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        db.unload_read_buffer(table_name, partition_key, id)
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        let id = db
            .persist_partition(table_name, partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();

        db.unload_read_buffer(table_name, partition_key, id)
            .unwrap();
    }
    let scenario6 = DbScenario {
        scenario_name: "Data in 2 parquet chunks in object store only".into(),
        db,
    };

    // Scenario 7: in a single chunk resulting from compacting MUB and RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1).await;
    for table_name in &table_names {
        // put chunk 1 into RUB
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await; // write to MUB
    for table_name in &table_names {
        // compact chunks into a single RUB chunk
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let scenario7 = DbScenario {
        scenario_name: "Data in one compacted read buffer chunk".into(),
        db,
    };

    vec![
        scenario1, scenario2, scenario3, scenario4, scenario5, scenario6, scenario7,
    ]
}

/// Rollover the mutable buffer and load chunk 0 to the read buffer and object store
pub async fn rollover_and_load(db: &Arc<Db>, partition_key: &str, table_name: &str) {
    db.persist_partition(table_name, partition_key, true)
        .await
        .unwrap();
}

// // This function loads one chunk of lp data into RUB for testing predicate pushdown
pub(crate) async fn make_one_rub_or_parquet_chunk_scenario(
    partition_key: &str,
    data: &str,
) -> Vec<DbScenario> {
    // Scenario 1: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let scenario1 = DbScenario {
        scenario_name: "--------------------- Data in read buffer".into(),
        db,
    };

    // Scenario 2: One closed chunk in Parquet only
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        let id = db
            .persist_partition(table_name, partition_key, true)
            .await
            .unwrap()
            .unwrap()
            .id();
        db.unload_read_buffer(table_name, partition_key, id)
            .unwrap();
    }
    let scenario2 = DbScenario {
        scenario_name: "--------------------- Data in object store only ".into(),
        db,
    };

    vec![scenario1, scenario2]
}

/// This helper filters out scenarios from another setup. If, for
/// example, one scenario triggers a bug that is not yet fixed
pub struct FilteredSetup<S, P>
where
    S: DbSetup,
    P: Fn(&DbScenario) -> bool,
{
    inner: S,
    filter: P,
}

impl<S, P> FilteredSetup<S, P>
where
    S: DbSetup,
    P: Fn(&DbScenario) -> bool + Send + Sync,
{
    /// Create a new setup that returns all scenarios from inner if
    /// filter(scenaro) returns true.
    pub fn new(inner: S, filter: P) -> Self {
        Self { inner, filter }
    }
}

#[async_trait]
impl<S, P> DbSetup for FilteredSetup<S, P>
where
    S: DbSetup,
    P: Fn(&DbScenario) -> bool + Send + Sync,
{
    async fn make(&self) -> Vec<DbScenario> {
        self.inner
            .make()
            .await
            .into_iter()
            .filter(|s| (self.filter)(s))
            .collect()
    }
}

/// No data
#[derive(Debug)]
pub struct ChunkOrder {}
#[async_trait]
impl DbSetup for ChunkOrder {
    async fn make(&self) -> Vec<DbScenario> {
        let partition_key = "1970-01-01T00";
        let table_name = "cpu";

        let db = make_db().await.db;

        // create first chunk: data->MUB->RUB
        write_lp(&db, "cpu,region=west user=1 100").await;
        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 0);

        // We prepare a persist, then drop the locks, perform another write, re-acquire locks
        // and start a persist operation. In practice the lifecycle doesn't drop the locks
        // before starting the persist operation, but this allows us to deterministically
        // interleave a persist with a write
        let partition = db.lockable_partition(table_name, partition_key).unwrap();
        let (chunks, flush_handle) = {
            let partition = partition.read();
            let chunks = LockablePartition::chunks(&partition);
            let mut partition = partition.upgrade();
            let flush_handle = LockablePartition::prepare_persist(&mut partition, true).unwrap();

            (chunks, flush_handle)
        };

        // create second chunk: data->MUB
        write_lp(&db, "cpu,region=west user=2 100").await;
        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 0);

        let tracker = {
            let partition = partition.write();
            let chunks = chunks.iter().map(|chunk| chunk.write()).collect();
            LockablePartition::persist_chunks(partition, chunks, flush_handle).unwrap()
        };

        tracker.join().await;
        assert!(tracker.get_status().result().unwrap().success());

        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 1);

        // Now we have the the following chunks (same partition and table):
        //
        // | ID | order | tag: region | field: user | time |
        // | -- | ----- | ----------- | ----------- | ---- |
        // |  1 |     1 | "west"      |           2 | 100  |
        // |  2 |     0 | "west"      |           1 | 100  |
        //
        // The result after deduplication should be:
        //
        // | tag: region | field: user | time |
        // | ----------- | ----------- | ---- |
        // | "west"      |           2 | 100  |
        //
        // So the query engine must use `order` as a primary key to sort chunks, NOT `id`.

        let scenario = DbScenario {
            scenario_name: "chunks where chunk ID alone cannot be used for ordering".into(),
            db,
        };

        vec![scenario]
    }
}
