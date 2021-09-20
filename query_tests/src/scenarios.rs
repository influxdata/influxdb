//! This module contains testing scenarios for Db
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use data_types::chunk_metadata::ChunkId;
use datafusion::logical_plan::{col, lit};
use once_cell::sync::OnceCell;

use predicate::predicate::{Predicate, PredicateBuilder};

use query::QueryChunk;

use async_trait::async_trait;

use server::utils::{
    count_mutable_buffer_chunks, count_object_store_chunks, count_read_buffer_chunks, make_db,
};
use server::{db::test_helpers::write_lp, Db};

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
            register_setup!(OneMeasurementThreeChunksWithDuplicates),
            register_setup!(OneMeasurementAllChunksDropped),
            register_setup!(ChunkOrder),
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
        assert_eq!(
            db.rollover_partition(table_name, partition_key)
                .await
                .unwrap()
                .unwrap()
                .id(),
            ChunkId::new(0),
        );
        assert_eq!(count_mutable_buffer_chunks(&db), 1); //
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // drop chunk 0
        db.drop_chunk(table_name, partition_key, ChunkId::new(0))
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
        assert_eq!(
            db.rollover_partition(table_name, partition_key)
                .await
                .unwrap()
                .unwrap()
                .id(),
            ChunkId::new(0),
        );
        assert_eq!(count_mutable_buffer_chunks(&db), 1); // 1 open chunk
        assert_eq!(count_read_buffer_chunks(&db), 0); // nothing yet
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now load the closed chunk into the RB
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // close chunk only
        assert_eq!(count_object_store_chunks(&db), 0); // nothing yet

        // Now write the data in RB to object store but keep it in RB
        db.persist_partition(
            "cpu",
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        // it should be the same chunk!
        assert_eq!(count_mutable_buffer_chunks(&db), 0); // open chunk only
        assert_eq!(count_read_buffer_chunks(&db), 1); // closed chunk only
        assert_eq!(count_object_store_chunks(&db), 1); // close chunk only

        // drop chunk 1 (the persisted one)
        db.drop_chunk(table_name, partition_key, ChunkId::new(1))
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

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
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

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
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

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(0))
            .await
            .unwrap();

        let lp_lines = vec![
            "h2o,state=CA,city=Boston other_temp=72.4 350",
            "o2,state=MA,city=Boston temp=53.4,reading=51 50",
            "o2,state=CA temp=79.0 300",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;

        vec![DbScenario {
            scenario_name: "Data in open chunk of mutable buffer and read buffer".into(),
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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(0))
            .await
            .unwrap();

        // tag: city
        let lp_lines = vec![
            "h2o,city=Boston other_temp=72.4 350",
            "h2o,city=Boston temp=53.4,reading=51 50",
        ];
        write_lp(&db, &lp_lines.join("\n")).await;
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(1))
            .await
            .unwrap();

        vec![DbScenario {
            scenario_name: "2 chunks in read buffer".into(),
            db,
        }]
    }
}

#[derive(Debug)]
/// Setup for four chunks with duplicates for deduplicate tests
pub struct OneMeasurementThreeChunksWithDuplicates {}
#[async_trait]
impl DbSetup for OneMeasurementThreeChunksWithDuplicates {
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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(0))
            .await
            .unwrap();

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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(1))
            .await
            .unwrap();

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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(2))
            .await
            .unwrap();

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
        db.rollover_partition("h2o", partition_key).await.unwrap();
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(3))
            .await
            .unwrap();

        vec![DbScenario {
            scenario_name: "Data in open chunk of mutable buffer and read buffer".into(),
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

        // Use a background task to do the work note when I used
        // TaskTracker::join, it ended up hanging for reasons I don't
        // now
        db.move_chunk_to_read_buffer("h2o", partition_key, ChunkId::new(0))
            .await
            .unwrap();

        write_lp(
            &db,
            &vec!["h2o,state=CA,city=Boston other_temp=72.4 350"].join("\n"),
        )
        .await;

        db.persist_partition(
            "h2o",
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        vec![DbScenario {
            scenario_name: "Data in parquet, and MUB".into(),
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

        make_one_chunk_scenarios(partition_key, &lp_lines.join("\n")).await
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

        let lp_data = lp_lines.join("\n");

        let db = make_db().await.db;
        write_lp(&db, &lp_data).await;

        let scenario1 = DbScenario {
            scenario_name: "Data in open chunk of mutable buffer".into(),
            db,
        };
        vec![scenario1]
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
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        db.drop_chunk(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();

        vec![DbScenario {
            scenario_name: "one measurement but all chunks are dropped".into(),
            db,
        }]
    }
}

/// This function loads one chunk of lp data into different scenarios that simulates
/// the data life cycle.
///
pub(crate) async fn make_one_chunk_scenarios(partition_key: &str, data: &str) -> Vec<DbScenario> {
    // Scenario 1: One open chunk in MUB
    let db = make_db().await.db;
    write_lp(&db, data).await;
    let scenario1 = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer".into(),
        db,
    };

    // Scenario 2: One closed chunk in MUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let scenario2 = DbScenario {
        scenario_name: "Data in closed chunk of mutable buffer".into(),
        db,
    };

    // Scenario 3: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
    }
    let scenario3 = DbScenario {
        scenario_name: "Data in read buffer".into(),
        db,
    };

    // Scenario 4: One closed chunk in both RUb and OS
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();

        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    }
    let scenario4 = DbScenario {
        scenario_name: "Data in both read buffer and object store".into(),
        db,
    };

    // Scenario 5: One closed chunk in OS only
    let db = make_db().await.db;
    let table_names = write_lp(&db, data).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
            .unwrap();
    }
    let scenario5 = DbScenario {
        scenario_name: "Data in object store only".into(),
        db,
    };

    vec![scenario1, scenario2, scenario3, scenario4, scenario5]
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();

        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();

        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(1))
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(2))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
            .unwrap();
    }
    let table_names = write_lp(&db, data2).await;
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(2))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        db.unload_read_buffer(table_name, partition_key, ChunkId::new(3))
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
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
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
}

// This function loads one chunk of lp data into RUB for testing predicate pushdown
pub(crate) async fn make_one_rub_or_parquet_chunk_scenario(
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
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
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
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
        db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        db.persist_partition(
            table_name,
            partition_key,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
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

        // create chunk 0: data->MUB->RUB
        write_lp(&db, "cpu,region=west user=1 100").await;
        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 0);
        assert_eq!(count_object_store_chunks(&db), 0);
        let chunk = db
            .move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
            .await
            .unwrap();
        assert_eq!(chunk.id(), ChunkId::new(0));
        assert_eq!(count_mutable_buffer_chunks(&db), 0);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 0);

        // create chunk 1: data->MUB
        write_lp(&db, "cpu,region=west user=2 100").await;
        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 0);

        // prevent chunk 1 from being part of the persistence
        // NOTE: In "real life" that could happen when writes happen while a persistence is in progress, but it's easier
        //       to trigger w/ this tiny locking trick.
        let lockable_chunk = db
            .lockable_chunk(table_name, partition_key, ChunkId::new(1))
            .unwrap();
        assert_eq!(lockable_chunk.id, ChunkId::new(1));
        lockable_chunk
            .chunk
            .write()
            .set_dropping(&Default::default())
            .unwrap();

        // transform chunk 0 into chunk 2 by persisting
        let chunk = db
            .persist_partition(
                "cpu",
                partition_key,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
        assert_eq!(chunk.id(), ChunkId::new(2));
        assert_eq!(count_mutable_buffer_chunks(&db), 1);
        assert_eq!(count_read_buffer_chunks(&db), 1);
        assert_eq!(count_object_store_chunks(&db), 1);

        // unlock chunk again
        lockable_chunk
            .chunk
            .write()
            .clear_lifecycle_action()
            .unwrap();

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

        // delete happens when data in MUB
        let scenario_mub = make_delete_mub(lp_lines.clone(), pred.clone()).await;

        // delete happens when data in MUB then moved to RUB
        let scenario_rub =
            make_delete_mub_to_rub(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens when data in MUB then moved to RUB and then persisted
        let scenario_rub_os = make_delete_mub_to_rub_and_os(
            lp_lines.clone(),
            pred.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB, then persisted, and then RUB is unloaded
        let scenario_os =
            make_delete_mub_to_os(lp_lines.clone(), pred, table_name, partition_key).await;

        // return scenarios to run queries
        vec![scenario_mub, scenario_rub, scenario_rub_os, scenario_os]
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
        let scenario_rub =
            make_delete_rub(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens to data in RUB then persisted
        let scenario_rub_os =
            make_delete_rub_to_os(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens to data in RUB then persisted then RUB unloaded
        let scenario_os =
            make_delete_rub_to_os_and_unload_rub(lp_lines.clone(), pred, table_name, partition_key)
                .await;

        // return scenarios to run queries
        vec![scenario_rub, scenario_rub_os, scenario_os]
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
        let scenario_rub_os =
            make_delete_os_with_rub(lp_lines.clone(), pred.clone(), table_name, partition_key)
                .await;

        // delete happens after data is persisted but still in RUB and then unload RUB
        let _scenario_rub_os_unload_rub = make_delete_os_with_rub_then_unload_rub(
            lp_lines.clone(),
            pred.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted and RUB is unloaded
        let _scenario_os = make_delete_os(lp_lines.clone(), pred, table_name, partition_key).await;

        // return scenarios to run queries
        // NGA todo: turn these 2 OS scenarios on. May need to wait for Marco to finish persisting delete predicates first
        // vec![scenario_rub_os, scenario_rub_os_unload_rub, scenario_os]
        vec![scenario_rub_os]
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

        // delete happens when data in MUB
        let scenario_mub = make_delete_mub(lp_lines.clone(), pred.clone()).await;

        // delete happens when data in MUB then moved to RUB
        let scenario_rub =
            make_delete_mub_to_rub(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens when data in MUB then moved to RUB and then persisted
        let scenario_rub_os = make_delete_mub_to_rub_and_os(
            lp_lines.clone(),
            pred.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens when data in MUB then moved to RUB, then persisted, and then RUB is unloaded
        let scenario_os =
            make_delete_mub_to_os(lp_lines.clone(), pred, table_name, partition_key).await;

        // return scenarios to run queries
        vec![scenario_mub, scenario_rub, scenario_rub_os, scenario_os]
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
        let scenario_rub =
            make_delete_rub(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens to data in RUB then persisted
        let scenario_rub_os =
            make_delete_rub_to_os(lp_lines.clone(), pred.clone(), table_name, partition_key).await;

        // delete happens to data in RUB then persisted then RUB unloaded
        let scenario_os =
            make_delete_rub_to_os_and_unload_rub(lp_lines.clone(), pred, table_name, partition_key)
                .await;

        // return scenarios to run queries
        vec![scenario_rub, scenario_rub_os, scenario_os]
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
        let scenario_rub_os =
            make_delete_os_with_rub(lp_lines.clone(), pred.clone(), table_name, partition_key)
                .await;

        // delete happens after data is persisted but still in RUB and then unload RUB
        let _scenario_rub_os_unload_rub = make_delete_os_with_rub_then_unload_rub(
            lp_lines.clone(),
            pred.clone(),
            table_name,
            partition_key,
        )
        .await;

        // delete happens after data is persisted and RUB is unloaded
        let _scenario_os = make_delete_os(lp_lines.clone(), pred, table_name, partition_key).await;

        // return scenarios to run queries
        // NGA todo: turn these 2 OS scenarios on. May need to wait for Marco to finish persisting delete predicates first
        //vec![scenario_rub_os, scenario_rub_os_unload_rub, scenario_os]
        vec![scenario_rub_os]
    }
}

// NGA todo next PR: Add these scenarios after deleted data is eliminated from scan
//  1. Many deletes, each has one or/and multi expressions
//  2. Many different-type chunks when a delete happens
//  3. Combination of above

async fn make_delete_mub(lp_lines: Vec<&str>, pred: Predicate) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // One open MUB, no RUB, no OS
    assert_eq!(count_mutable_buffer_chunks(&db), 1);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 0);
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // Still one but frozen MUB, no RUB, no OS
    assert_eq!(count_mutable_buffer_chunks(&db), 1);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 0);

    DbScenario {
        scenario_name: "Deleted data in MUB".into(),
        db,
    }
}

async fn make_delete_mub_to_rub(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // delete data in MUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // move MUB to RUB and the delete predicate will be automatically included in RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // No MUB, one RUB, no OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 1);
    assert_eq!(count_object_store_chunks(&db), 0);

    DbScenario {
        scenario_name: "Deleted data in RUB moved from MUB".into(),
        db,
    }
}

async fn make_delete_mub_to_rub_and_os(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // delete data in MUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // move MUB to RUB and the delete predicate will be automatically included in RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // No MUB, one RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 1);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in RUB and OS".into(),
        db,
    }
}

async fn make_delete_mub_to_os(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // delete data in MUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // move MUB to RUB and the delete predicate will be automatically included in RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // remove RUB
    db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
        .unwrap();
    // No MUB, no RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in OS".into(),
        db,
    }
}

async fn make_delete_rub(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // delete data in RUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // No MUB, one RUB, no OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 1);
    assert_eq!(count_object_store_chunks(&db), 0);

    DbScenario {
        scenario_name: "Deleted data in RUB".into(),
        db,
    }
}

async fn make_delete_rub_to_os(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // delete data in RUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // No MUB, one RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 1);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in RUB and then persisted to OS".into(),
        db,
    }
}

async fn make_delete_rub_to_os_and_unload_rub(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // delete data in RUB
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // remove RUB
    db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
        .unwrap();
    // No MUB, no RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in RUB then persisted to OS then RUB unloaded".into(),
        db,
    }
}

async fn make_delete_os_with_rub(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // delete data after persisted but RUB still available
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // No MUB, one RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 1);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in OS with RUB".into(),
        db,
    }
}

async fn make_delete_os_with_rub_then_unload_rub(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // delete data after persisted but RUB still available
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // remove RUB
    db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
        .unwrap();
    // No MUB, no RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in OS only but the delete happens before RUB is unloaded"
            .into(),
        db,
    }
}

async fn make_delete_os(
    lp_lines: Vec<&str>,
    pred: Predicate,
    table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;
    // create an open MUB
    write_lp(&db, &lp_lines.join("\n")).await;
    // move MUB to RUB
    db.rollover_partition(table_name, partition_key)
        .await
        .unwrap();
    db.move_chunk_to_read_buffer(table_name, partition_key, ChunkId::new(0))
        .await
        .unwrap();
    // persist RUB and the delete predicate will be automatically included in the OS chunk
    db.persist_partition(
        table_name,
        partition_key,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap();
    // remove RUB
    db.unload_read_buffer(table_name, partition_key, ChunkId::new(1))
        .unwrap();
    // delete data after persisted but RUB still available
    db.delete("cpu", Arc::new(pred)).await.unwrap();
    // No MUB, no RUB, one OS
    assert_eq!(count_mutable_buffer_chunks(&db), 0);
    assert_eq!(count_read_buffer_chunks(&db), 0);
    assert_eq!(count_object_store_chunks(&db), 1);

    DbScenario {
        scenario_name: "Deleted data in OS and the delete happens after RUB is unloaded".into(),
        db,
    }
}
