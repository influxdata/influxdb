//! This module contains testing scenarios for Db

use query::{test::TestLPWriter, PartitionChunk};

use async_trait::async_trait;

use crate::db::Db;

use super::utils::make_db;

/// Holds a database and a description of how its data was configured
pub struct DBScenario {
    pub scenario_name: String,
    pub db: Db,
}

#[async_trait]
pub trait DBSetup {
    // Create several scenarios, scenario has the same data, but
    // different physical arrangements (e.g.  the data is in different chunks)
    async fn make(&self) -> Vec<DBScenario>;
}

/// No data
pub struct NoData {}
#[async_trait]
impl DBSetup for NoData {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";
        let db = make_db();
        let scenario1 = DBScenario {
            scenario_name: "New, Empty Database".into(),
            db,
        };

        // listing partitions (which may create an entry in a map)
        // in an empty database
        let db = make_db();
        assert_eq!(db.mutable_buffer_chunks(partition_key).len(), 1); // only open chunk
        assert_eq!(db.read_buffer_chunks(partition_key).len(), 0);
        let scenario2 = DBScenario {
            scenario_name: "New, Empty Database after partitions are listed".into(),
            db,
        };

        // a scenario where the database has had data loaded and then deleted
        let db = make_db();
        let data = "cpu,region=west user=23.2 100";
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, data).await.unwrap();
        // move data out of open chunk
        assert_eq!(db.rollover_partition(partition_key).await.unwrap().id(), 0);
        // drop it
        db.drop_mutable_buffer_chunk(partition_key, 0)
            .await
            .unwrap();

        assert_eq!(db.mutable_buffer_chunks(partition_key).len(), 1);

        assert_eq!(db.read_buffer_chunks(partition_key).len(), 0); // only open chunk

        let scenario3 = DBScenario {
            scenario_name: "Empty Database after drop chunk".into(),
            db,
        };

        vec![scenario1, scenario2, scenario3]
    }
}

/// Two measurements data in a single mutable buffer chunk
pub struct TwoMeasurements {}
#[async_trait]
impl DBSetup for TwoMeasurements {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";
        let data = "cpu,region=west user=23.2 100\n\
                    cpu,region=west user=21.0 150\n\
                    disk,region=east bytes=99i 200";

        make_one_chunk_scenarios(partition_key, data).await
    }
}

/// Single measurement that has several different chunks with
/// different (but compatible) schema
pub struct MultiChunkSchemaMerge {}
#[async_trait]
impl DBSetup for MultiChunkSchemaMerge {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";
        let data1 = "cpu,region=west user=23.2,system=5.0 100\n\
                     cpu,region=west user=21.0,system=6.0 150";
        let data2 = "cpu,region=east,host=foo user=23.2 100\n\
                     cpu,region=west,host=bar user=21.0 250";

        make_two_chunk_scenarios(partition_key, data1, data2).await
    }
}

/// Two measurements data with many null values
pub struct TwoMeasurementsManyNulls {}
#[async_trait]
impl DBSetup for TwoMeasurementsManyNulls {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";
        let data1 = "h2o,state=CA,city=LA,county=LA temp=70.4 100\n\
                        h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250\n\
                        o2,state=MA,city=Boston temp=50.4 200\n\
                        o2,state=CA temp=79.0 300\n";
        let data2 = "o2,state=NY temp=60.8 400\n\
                        o2,state=NY,city=NYC temp=61.0 500\n\
                        o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600\n";

        make_two_chunk_scenarios(partition_key, data1, data2).await
    }
}

pub struct TwoMeasurementsManyFields {}
#[async_trait]
impl DBSetup for TwoMeasurementsManyFields {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        let data1 = "h2o,state=MA,city=Boston temp=70.4 50\n\
                        h2o,state=MA,city=Boston other_temp=70.4 250\n\
                        h2o,state=CA,city=Boston other_temp=72.4 350\n\
                        o2,state=MA,city=Boston temp=53.4,reading=51 50\n\
                        o2,state=CA temp=79.0 300";
        let data2 = "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000";
        make_two_chunk_scenarios(partition_key, data1, data2).await
    }
}

pub struct OneMeasurementManyFields {}
#[async_trait]
impl DBSetup for OneMeasurementManyFields {
    async fn make(&self) -> Vec<DBScenario> {
        let partition_key = "1970-01-01T00";

        // Order this so field3 comes before field2
        // (and thus the columns need to get reordered)
        let data = "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100\n\
                   h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100\n\
                   h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100\n\
                   h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000";

        make_one_chunk_scenarios(partition_key, data).await
    }
}

/// This data (from end to end test)
pub struct EndToEndTest {}
#[async_trait]
impl DBSetup for EndToEndTest {
    async fn make(&self) -> Vec<DBScenario> {
        let lp_data = "cpu_load_short,host=server01,region=us-west value=0.64 0000\n\
                       cpu_load_short,host=server01 value=27.99 1000\n\
                       cpu_load_short,host=server02,region=us-west value=3.89 2000\n\
                       cpu_load_short,host=server01,region=us-east value=1234567.891011 3000\n\
                       cpu_load_short,host=server01,region=us-west value=0.000003 4000\n\
                       system,host=server03 uptime=1303385 5000\n\
                       swap,host=server01,name=disk0 in=3,out=4 6000\n\
                       status active=t 7000\n\
                       attributes color=\"blue\" 8000\n";

        let db = make_db();
        let mut writer = TestLPWriter::default();
        let res = writer.write_lp_string(&db, lp_data).await;
        assert!(res.is_ok(), "Error: {}", res.unwrap_err());

        let scenario1 = DBScenario {
            scenario_name: "Data in open chunk of mutable buffer".into(),
            db,
        };
        vec![scenario1]
    }
}

/// This function loads two chunks of lp data into 4 different scenarios
///
/// Data in single open mutable buffer chunk
/// Data in single closed mutable buffer chunk, one closed mutable chunk
/// Data in both read buffer and mutable buffer chunk
/// Data in one only read buffer chunk
async fn make_one_chunk_scenarios(partition_key: &str, data: &str) -> Vec<DBScenario> {
    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data).await.unwrap();
    let scenario1 = DBScenario {
        scenario_name: "Data in open chunk of mutable buffer".into(),
        db,
    };

    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    let scenario2 = DBScenario {
        scenario_name: "Data in closed chunk of mutable buffer".into(),
        db,
    };

    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    db.load_chunk_to_read_buffer(partition_key, 0)
        .await
        .unwrap();
    let scenario3 = DBScenario {
        scenario_name: "Data in both read buffer and mutable buffer".into(),
        db,
    };

    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    db.load_chunk_to_read_buffer(partition_key, 0)
        .await
        .unwrap();
    db.drop_mutable_buffer_chunk(partition_key, 0)
        .await
        .unwrap();
    let scenario4 = DBScenario {
        scenario_name: "Data in only read buffer and not mutable buffer".into(),
        db,
    };

    vec![scenario1, scenario2, scenario3, scenario4]
}

/// This function loads two chunks of lp data into 4 different scenarios
///
/// Data in single open mutable buffer chunk
/// Data in one open mutable buffer chunk, one closed mutable chunk
/// Data in one open mutable buffer chunk, one read buffer chunk
/// Data in one two read buffer chunks,
async fn make_two_chunk_scenarios(
    partition_key: &str,
    data1: &str,
    data2: &str,
) -> Vec<DBScenario> {
    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data1).await.unwrap();
    writer.write_lp_string(&db, data2).await.unwrap();
    let scenario1 = DBScenario {
        scenario_name: "Data in single open chunk of mutable buffer".into(),
        db,
    };

    // spread across 2 mutable buffer chunks
    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data1).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    writer.write_lp_string(&db, data2).await.unwrap();
    let scenario2 = DBScenario {
        scenario_name: "Data in one open chunk and one closed chunk of mutable buffer".into(),
        db,
    };

    // spread across 1 mutable buffer, 1 read buffer chunks
    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data1).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    db.load_chunk_to_read_buffer(partition_key, 0)
        .await
        .unwrap();
    db.drop_mutable_buffer_chunk(partition_key, 0)
        .await
        .unwrap();
    writer.write_lp_string(&db, data2).await.unwrap();
    let scenario3 = DBScenario {
        scenario_name: "Data in open chunk of mutable buffer, and one chunk of read buffer".into(),
        db,
    };

    // in 2 read buffer chunks
    let db = make_db();
    let mut writer = TestLPWriter::default();
    writer.write_lp_string(&db, data1).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();
    writer.write_lp_string(&db, data2).await.unwrap();
    db.rollover_partition(partition_key).await.unwrap();

    db.load_chunk_to_read_buffer(partition_key, 0)
        .await
        .unwrap();
    db.drop_mutable_buffer_chunk(partition_key, 0)
        .await
        .unwrap();

    db.load_chunk_to_read_buffer(partition_key, 1)
        .await
        .unwrap();
    db.drop_mutable_buffer_chunk(partition_key, 1)
        .await
        .unwrap();
    let scenario4 = DBScenario {
        scenario_name: "Data in two read buffer chunks".into(),
        db,
    };

    vec![scenario1, scenario2, scenario3, scenario4]
}
