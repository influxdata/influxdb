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
        assert_eq!(db.mutable_buffer_chunks(partition_key).await.len(), 1); // only open chunk
        assert_eq!(db.read_buffer_chunks(partition_key).await.len(), 0);
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

        assert_eq!(db.mutable_buffer_chunks(partition_key).await.len(), 1);

        assert_eq!(db.read_buffer_chunks(partition_key).await.len(), 0); // only open chunk

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
            scenario_name: "Data in only buffer and not mutable buffer".into(),
            db,
        };

        vec![scenario1, scenario2, scenario3, scenario4]
    }
}
