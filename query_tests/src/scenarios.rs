//! This module contains testing scenarios for Db

pub mod delete;
pub mod library;
pub mod util;

use async_trait::async_trait;
use delete::{
    OneDeleteMultiExprsOneChunk, OneDeleteSimpleExprOneChunk, OneDeleteSimpleExprOneChunkDeleteAll,
    ThreeDeleteThreeChunks, TwoDeletesMultiExprsOneChunk,
};
use once_cell::sync::OnceCell;
use std::{collections::HashMap, sync::Arc};

/// Reexport library of scenarios
pub use library::*;

use crate::db::AbstractDb;

/// Holds a database and a description of how its data was configured
#[derive(Debug)]
pub struct DbScenario {
    pub scenario_name: String,
    pub db: Arc<dyn AbstractDb>,
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
            register_setup!(TwoMeasurementsManyFields),
            register_setup!(TwoMeasurementsPredicatePushDown),
            register_setup!(TwoMeasurementsManyFieldsOneChunk),
            register_setup!(OneMeasurementFourChunksWithDuplicates),
            register_setup!(OneMeasurementFourChunksWithDuplicatesParquetOnly),
            register_setup!(OneMeasurementFourChunksWithDuplicatesWithIngester),
            register_setup!(TwentySortedParquetFiles),
            register_setup!(ThreeDeleteThreeChunks),
            register_setup!(OneDeleteSimpleExprOneChunkDeleteAll),
            register_setup!(OneDeleteSimpleExprOneChunk),
            register_setup!(OneDeleteMultiExprsOneChunk),
            register_setup!(TwoDeletesMultiExprsOneChunk),
            register_setup!(OneMeasurementRealisticTimes),
            register_setup!(TwoMeasurementsManyFieldsTwoChunks),
            register_setup!(ManyFieldsSeveralChunks),
            register_setup!(TwoChunksMissingColumns),
            register_setup!(AllTypes),
            register_setup!(PeriodsInNames),
            register_setup!(TwoChunksDedupWeirdnessParquet),
            register_setup!(TwoChunksDedupWeirdnessParquetIngester),
            register_setup!(ThreeChunksWithRetention),
        ]
        .into_iter()
        .map(|(name, setup)| (name.to_string(), setup as Arc<dyn DbSetup>))
        .collect()
    })
}

/// Return a reference to the specified scenario
pub fn get_db_setup(setup_name: impl AsRef<str>) -> Option<Arc<dyn DbSetup>> {
    get_all_setups().get(setup_name.as_ref()).map(Arc::clone)
}
