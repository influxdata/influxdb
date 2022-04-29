//! This module contains util functions for testing scenarios
use super::DbScenario;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{chunk_metadata::ChunkId, delete_predicate::DeletePredicate};
use data_types2::{
    IngesterQueryRequest, NonEmptyString, PartitionId, Sequence, SequenceNumber, SequencerId,
    TombstoneId,
};
use db::test_helpers::chunk_ids_rub;
use db::{
    test_helpers::write_lp,
    utils::{count_mub_table_chunks, count_os_table_chunks, count_rub_table_chunks, make_db},
    Db,
};
use dml::{DmlDelete, DmlMeta, DmlOperation, DmlWrite};
use futures::StreamExt;
use generated_types::influxdata::iox::ingester::v1::{
    IngesterQueryResponseMetadata, PartitionStatus,
};
use influxdb_iox_client::flight::Error as FlightError;
use ingester::data::{IngesterData, IngesterQueryResponse, Persister, SequencerData};
use ingester::lifecycle::LifecycleHandle;
use ingester::partioning::{Partitioner, PartitionerError};
use ingester::querier_handler::prepare_data_to_querier;
use iox_catalog::interface::get_schema_by_name;
use iox_tests::util::{TestCatalog, TestNamespace, TestSequencer};
use itertools::Itertools;
use mutable_batch::MutableBatch;
use mutable_batch_lp::LinesConverter;
use querier::{
    IngesterConnectionImpl, IngesterFlightClient, IngesterFlightClientError,
    IngesterFlightClientQueryData, QuerierCatalogCache, QuerierNamespace,
};
use query::QueryChunk;
use schema::selection::Selection;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::sync::Mutex;
use std::{fmt::Display, sync::Arc};

// Structs, enums, and functions used to exhaust all test scenarios of chunk life cycle
// & when delete predicates are applied

// STRUCTs & ENUMs
#[derive(Debug, Clone)]
pub struct ChunkDataOld<'a> {
    /// Line protocol data of this chunk
    pub lp_lines: Vec<&'a str>,
    /// which stage this chunk will be created
    pub chunk_stage: ChunkStageOld,
}

#[derive(Debug, Clone, Default)]
pub struct ChunkDataNew<'a, 'b> {
    /// Line protocol data of this chunk
    pub lp_lines: Vec<&'a str>,

    /// which stage this chunk will be created.
    ///
    /// If not set, this chunk will be created in [all](ChunkStageNew::all) stages. This can be helpful when the test
    /// scenario is not specific to the chunk stage. If this is used for multiple chunks, then all stage permutations
    /// will be generated.
    pub chunk_stage: Option<ChunkStageNew>,

    /// Delete predicates
    pub preds: Vec<PredNew<'b>>,

    /// Table that should be deleted.
    pub delete_table_name: Option<&'a str>,

    /// Partition key
    pub partition_key: &'a str,
}

impl<'a, 'b> ChunkDataNew<'a, 'b> {
    fn with_chunk_stage(self, chunk_stage: ChunkStageNew) -> Self {
        assert!(self.chunk_stage.is_none());

        Self {
            chunk_stage: Some(chunk_stage),
            ..self
        }
    }

    /// Replace [`DeleteTimeNew::Begin`] and [`DeleteTimeNew::End`] with values that correspond to the linked [`ChunkStageNew`].
    fn replace_begin_and_end_delete_times(self) -> Self {
        Self {
            preds: self
                .preds
                .into_iter()
                .map(|pred| {
                    pred.replace_begin_and_end_delete_times(
                        self.chunk_stage
                            .expect("chunk stage must be set at this point"),
                    )
                })
                .collect(),
            ..self
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChunkStageOld {
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

impl Display for ChunkStageOld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mubo => write!(f, "Open MUB"),
            Self::Mubf => write!(f, "Frozen MUB"),
            Self::Rub => write!(f, "RUB"),
            Self::RubOs => write!(f, "RUB & OS"),
            Self::Os => write!(f, "OS"),
        }
    }
}

impl ChunkStageOld {
    /// return the list of all chunk types
    pub fn all() -> Vec<Self> {
        vec![Self::Mubo, Self::Mubf, Self::Rub, Self::RubOs, Self::Os]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStageNew {
    /// In parquet file.
    Parquet,

    /// In ingester.
    Ingester,
}

impl Display for ChunkStageNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parquet => write!(f, "Parquet"),
            Self::Ingester => write!(f, "Ingester"),
        }
    }
}

impl PartialOrd for ChunkStageNew {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // allow multiple parquet chunks (for the same partition). sequence numbers will be used for ordering.
            (Self::Parquet, Self::Parquet) => Some(Ordering::Equal),

            // "parquet" chunks are older (i.e. come earlier) than chunks that still life in the ingester
            (Self::Parquet, Self::Ingester) => Some(Ordering::Less),
            (Self::Ingester, Self::Parquet) => Some(Ordering::Greater),

            // it's impossible for two chunks (for the same partition) to be in the ingester stage
            (Self::Ingester, Self::Ingester) => None,
        }
    }
}

impl ChunkStageNew {
    /// return the list of all chunk types
    pub fn all() -> Vec<Self> {
        vec![Self::Parquet, Self::Ingester]
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChunkStage {
    Old(ChunkStageOld),
    New(ChunkStageNew),
}

impl Display for ChunkStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Old(stage) => write!(f, "Old: {}", stage),
            Self::New(stage) => write!(f, "New: {}", stage),
        }
    }
}

impl ChunkStage {
    /// return the list of all chunk types
    pub fn all() -> Vec<Self> {
        ChunkStageOld::all()
            .into_iter()
            .map(ChunkStage::Old)
            .chain(ChunkStageNew::all().into_iter().map(ChunkStage::New))
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct PredOld<'a> {
    /// Delete predicate
    predicate: &'a DeletePredicate,
    /// At which chunk stage this predicate is applied
    delete_time: DeleteTimeOld,
}

#[derive(Debug, Clone)]
pub struct PredNew<'a> {
    /// Delete predicate
    pub predicate: &'a DeletePredicate,

    /// At which chunk stage this predicate is applied
    pub delete_time: DeleteTimeNew,
}

impl<'a> PredNew<'a> {
    /// Replace [`DeleteTimeNew::Begin`] and [`DeleteTimeNew::End`] with values that correspond to the linked [`ChunkStageNew`].
    fn replace_begin_and_end_delete_times(self, stage: ChunkStageNew) -> Self {
        Self {
            delete_time: self.delete_time.replace_begin_and_end_delete_times(stage),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
pub enum Pred<'a> {
    Old(PredOld<'a>),
    New(PredNew<'a>),
}

impl<'a> Pred<'a> {
    fn new(predicate: &'a DeletePredicate, delete_time: DeleteTime) -> Self {
        match delete_time {
            DeleteTime::Old(delete_time) => Self::Old(PredOld {
                predicate,
                delete_time,
            }),
            DeleteTime::New(delete_time) => Self::New(PredNew {
                predicate,
                delete_time,
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteTimeOld {
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

impl DeleteTimeOld {
    /// Return all DeleteTime at and after the given chunk stage
    pub fn all_from_and_before(chunk_stage: ChunkStageOld) -> Vec<Self> {
        match chunk_stage {
            ChunkStageOld::Mubo => vec![Self::Mubo],
            ChunkStageOld::Mubf => vec![Self::Mubo, Self::Mubf],
            ChunkStageOld::Rub => {
                vec![Self::Mubo, Self::Mubf, Self::Rub]
            }
            ChunkStageOld::RubOs => vec![Self::Mubo, Self::Mubf, Self::Rub, Self::RubOs],
            ChunkStageOld::Os => vec![Self::Mubo, Self::Mubf, Self::Rub, Self::RubOs, Self::Os],
        }
    }

    pub fn begin() -> Self {
        Self::Mubo
    }

    pub fn end() -> Self {
        Self::End
    }
}

/// Describes when a delete predicate was applied.
///
/// # Ordering
/// Compared to [`ChunkStageNew`], the ordering here may seem a bit confusing. While the latest payload / LP data
/// resists in the ingester and is not yet available as a parquet file, the latest tombstones apply to parquet files and
/// were (past tense!) NOT applied while the LP data was in the ingester.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteTimeNew {
    /// Special delete time which marks the first time that could be used from deletion.
    ///
    /// May depend on [`ChunkStageNew`].
    Begin,

    /// Delete predicate is added while chunk is was still in ingester memory.
    Ingester {
        /// Flag if the tombstone also exists in the catalog.
        ///
        /// If this is set to `false`, then the tombstone was applied by the ingester but does not exist in the catalog
        /// any longer. This can be because:
        ///
        /// - the ingester decided that it doesn't need to be added to the catalog (this is currently/2022-04-21 not implemented!)
        /// - the compactor pruned the tombstone from the catalog because there are zero affected parquet files
        also_in_catalog: bool,
    },

    /// Delete predicate is added to chunks at their parquet stage
    Parquet,

    /// Special delete time which marks the last time that could be used from deletion.
    ///
    /// May depend on [`ChunkStageNew`].
    End,
}

impl DeleteTimeNew {
    /// Return all DeleteTime at and after the given chunk stage
    pub fn all_from_and_before(chunk_stage: ChunkStageNew) -> Vec<DeleteTimeNew> {
        match chunk_stage {
            ChunkStageNew::Parquet => vec![
                Self::Ingester {
                    also_in_catalog: true,
                },
                Self::Ingester {
                    also_in_catalog: false,
                },
                Self::Parquet,
            ],
            ChunkStageNew::Ingester => vec![
                Self::Ingester {
                    also_in_catalog: true,
                },
                Self::Ingester {
                    also_in_catalog: false,
                },
            ],
        }
    }

    /// Replace [`DeleteTimeNew::Begin`] and [`DeleteTimeNew::End`] with values that correspond to the linked [`ChunkStageNew`].
    fn replace_begin_and_end_delete_times(self, stage: ChunkStageNew) -> Self {
        match self {
            Self::Begin => Self::begin_for(stage),
            Self::End => Self::end_for(stage),
            other @ (Self::Ingester { .. } | Self::Parquet) => other,
        }
    }

    fn begin_for(_stage: ChunkStageNew) -> Self {
        Self::Ingester {
            also_in_catalog: true,
        }
    }

    fn end_for(stage: ChunkStageNew) -> Self {
        match stage {
            ChunkStageNew::Ingester => Self::Ingester {
                also_in_catalog: true,
            },
            ChunkStageNew::Parquet => Self::Parquet,
        }
    }
}

impl Display for DeleteTimeNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Begin => write!(f, "Begin"),
            Self::Ingester {
                also_in_catalog: false,
            } => write!(f, "Ingester w/o catalog entry"),
            Self::Ingester {
                also_in_catalog: true,
            } => write!(f, "Ingester w/ catalog entry"),
            Self::Parquet => write!(f, "Parquet"),
            Self::End => write!(f, "End"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteTime {
    Old(DeleteTimeOld),
    New(DeleteTimeNew),
}

impl DeleteTime {
    /// Return all DeleteTime at and after the given chunk stage
    fn all_from_and_before(chunk_stage: ChunkStage) -> Vec<Self> {
        match chunk_stage {
            ChunkStage::Old(chunk_stage) => DeleteTimeOld::all_from_and_before(chunk_stage)
                .into_iter()
                .map(Self::Old)
                .collect(),
            ChunkStage::New(chunk_stage) => DeleteTimeNew::all_from_and_before(chunk_stage)
                .into_iter()
                .map(Self::New)
                .collect(),
        }
    }

    fn begin_for(chunk_stage: ChunkStage) -> Self {
        match chunk_stage {
            ChunkStage::Old(_) => Self::Old(DeleteTimeOld::begin()),
            ChunkStage::New(chunk_stage) => Self::New(DeleteTimeNew::begin_for(chunk_stage)),
        }
    }

    fn end_for(chunk_stage: ChunkStage) -> Self {
        match chunk_stage {
            ChunkStage::Old(_) => Self::Old(DeleteTimeOld::end()),
            ChunkStage::New(chunk_stage) => Self::New(DeleteTimeNew::end_for(chunk_stage)),
        }
    }
}

// --------------------------------------------------------------------------------------------

/// All scenarios chunk stages and their life cycle moves for given set of delete predicates.
/// If the delete predicates are empty, all scenarios of different chunk stages will be returned.
pub async fn all_scenarios_for_one_chunk(
    // These delete predicates are applied at all stages of the chunk life cycle
    chunk_stage_preds: Vec<&DeletePredicate>,
    // These delete predicates are applied to all chunks at their final stages
    at_end_preds: Vec<&DeletePredicate>,
    // Input data, formatted as line protocol.  One chunk will be created for each measurement
    // (table) that appears in the input
    lp_lines: Vec<&str>,
    // Table to which the delete predicates will be applied
    delete_table_name: &str,
    // Partition of the chunk
    partition_key: &str,
) -> Vec<DbScenario> {
    let mut scenarios = vec![];
    // Go over chunk stages
    for chunk_stage in ChunkStage::all() {
        // Apply delete chunk_stage_preds to this chunk stage at
        // all stages at and before that in the life cycle to the chunk
        // But only need to get all delete times if chunk_stage_preds is not empty,
        // otherwise, produce only one scenario of each chunk stage
        let mut delete_times = vec![DeleteTime::begin_for(chunk_stage)];
        if !chunk_stage_preds.is_empty() {
            delete_times = DeleteTime::all_from_and_before(chunk_stage)
        };

        // Make delete predicates that happen when all chunks in their final stages
        let end_preds: Vec<Pred> = at_end_preds
            .iter()
            .map(|p| Pred::new(*p, DeleteTime::end_for(chunk_stage)))
            .collect();

        for delete_time in delete_times {
            // make delete predicate with time it happens
            let mut preds: Vec<Pred> = chunk_stage_preds
                .iter()
                .map(|p| Pred::new(*p, delete_time))
                .collect();
            // extend at-end predicates
            preds.extend(end_preds.clone());

            // make this specific chunk stage & delete predicates scenario
            scenarios.push(
                make_chunk_with_deletes_at_different_stages(
                    lp_lines.clone(),
                    chunk_stage,
                    preds,
                    delete_table_name,
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
    delete_table_name: &str,
    partition_key: &str,
) -> DbScenario {
    match chunk_stage {
        ChunkStage::Old(chunk_stage) => {
            let preds: Vec<_> = preds
                .into_iter()
                .map(|p| match p {
                    Pred::Old(pred) => pred,
                    Pred::New(_) => panic!("mixed new and old"),
                })
                .collect();

            make_chunk_with_deletes_at_different_stages_old(
                lp_lines,
                chunk_stage,
                preds,
                delete_table_name,
                partition_key,
            )
            .await
        }
        ChunkStage::New(chunk_stage) => {
            let preds: Vec<_> = preds
                .into_iter()
                .map(|p| match p {
                    Pred::Old(_) => panic!("mixed new and old"),
                    Pred::New(pred) => pred,
                })
                .collect();

            make_chunk_with_deletes_at_different_stages_new(
                lp_lines,
                chunk_stage,
                preds,
                (!delete_table_name.is_empty()).then(|| delete_table_name),
                partition_key,
            )
            .await
        }
    }
}

async fn make_chunk_with_deletes_at_different_stages_old(
    lp_lines: Vec<&str>,
    chunk_stage: ChunkStageOld,
    preds: Vec<PredOld<'_>>,
    delete_table_name: &str,
    partition_key: &str,
) -> DbScenario {
    let db = make_db().await.db;

    // ----------------------
    // Make an open MUB
    //
    // There may be more than one tables in the lp data
    let tables = write_lp(&db, &lp_lines.join("\n"));
    for table in &tables {
        let num_mubs = count_mub_table_chunks(&db, table.as_str(), partition_key);
        // must be one MUB per table
        assert_eq!(num_mubs, 1);
    }
    // Apply delete predicate
    let mut deleted = false;
    let mut display = "".to_string();
    let mut count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTimeOld::Mubo {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
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
        ChunkStageOld::Mubf | ChunkStageOld::Rub | ChunkStageOld::RubOs | ChunkStageOld::Os => {
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
                let num_mubs = count_mub_table_chunks(&db, table.as_str(), partition_key);
                let num_rubs = count_rub_table_chunks(&db, table.as_str(), partition_key);
                assert_eq!(num_mubs, 1);
                assert_eq!(num_rubs, 0);
            }
        }
        _ => {}
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTimeOld::Mubf {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
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
        ChunkStageOld::Rub | ChunkStageOld::RubOs | ChunkStageOld::Os => {
            let mut no_more_data = false;
            for table in &tables {
                // Compact this MUB of this table
                let chunk_result = db.compact_partition(table, partition_key).await.unwrap();

                // Verify no MUB and one RUB if not all data was soft deleted
                let num_mubs = count_mub_table_chunks(&db, table.as_str(), partition_key);
                let num_rubs = count_rub_table_chunks(&db, table.as_str(), partition_key);
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
        if pred.delete_time == DeleteTimeOld::Rub {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
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
        ChunkStageOld::RubOs | ChunkStageOld::Os => {
            let mut no_more_data = false;
            for table in &tables {
                // Persist RUB of this table
                let chunk_result = db
                    .persist_partition(table, partition_key, true)
                    .await
                    .unwrap();

                // Verify no MUB and one RUB if not all data was soft deleted
                let num_mubs = count_mub_table_chunks(&db, table.as_str(), partition_key);
                let num_rubs = count_rub_table_chunks(&db, table.as_str(), partition_key);
                let num_os = count_os_table_chunks(&db, table.as_str(), partition_key);
                assert_eq!(num_mubs, 0);

                // For delete table, two different things can happen
                if table == delete_table_name {
                    match chunk_result {
                        // Not all rows were soft deleted
                        Some(_chunk) => {
                            assert_eq!(num_rubs, 1); // still have RUB with the persisted OS
                            assert_eq!(num_os, 1);
                        }
                        // All rows in the RUB were soft deleted. There is nothing after compacting and hence
                        // nothing will be left and persisted.
                        None => {
                            assert_eq!(num_rubs, 0);
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
        if pred.delete_time == DeleteTimeOld::RubOs {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
                .unwrap();
            count = 1;
        }
    }
    if count > 0 {
        display.push_str(format!(", with {} deletes from RUB & OS", count).as_str());
    }

    // ----------------------
    // Unload RUB
    if let ChunkStageOld::Os = chunk_stage {
        for table in &tables {
            // retrieve its chunk_id first
            let rub_chunk_ids = chunk_ids_rub(&db, Some(table.as_str()), Some(partition_key));
            assert_eq!(rub_chunk_ids.len(), 1);
            db.unload_read_buffer(table.as_str(), partition_key, rub_chunk_ids[0])
                .unwrap();

            // verify chunk stages
            let num_mubs = count_mub_table_chunks(&db, table.as_str(), partition_key);
            let num_rubs = count_rub_table_chunks(&db, table.as_str(), partition_key);
            let num_os = count_os_table_chunks(&db, table.as_str(), partition_key);
            assert_eq!(num_mubs, 0);
            assert_eq!(num_rubs, 0);
            assert_eq!(num_os, 1);
        }
    }
    // Apply delete predicate
    count = 0;
    for pred in &preds {
        if pred.delete_time == DeleteTimeOld::Os || pred.delete_time == DeleteTimeOld::End {
            db.delete(delete_table_name, Arc::new(pred.predicate.clone()))
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

async fn make_chunk_with_deletes_at_different_stages_new(
    lp_lines: Vec<&str>,
    chunk_stage: ChunkStageNew,
    preds: Vec<PredNew<'_>>,
    delete_table_name: Option<&str>,
    partition_key: &str,
) -> DbScenario {
    let chunk_data = ChunkDataNew {
        lp_lines,
        chunk_stage: Some(chunk_stage),
        preds,
        delete_table_name,
        partition_key,
    };
    let mut mock_ingester = MockIngester::new().await;
    let scenario_name = make_ng_chunk(&mut mock_ingester, chunk_data).await;

    let db = mock_ingester.into_query_namespace().await;

    DbScenario { scenario_name, db }
}

/// Build many chunks which are in different stages
//  Note that, after a lot of thoughts, I decided to have 2 separated functions, this one and the one above.
//  The above tests delete predicates before and/or after a chunk is moved to different stages, while
//  this function tests different-stage chunks in various stages when one or many deletes happen.
//  Even though these 2 functions have some overlapped code, merging them in one
//  function will created a much more complicated cases to handle
pub async fn make_different_stage_chunks_with_deletes_scenario_old(
    data: Vec<ChunkDataOld<'_>>,
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
        write_lp(&db, &chunk_data.lp_lines.join("\n"));
        // 0 does not represent the real chunk id. It is here just to initialize the chunk_id  variable for later assignment
        let mut chunk_id = db.chunk_summaries()[0].id;

        // ----------
        // freeze MUB
        match chunk_data.chunk_stage {
            ChunkStageOld::Mubf | ChunkStageOld::Rub | ChunkStageOld::RubOs | ChunkStageOld::Os => {
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
            ChunkStageOld::Rub | ChunkStageOld::RubOs | ChunkStageOld::Os => {
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
            ChunkStageOld::RubOs | ChunkStageOld::Os => {
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
        if let ChunkStageOld::Os = chunk_data.chunk_stage {
            db.unload_read_buffer(table_name, partition_key, chunk_id)
                .unwrap();
        }
    }

    // ----------
    // Apply all delete predicates
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone())).unwrap();
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

pub async fn make_os_chunks_and_then_compact_with_different_scenarios_with_delete(
    lp_lines_vec: Vec<Vec<&str>>,
    preds: Vec<&DeletePredicate>,
    table_name: &str,
    partition_key: &str,
) -> Vec<DbScenario> {
    // Scenario 1: apply deletes and then compact all 3 chunks
    let (db, chunk_ids) =
        make_contiguous_os_chunks(lp_lines_vec.clone(), table_name, partition_key).await;
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone())).unwrap();
    }
    db.compact_object_store_chunks(table_name, partition_key, chunk_ids)
        .unwrap()
        .join()
        .await;

    let scenario_name = "Deletes and then compact all OS chunks".to_string();
    let scenario_1 = DbScenario { scenario_name, db };

    // Scenario 2: compact all 3 chunks and apply deletes
    let (db, chunk_ids) =
        make_contiguous_os_chunks(lp_lines_vec.clone(), table_name, partition_key).await;
    db.compact_object_store_chunks(table_name, partition_key, chunk_ids)
        .unwrap()
        .join()
        .await;
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone())).unwrap();
    }
    let scenario_name = "Compact all OS chunks and then deletes".to_string();
    let scenario_2 = DbScenario { scenario_name, db };

    // Scenario 3: apply deletes then compact the first n-1 chunks
    let (db, chunk_ids) =
        make_contiguous_os_chunks(lp_lines_vec.clone(), table_name, partition_key).await;
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone())).unwrap();
    }
    let (_last_chunk_id, chunk_ids_but_last) = chunk_ids.split_last().unwrap();
    db.compact_object_store_chunks(table_name, partition_key, chunk_ids_but_last.to_vec())
        .unwrap()
        .join()
        .await;
    let scenario_name = "Deletes and then compact all but last OS chunk".to_string();
    let scenario_3 = DbScenario { scenario_name, db };

    // Scenario 4: compact the first n-1 chunks then apply deletes
    let (db, chunk_ids) =
        make_contiguous_os_chunks(lp_lines_vec.clone(), table_name, partition_key).await;
    let (_last_chunk_id, chunk_ids_but_last) = chunk_ids.split_last().unwrap();
    db.compact_object_store_chunks(table_name, partition_key, chunk_ids_but_last.to_vec())
        .unwrap()
        .join()
        .await;
    for pred in &preds {
        db.delete(table_name, Arc::new((*pred).clone())).unwrap();
    }
    let scenario_name = "Compact all but last OS chunk and then deletes".to_string();
    let scenario_4 = DbScenario { scenario_name, db };

    vec![scenario_1, scenario_2, scenario_3, scenario_4]
}

async fn make_contiguous_os_chunks(
    lp_lines_vec: Vec<Vec<&str>>,
    table_name: &str,
    partition_key: &str,
) -> (Arc<Db>, Vec<ChunkId>) {
    // This test is aimed for at least 3 chunks
    assert!(lp_lines_vec.len() >= 3);

    // First make all OS chunks fot the lp_lins_vec
    // Define they are OS
    let mut chunk_data_vec = vec![];
    for lp_lines in lp_lines_vec {
        let chunk_data = ChunkDataOld {
            lp_lines: lp_lines.clone(),
            chunk_stage: ChunkStageOld::Os,
        };
        chunk_data_vec.push(chunk_data);
    }
    // Make db with those OS chunks
    let scenario = make_different_stage_chunks_with_deletes_scenario_old(
        chunk_data_vec,
        vec![], // not delete anything yet
        table_name,
        partition_key,
    )
    .await;

    // Get chunk ids in contiguous order
    let db = Arc::downcast::<Db>(scenario.db.as_any_arc()).unwrap();
    let partition = db.partition(table_name, partition_key).unwrap();
    let partition = partition.read();
    let mut keyed_chunks: Vec<(_, _)> = partition
        .keyed_chunks()
        .into_iter()
        .map(|(id, order, _chunk)| (id, order))
        .collect();
    keyed_chunks.sort_by(|(_id1, order1), (_id2, order2)| order1.cmp(order2));

    let chunk_ids: Vec<_> = keyed_chunks.iter().map(|(id, _order)| *id).collect();

    (db, chunk_ids)
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
    make_two_chunk_scenarios_old(partition_key, data1, data2)
        .await
        .into_iter()
        .chain(make_two_chunk_scenarios_new(partition_key, data1, data2).await)
        .collect()
}

async fn make_two_chunk_scenarios_old(
    partition_key: &str,
    data1: &str,
    data2: &str,
) -> Vec<DbScenario> {
    let db = make_db().await.db;
    write_lp(&db, data1);
    write_lp(&db, data2);
    let scenario1 = DbScenario {
        scenario_name: "Data in single open chunk of mutable buffer".into(),
        db,
    };

    // spread across 2 mutable buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.rollover_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    write_lp(&db, data2);
    let scenario2 = DbScenario {
        scenario_name: "Data in one open chunk and one closed chunk of mutable buffer".into(),
        db,
    };

    // spread across 1 mutable buffer, 1 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    write_lp(&db, data2);
    let scenario3 = DbScenario {
        scenario_name: "Data in open chunk of mutable buffer, and one chunk of read buffer".into(),
        db,
    };

    // in 2 read buffer chunks
    let db = make_db().await.db;
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2);
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
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        db.persist_partition(table_name, partition_key, true)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2);
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
    let table_names = write_lp(&db, data1);
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
    let table_names = write_lp(&db, data2);
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
    let table_names = write_lp(&db, data1);
    for table_name in &table_names {
        // put chunk 1 into RUB
        db.compact_partition(table_name, partition_key)
            .await
            .unwrap();
    }
    let table_names = write_lp(&db, data2); // write to MUB
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

async fn make_two_chunk_scenarios_new(
    partition_key: &str,
    data1: &str,
    data2: &str,
) -> Vec<DbScenario> {
    let lp_lines1: Vec<_> = data1.split('\n').collect();
    let lp_lines2: Vec<_> = data2.split('\n').collect();

    make_n_chunks_scenario_new(&[
        ChunkDataNew {
            lp_lines: lp_lines1,
            partition_key,
            ..Default::default()
        },
        ChunkDataNew {
            lp_lines: lp_lines2,
            partition_key,
            ..Default::default()
        },
    ])
    .await
}

pub async fn make_n_chunks_scenario_new(chunks: &[ChunkDataNew<'_, '_>]) -> Vec<DbScenario> {
    let n_stages_unset = chunks
        .iter()
        .filter(|chunk| chunk.chunk_stage.is_none())
        .count();

    let mut scenarios = vec![];

    for stages in ChunkStageNew::all()
        .into_iter()
        .combinations_with_replacement(n_stages_unset)
    {
        // filter out unordered stages
        if !stages.windows(2).all(|stages| {
            stages[0]
                .partial_cmp(&stages[1])
                .map(|o| o.is_le())
                .unwrap_or_default()
        }) {
            continue;
        }

        let mut scenario_name = format!("{} chunks:", chunks.len());
        let mut stages_it = stages.iter();
        let mut mock_ingester = MockIngester::new().await;

        for chunk_data in chunks {
            let mut chunk_data = chunk_data.clone();

            if chunk_data.chunk_stage.is_none() {
                let chunk_stage = stages_it.next().expect("generated enough stages");
                chunk_data = chunk_data.with_chunk_stage(*chunk_stage);
            }

            let chunk_data = chunk_data.replace_begin_and_end_delete_times();

            let name = make_ng_chunk(&mut mock_ingester, chunk_data).await;

            write!(&mut scenario_name, ", {}", name).unwrap();
        }

        assert!(stages_it.next().is_none(), "generated too many stages");

        let db = mock_ingester.into_query_namespace().await;
        scenarios.push(DbScenario { scenario_name, db });
    }

    scenarios
}

// // This function loads one chunk of lp data into RUB for testing predicate pushdown
pub(crate) async fn make_one_rub_or_parquet_chunk_scenario(
    partition_key: &str,
    data: &str,
) -> Vec<DbScenario> {
    // Scenario 1: One closed chunk in RUB
    let db = make_db().await.db;
    let table_names = write_lp(&db, data);
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
    let table_names = write_lp(&db, data);
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

/// Create given chunk using the given ingester.
///
/// Returns a human-readable chunk description.
async fn make_ng_chunk(mock_ingester: &mut MockIngester, chunk: ChunkDataNew<'_, '_>) -> String {
    let chunk_stage = chunk.chunk_stage.expect("chunk stage should be set");

    // create chunk
    match chunk_stage {
        ChunkStageNew::Ingester => {
            // process write
            let (op, _partition_ids) = mock_ingester
                .simulate_write_routing(&chunk.lp_lines, chunk.partition_key)
                .await;
            mock_ingester.buffer_operation(op).await;

            // process delete predicates
            if let Some(delete_table_name) = chunk.delete_table_name {
                for pred in &chunk.preds {
                    match pred.delete_time {
                        DeleteTimeNew::Ingester { .. } => {
                            let op = mock_ingester
                                .simulate_delete_routing(delete_table_name, pred.predicate.clone())
                                .await;
                            mock_ingester.buffer_operation(op).await;
                        }
                        other @ DeleteTimeNew::Parquet => {
                            panic!("Cannot have delete time '{other}' for ingester chunk")
                        }
                        DeleteTimeNew::Begin | DeleteTimeNew::End => {
                            unreachable!("Begin/end cases should have been replaced with concrete instances at this point")
                        }
                    }
                }
            }
        }
        ChunkStageNew::Parquet => {
            // process write
            let (op, partition_ids) = mock_ingester
                .simulate_write_routing(&chunk.lp_lines, chunk.partition_key)
                .await;
            mock_ingester.buffer_operation(op).await;

            // model delete predicates that are materialized (applied) by the ingester,
            // during parquet file creation
            let mut tombstone_ids = vec![];
            if let Some(delete_table_name) = chunk.delete_table_name {
                for pred in &chunk.preds {
                    match pred.delete_time {
                        DeleteTimeNew::Ingester { .. } => {
                            let ids_pre = mock_ingester.tombstone_ids(delete_table_name).await;

                            let op = mock_ingester
                                .simulate_delete_routing(delete_table_name, pred.predicate.clone())
                                .await;
                            mock_ingester.buffer_operation(op).await;

                            // tombstones are created immediately, need to remember their ID to handle deletion later
                            let mut tombstone_id = None;
                            for id in mock_ingester.tombstone_ids(delete_table_name).await {
                                if !ids_pre.contains(&id) {
                                    assert!(tombstone_id.is_none(), "Added multiple tombstones?!");
                                    tombstone_id = Some(id);
                                }
                            }
                            tombstone_ids.push(tombstone_id.expect("No tombstone added?!"));
                        }
                        DeleteTimeNew::Parquet => {
                            // will be attached AFTER the chunk was created
                        }
                        DeleteTimeNew::Begin | DeleteTimeNew::End => {
                            unreachable!("Begin/end cases should have been replaced with concrete instances at this point")
                        }
                    }
                }
            }

            mock_ingester.persist(&partition_ids).await;

            // model post-persist delete predicates
            if let Some(delete_table_name) = chunk.delete_table_name {
                let mut id_it = tombstone_ids.iter();
                for pred in &chunk.preds {
                    match pred.delete_time {
                        DeleteTimeNew::Ingester { also_in_catalog } => {
                            // should still have a tombstone
                            let tombstone_id = *id_it.next().unwrap();
                            mock_ingester
                                .catalog
                                .catalog
                                .repositories()
                                .await
                                .tombstones()
                                .get_by_id(tombstone_id)
                                .await
                                .unwrap()
                                .expect("tombstone should be present");

                            if !also_in_catalog {
                                mock_ingester
                                    .catalog
                                    .catalog
                                    .repositories()
                                    .await
                                    .tombstones()
                                    .remove(&[tombstone_id])
                                    .await
                                    .unwrap();
                            }
                        }
                        DeleteTimeNew::Parquet => {
                            // create new tombstone
                            let op = mock_ingester
                                .simulate_delete_routing(delete_table_name, pred.predicate.clone())
                                .await;
                            mock_ingester.buffer_operation(op).await;
                        }
                        DeleteTimeNew::Begin | DeleteTimeNew::End => {
                            unreachable!("Begin/end cases should have been replaced with concrete instances at this point")
                        }
                    }
                }
            }
        }
    }

    let mut name = format!("NG Chunk {}", chunk_stage);
    let n_preds = chunk.preds.len();
    if n_preds > 0 {
        let delete_names: Vec<_> = chunk
            .preds
            .iter()
            .map(|p| p.delete_time.to_string())
            .collect();
        write!(
            name,
            " with {} delete(s) ({})",
            n_preds,
            delete_names.join(", ")
        )
        .unwrap();
    }
    name
}

/// Ingester that can be controlled specifically for query tests.
///
/// This uses as much ingester code as possible but allows more direct control over aspects like lifecycle and
/// partioning.
#[derive(Debug)]
struct MockIngester {
    /// Test catalog state.
    catalog: Arc<TestCatalog>,

    /// Namespace used for testing.
    ns: Arc<TestNamespace>,

    /// Sequencer used for testing.
    sequencer: Arc<TestSequencer>,

    /// Special partitioner that lets us control to which partition we write.
    partitioner: Arc<ConstantPartitioner>,

    /// Memory of partition keys for certain sequence numbers.
    ///
    /// This is currently required because [`DmlWrite`] does not carry partiion information so we need to do that. In
    /// production this is not required because the router and the ingester use the same partition logic, but we need
    /// direct control over the partion key for the query tests.
    partition_keys: HashMap<SequenceNumber, String>,

    /// Ingester state.
    ingester_data: Arc<IngesterData>,

    /// Next sequence number.
    ///
    /// This is kinda a poor-mans write buffer.
    sequence_counter: u64,
}

impl MockIngester {
    /// Create new empty ingester.
    async fn new() -> Self {
        let catalog = TestCatalog::new();
        let ns = catalog.create_namespace("test_db").await;
        let sequencer = ns.create_sequencer(1).await;

        let sequencers = BTreeMap::from([(
            sequencer.sequencer.id,
            SequencerData::new(
                sequencer.sequencer.kafka_partition,
                catalog.metric_registry(),
            ),
        )]);
        let partitioner = Arc::new(ConstantPartitioner::default());
        let ingester_data = Arc::new(IngesterData::new(
            catalog.object_store(),
            catalog.catalog(),
            sequencers,
            Arc::clone(&partitioner) as _,
            catalog.exec(),
            BackoffConfig::default(),
        ));

        Self {
            catalog,
            ns,
            sequencer,
            partitioner,
            partition_keys: Default::default(),
            ingester_data,
            sequence_counter: 0,
        }
    }

    /// Buffer up [`DmlOperation`] in ingester memory.
    ///
    /// This will never persist.
    ///
    /// Takes `&self mut` because our partioning implementation does not work with concurrent access.
    async fn buffer_operation(&mut self, dml_operation: DmlOperation) {
        let lifecycle_handle = NoopLifecycleHandle {};

        // set up partitioner for writes
        if matches!(dml_operation, DmlOperation::Write(_)) {
            let sequence_number = SequenceNumber::new(
                dml_operation.meta().sequence().unwrap().sequence_number as i64,
            );
            self.partitioner
                .set(self.partition_keys.get(&sequence_number).unwrap().clone());
        }

        let should_pause = self
            .ingester_data
            .buffer_operation(
                self.sequencer.sequencer.id,
                dml_operation,
                &lifecycle_handle,
            )
            .await
            .unwrap();
        assert!(!should_pause);
    }

    /// Persists the given set of partitions.
    async fn persist(&mut self, partition_ids: &[PartitionId]) {
        for partition_id in partition_ids {
            self.ingester_data.persist(*partition_id).await;
        }
    }

    /// Draws new sequence number.
    fn next_sequence_number(&mut self) -> u64 {
        let next = self.sequence_counter;
        self.sequence_counter += 1;
        next
    }

    /// Simulate what the router would do when it encounters the given set of line protocol lines.
    ///
    /// In contrast to a real router we have tight control over the partition key here.
    async fn simulate_write_routing(
        &mut self,
        lp_lines: &[&str],
        partition_key: &str,
    ) -> (DmlOperation, Vec<PartitionId>) {
        // detect table names and schemas from LP lines
        let mut converter = LinesConverter::new(0);
        for lp_line in lp_lines {
            converter.write_lp(lp_line).unwrap();
        }
        let (mutable_batches, _stats) = converter.finish().unwrap();

        // set up catalog
        let tables = {
            // sort names so that IDs are deterministic
            let mut table_names: Vec<_> = mutable_batches.keys().cloned().collect();
            table_names.sort();

            let mut tables = vec![];
            for table_name in table_names {
                let table = self.ns.create_table(&table_name).await;
                tables.push(table);
            }
            tables
        };
        let mut partition_ids = vec![];
        for table in &tables {
            let partition = table
                .with_sequencer(&self.sequencer)
                .create_partition(partition_key)
                .await;
            partition_ids.push(partition.partition.id);
        }
        for table in tables {
            let schema = mutable_batches
                .get(&table.table.name)
                .unwrap()
                .schema(Selection::All)
                .unwrap();

            for (t, field) in schema.iter() {
                let t = t.unwrap();
                table.create_column(field.name(), t.into()).await;
            }
        }

        let sequence_number = self.next_sequence_number();
        self.partition_keys.insert(
            SequenceNumber::new(sequence_number as i64),
            partition_key.to_string(),
        );
        let meta = DmlMeta::sequenced(
            Sequence::new(self.sequencer.sequencer.id.get() as u32, sequence_number),
            self.catalog.time_provider().now(),
            None,
            0,
        );
        let op = DmlOperation::Write(DmlWrite::new(
            self.ns.namespace.name.clone(),
            mutable_batches,
            meta,
        ));
        (op, partition_ids)
    }

    /// Simulates what the router would do when it encouters the given delete predicate.
    async fn simulate_delete_routing(
        &mut self,
        delete_table_name: &str,
        predicate: DeletePredicate,
    ) -> DmlOperation {
        // ensure that table for deletions is also present in the catalog
        self.ns.create_table(delete_table_name).await;

        let sequence_number = self.next_sequence_number();
        let meta = DmlMeta::sequenced(
            Sequence::new(self.sequencer.sequencer.id.get() as u32, sequence_number),
            self.catalog.time_provider().now(),
            None,
            0,
        );
        DmlOperation::Delete(DmlDelete::new(
            self.ns.namespace.name.clone(),
            predicate,
            Some(NonEmptyString::new(delete_table_name).unwrap()),
            meta,
        ))
    }

    /// Get the current set of tombstone IDs for the given table.
    async fn tombstone_ids(&self, table_name: &str) -> Vec<TombstoneId> {
        let table_id = self
            .catalog
            .catalog
            .repositories()
            .await
            .tables()
            .create_or_get(table_name, self.ns.namespace.id)
            .await
            .unwrap()
            .id;
        let tombstones = self
            .catalog
            .catalog
            .repositories()
            .await
            .tombstones()
            .list_by_table(table_id)
            .await
            .unwrap();
        tombstones.iter().map(|t| t.id).collect()
    }

    /// Finalizes the ingester and creates a querier namespace that can be used for query tests.
    ///
    /// The querier namespace will hold a simulated connection to the ingester to be able to query unpersisted data.
    async fn into_query_namespace(self) -> Arc<QuerierNamespace> {
        let mut repos = self.catalog.catalog.repositories().await;
        let schema = Arc::new(
            get_schema_by_name(&self.ns.namespace.name, repos.as_mut())
                .await
                .unwrap(),
        );

        let catalog = Arc::clone(&self.catalog);
        let ns = Arc::clone(&self.ns);
        let catalog_cache = Arc::new(QuerierCatalogCache::new(
            self.catalog.catalog(),
            self.catalog.time_provider(),
        ));
        let ingester_connection = IngesterConnectionImpl::new_with_flight_client(
            vec![String::from("some_address")],
            Arc::new(self),
            Arc::clone(&catalog_cache),
        );
        let ingester_connection = Arc::new(ingester_connection);

        Arc::new(QuerierNamespace::new_testing(
            catalog_cache,
            catalog.object_store(),
            catalog.metric_registry(),
            ns.namespace.name.clone().into(),
            schema,
            catalog.exec(),
            ingester_connection,
        ))
    }
}

/// Special [`LifecycleHandle`] that never persists and always accepts more data.
///
/// This is useful to control persists manually.
struct NoopLifecycleHandle {}

impl LifecycleHandle for NoopLifecycleHandle {
    fn log_write(
        &self,
        _partition_id: PartitionId,
        _sequencer_id: SequencerId,
        _sequence_number: SequenceNumber,
        _bytes_written: usize,
    ) -> bool {
        // do NOT pause ingest
        false
    }

    fn can_resume_ingest(&self) -> bool {
        true
    }
}

/// Special partitioner that returns a constant values.
#[derive(Debug, Default)]
struct ConstantPartitioner {
    partition_key: Mutex<String>,
}

impl ConstantPartitioner {
    /// Set partition key.
    fn set(&self, partition_key: String) {
        *self.partition_key.lock().unwrap() = partition_key;
    }
}

impl Partitioner for ConstantPartitioner {
    fn partition_key(&self, _batch: &MutableBatch) -> Result<String, PartitionerError> {
        Ok(self.partition_key.lock().unwrap().clone())
    }
}

#[async_trait]
impl IngesterFlightClient for MockIngester {
    async fn query(
        &self,
        _ingester_address: &str,
        request: IngesterQueryRequest,
    ) -> Result<Box<dyn IngesterFlightClientQueryData>, IngesterFlightClientError> {
        // NOTE: we MUST NOT unwrap errors here because some query tests assert error behavior (e.g. passing predicates
        // of wrong types)
        let response = prepare_data_to_querier(&self.ingester_data, &request)
            .await
            .map_err(|e| IngesterFlightClientError::Flight {
                source: FlightError::ArrowError(arrow::error::ArrowError::ExternalError(Box::new(
                    e,
                ))),
            })?;

        Ok(Box::new(QueryDataAdapter::new(response)))
    }
}

/// Helper struct to present [`IngesterQueryResponse`] (produces by the ingester) as a [`IngesterFlightClientQueryData`]
/// (used by the querier) without doing any real gRPC IO.
#[derive(Debug)]
struct QueryDataAdapter {
    response: IngesterQueryResponse,
    app_metadata: IngesterQueryResponseMetadata,
}

impl QueryDataAdapter {
    /// Create new adapter.
    ///
    /// This pre-calculates some data structure that we are going to need later.
    fn new(response: IngesterQueryResponse) -> Self {
        let app_metadata = IngesterQueryResponseMetadata {
            unpersisted_partitions: response
                .unpersisted_partitions
                .iter()
                .map(|(id, status)| {
                    (
                        id.get(),
                        PartitionStatus {
                            parquet_max_sequence_number: status
                                .parquet_max_sequence_number
                                .map(|x| x.get()),
                            tombstone_max_sequence_number: status
                                .tombstone_max_sequence_number
                                .map(|x| x.get()),
                        },
                    )
                })
                .collect(),
            batch_partition_ids: response
                .batch_partition_ids
                .iter()
                .map(|id| id.get())
                .collect(),
        };

        Self {
            response,
            app_metadata,
        }
    }
}

#[async_trait]
impl IngesterFlightClientQueryData for QueryDataAdapter {
    async fn next(&mut self) -> Result<Option<RecordBatch>, FlightError> {
        Ok(self.response.data.next().await.map(|x| x.unwrap()))
    }

    fn app_metadata(&self) -> &IngesterQueryResponseMetadata {
        &self.app_metadata
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.response.schema.as_arrow()
    }
}
