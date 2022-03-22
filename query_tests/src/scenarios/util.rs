//! This module contains util functions for testing scenarios
use super::DbScenario;
use data_types::{chunk_metadata::ChunkId, delete_predicate::DeletePredicate};
use db::test_helpers::chunk_ids_rub;
use db::{
    test_helpers::write_lp,
    utils::{count_mub_table_chunks, count_os_table_chunks, count_rub_table_chunks, make_db},
    Db,
};
use query::QueryChunk;
use schema::merge::SchemaMerger;
use schema::selection::Selection;
use std::collections::BTreeMap;
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

#[derive(Debug, Clone)]
pub struct ChunkDataNew<'a> {
    /// Line protocol data of this chunk
    pub lp_lines: Vec<&'a str>,
    /// which stage this chunk will be created
    pub chunk_stage: ChunkStageNew,
}

#[derive(Debug, Clone)]
pub enum ChunkData<'a> {
    Old(ChunkDataOld<'a>),
    New(ChunkDataNew<'a>),
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

#[derive(Debug, Clone, Copy)]
pub enum ChunkStageNew {
    /// In parquet file.
    Parquet,
}

impl Display for ChunkStageNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parquet => write!(f, "Parquet"),
        }
    }
}

impl ChunkStageNew {
    /// return the list of all chunk types
    pub fn all() -> Vec<Self> {
        vec![Self::Parquet]
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
    predicate: &'a DeletePredicate,
    /// At which chunk stage this predicate is applied
    delete_time: DeleteTimeNew,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteTimeNew {
    /// Delete  predicate is added to chunks at their parquet stage
    Parquet,
}

impl DeleteTimeNew {
    /// Return all DeleteTime at and after the given chunk stage
    pub fn all_from_and_before(chunk_stage: ChunkStageNew) -> Vec<DeleteTimeNew> {
        match chunk_stage {
            ChunkStageNew::Parquet => vec![Self::Parquet],
        }
    }

    pub fn begin() -> Self {
        Self::Parquet
    }

    pub fn end() -> Self {
        Self::Parquet
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteTime {
    Old(DeleteTimeOld),
    New(DeleteTimeNew),
}

impl DeleteTime {
    /// Return all DeleteTime at and after the given chunk stage
    pub fn all_from_and_before(chunk_stage: ChunkStage) -> Vec<Self> {
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

    pub fn begin_for(chunk_stage: ChunkStage) -> Self {
        match chunk_stage {
            ChunkStage::Old(_) => Self::Old(DeleteTimeOld::begin()),
            ChunkStage::New(_) => Self::New(DeleteTimeNew::begin()),
        }
    }

    pub fn end_for(chunk_stage: ChunkStage) -> Self {
        match chunk_stage {
            ChunkStage::Old(_) => Self::Old(DeleteTimeOld::end()),
            ChunkStage::New(_) => Self::New(DeleteTimeNew::end()),
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
                delete_table_name,
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
    delete_table_name: &str,
    partition_key: &str,
) -> DbScenario {
    use iox_tests::util::TestCatalog;
    use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
    use querier::{cache::CatalogCache, namespace::QuerierNamespace};

    // detect table names and schemas from LP lines
    let (lp_lines_grouped, schemas) = {
        let mut lp_lines_grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut schemas: BTreeMap<_, SchemaMerger> = BTreeMap::new();

        for lp in lp_lines {
            let (table_name, batch) = lp_to_mutable_batch(lp);

            lp_lines_grouped
                .entry(table_name.clone())
                .or_default()
                .push(lp);

            let schema = batch.schema(Selection::All).unwrap();
            let merger = schemas.entry(table_name).or_default();
            *merger = merger.clone().merge(&schema).unwrap();
        }

        let schemas: BTreeMap<_, _> = schemas
            .into_iter()
            .map(|(table_name, merger)| (table_name, merger.build()))
            .collect();

        (lp_lines_grouped, schemas)
    };

    // set up catalog
    let catalog = TestCatalog::new();
    let ns = catalog.create_namespace("test_db").await;
    let sequencer = ns.create_sequencer(1).await;
    let tables = {
        // need to use a temporary vector because BTree iterators ain't `Send`
        let table_names: Vec<_> = lp_lines_grouped.keys().cloned().collect();

        let mut tables = BTreeMap::new();
        for table_name in table_names {
            let table = ns.create_table(&table_name).await;
            tables.insert(table_name, table);
        }
        tables
    };
    let partitions = {
        // need to use a temporary vector because BTree iterators ain't `Send`
        let tables: Vec<_> = tables.values().cloned().collect();

        let mut partitions = BTreeMap::new();
        for table in tables {
            let partition = table
                .with_sequencer(&sequencer)
                .create_partition(partition_key)
                .await;
            partitions.insert(table.table.name.clone(), partition);
        }
        partitions
    };
    for (table_name, schema) in schemas {
        let table = tables.get(&table_name).unwrap();

        for (t, field) in schema.iter() {
            let t = t.unwrap();
            table.create_column(field.name(), t.into()).await;
        }
    }

    // create chunks
    match chunk_stage {
        ChunkStageNew::Parquet => {
            // need to use a temporary vector because BTree iterators ain't `Send`
            let lp_lines_grouped: Vec<_> = lp_lines_grouped.into_iter().collect();

            for (table_name, lp_lines) in lp_lines_grouped {
                let partition = partitions.get(&table_name).unwrap();
                let min_seq = 1;
                let max_seq = 1;
                let min_time = 0;
                let max_time = 0;
                partition
                    .create_parquet_file_with_min_max(
                        &lp_lines.join("\n"),
                        min_seq,
                        max_seq,
                        min_time,
                        max_time,
                    )
                    .await;
            }
        }
    }

    // attach delete predicates
    let n_preds = preds.len();
    if let Some(table) = tables.get(delete_table_name) {
        for (i, pred) in preds.into_iter().enumerate() {
            match pred.delete_time {
                DeleteTimeNew::Parquet => {
                    // parquet files got created w/ sequence number = 1
                    let sequence_number = 2 + i;

                    let min_time = pred.predicate.range.start();
                    let max_time = pred.predicate.range.end();
                    let predicate = pred.predicate.expr_sql_string();
                    table
                        .with_sequencer(&sequencer)
                        .create_tombstone(sequence_number as i64, min_time, max_time, &predicate)
                        .await;
                }
            }
        }
    }

    let catalog_cache = Arc::new(CatalogCache::new(
        catalog.catalog(),
        catalog.time_provider(),
    ));
    let db = Arc::new(QuerierNamespace::new(
        catalog_cache,
        ns.namespace.name.clone().into(),
        ns.namespace.id,
        catalog.metric_registry(),
        catalog.object_store(),
        catalog.time_provider(),
        catalog.exec(),
    ));
    db.sync().await;

    let scenario_name = format!("NG Chunk {} with {} deletes", chunk_stage, n_preds);
    DbScenario { scenario_name, db }
}

/// Build many chunks which are in different stages
//  Note that, after a lot of thoughts, I decided to have 2 separated functions, this one and the one above.
//  The above tests delete predicates before and/or after a chunk is moved to different stages, while
//  this function tests different-stage chunks in various stages when one or many deletes happen.
//  Even though these 2 functions have some overlapped code, merging them in one
//  function will created a much more complicated cases to handle
pub async fn make_different_stage_chunks_with_deletes_scenario(
    _data: Vec<ChunkData<'_>>,
    _preds: Vec<&DeletePredicate>,
    _table_name: &str,
    _partition_key: &str,
) -> DbScenario {
    // this is used by `delete.rs` but currently that only generates OG data
    unimplemented!()
}

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

pub async fn make_contiguous_os_chunks(
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

/// This function loads one chunk of lp data into MUB only
///
pub(crate) async fn make_one_chunk_mub_scenario(data: &str) -> Vec<DbScenario> {
    // Scenario 1: One open chunk in MUB
    let db = make_db().await.db;
    write_lp(&db, data);
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
    let table_names = write_lp(&db, data);
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
