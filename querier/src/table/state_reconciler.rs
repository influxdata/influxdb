//! Logic to reconcile the catalog and ingester state
//!
//! # Usage
//!
//! The code in this module should be used like this:
//!
//! 1. **Ingester Request:** Request data from ingester(s). This will create [`IngesterPartition`]s.
//! 2. **Catalog Query:** Query parquet files and tombstones from catalog. It is important that
//!    this happens AFTER the ingester request. This will create [`ParquetFileWithMetadata`] and
//!    [`Tombstone`].
//! 3. **Pruning:** Call [`filter_parquet_files`] and [`tombstone_exclude_list`] to filter out
//!    files and tombstones that are too new (i.e. were created between step 1 and 2).

mod interface;

use data_types::{ParquetFileWithMetadata, PartitionId, SequencerId, Tombstone, TombstoneId};
use iox_query::QueryChunk;
use observability_deps::tracing::debug;
use snafu::Snafu;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{chunk::ParquetChunkAdapter, tombstone::QuerierTombstone, IngesterPartition};

use self::interface::{IngesterPartitionInfo, ParquetFileInfo, TombstoneInfo};

#[derive(Snafu, Debug)]
pub enum ReconcileError {
    #[snafu(display("Compactor processed file that the querier would need to split apart which is not yet implemented"))]
    CompactorConflict,
}

/// Handles reconciling catalog and ingester state.
#[derive(Debug)]
pub struct Reconciler {
    table_name: Arc<str>,
    namespace_name: Arc<str>,
    chunk_adapter: Arc<ParquetChunkAdapter>,
}

impl Reconciler {
    pub(crate) fn new(
        table_name: Arc<str>,
        namespace_name: Arc<str>,
        chunk_adapter: Arc<ParquetChunkAdapter>,
    ) -> Self {
        Self {
            table_name,
            namespace_name,
            chunk_adapter,
        }
    }

    /// Reconciles ingester state (ingester_partitions) and catalog
    /// state (parquet_files and tombstones), producing a list of
    /// chunks to query
    pub(crate) async fn reconcile(
        &self,
        ingester_partitions: Vec<Arc<IngesterPartition>>,
        tombstones: Vec<Tombstone>,
        parquet_files: Vec<ParquetFileWithMetadata>,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, ReconcileError> {
        let tombstone_exclusion = tombstone_exclude_list(&ingester_partitions, &tombstones);

        let querier_tombstones: Vec<_> =
            tombstones.into_iter().map(QuerierTombstone::from).collect();

        // match chunks and tombstones
        let mut tombstones_by_sequencer: HashMap<SequencerId, Vec<QuerierTombstone>> =
            HashMap::new();

        for tombstone in querier_tombstones {
            tombstones_by_sequencer
                .entry(tombstone.sequencer_id())
                .or_default()
                .push(tombstone);
        }

        //
        let parquet_files = filter_parquet_files(&ingester_partitions, parquet_files)?;

        debug!(
            ?parquet_files,
            namespace=%self.namespace_name(),
            table_name=%self.table_name(),
            "Parquet files after filtering"
        );

        // convert parquet files and tombstones into QuerierChunks
        let mut parquet_chunks = Vec::with_capacity(parquet_files.len());
        for parquet_file_with_metadata in parquet_files {
            if let Some(chunk) = self
                .chunk_adapter
                .new_querier_chunk(parquet_file_with_metadata)
                .await
            {
                parquet_chunks.push(chunk);
            }
        }
        debug!(num_chunks=%parquet_chunks.len(), "Created parquet chunks");

        let mut chunks: Vec<Arc<dyn QueryChunk>> =
            Vec::with_capacity(parquet_chunks.len() + ingester_partitions.len());

        for chunk in parquet_chunks.into_iter() {
            let chunk = if let Some(tombstones) =
                tombstones_by_sequencer.get(&chunk.meta().sequencer_id())
            {
                let mut delete_predicates = Vec::with_capacity(tombstones.len());
                for tombstone in tombstones {
                    // check conditions that don't need catalog access first to avoid unnecessary catalog load

                    // Check if tombstone should be excluded based on the ingester response
                    if tombstone_exclusion
                        .contains(&(chunk.meta().partition_id(), tombstone.tombstone_id()))
                    {
                        continue;
                    }

                    // Check if tombstone even applies to the sequence number range within the parquet file. There
                    // are the following cases here:
                    //
                    // 1. Tombstone comes before chunk min sequencer number:
                    //    There is no way the tombstone can affect the chunk.
                    // 2. Tombstone comes after chunk max sequencer number:
                    //    Tombstone affects whole chunk (it might be marked as processed though, we'll check that
                    //    further down).
                    // 3. Tombstone is in the min-max sequencer number range of the chunk:
                    //    Technically the querier has NO way to determine the rows that are affected by the tombstone
                    //    since we have no row-level sequence numbers. Such a file can be created by two sources -- the
                    //    ingester and the compactor. The ingester must have materialized the tombstone while creating
                    //    the parquet file, so the querier can skip it. The compactor also materialized the tombstones,
                    //    so we can skip it as well. In the compactor case the tombstone will even be marked as
                    //    processed.
                    //
                    // So the querier only needs to consider the tombstone in case 2.
                    if tombstone.sequence_number() <= chunk.meta().max_sequence_number() {
                        continue;
                    }

                    // TODO: also consider time ranges (https://github.com/influxdata/influxdb_iox/issues/4086)

                    // check if tombstone is marked as processed
                    if self
                        .chunk_adapter
                        .catalog_cache()
                        .processed_tombstones()
                        .exists(
                            chunk
                                .parquet_file_id()
                                .expect("just created from a parquet file"),
                            tombstone.tombstone_id(),
                        )
                        .await
                    {
                        continue;
                    }

                    delete_predicates.push(Arc::clone(tombstone.delete_predicate()));
                }
                chunk.with_delete_predicates(delete_predicates)
            } else {
                chunk
            };

            chunks.push(Arc::new(chunk) as Arc<dyn QueryChunk>);
        }

        // Add ingester chunks to the overall chunk list.
        // - filter out chunks that don't have any record batches
        // - tombstones don't need to be applied since they were already materialized by the ingester
        let ingester_chunks = ingester_partitions
            .into_iter()
            .filter(|c| c.has_batches())
            .map(|c| c as Arc<dyn QueryChunk>);

        chunks.extend(ingester_chunks);
        debug!(num_chunks=%chunks.len(), "Final chunk count after reconcilation");

        Ok(chunks)
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    #[must_use]
    pub fn namespace_name(&self) -> &str {
        self.namespace_name.as_ref()
    }
}

/// Filter out parquet files that contain "too new" data.
///
/// The caller may only use the returned parquet files.
///
/// This will remove files that are part of the catalog but that contain data that the ingester persisted AFTER the
/// querier contacted it. See module-level documentation about the order in which the communication and the information
/// processing should take place.
///
/// Note that the querier (and this method) do NOT care about the actual age of the parquet files, since the compactor
/// is free to to process files at any given moment (e.g. to combine them or to materialize tombstones). However if the
/// compactor combines files in a way that the querier would need to split it into "desired" data and "too new" data
/// then we will currently bail out with [`ReconcileError`].
fn filter_parquet_files<I, P>(
    ingester_partitions: &[I],
    parquet_files: Vec<P>,
) -> Result<Vec<P>, ReconcileError>
where
    I: IngesterPartitionInfo,
    P: ParquetFileInfo,
{
    // Build partition-based lookup table.
    //
    // Note that we don't need to take the sequencer ID into account here because each partition is not only bound to a
    // table but also to a sequencer.
    let lookup_table: HashMap<PartitionId, &I> = ingester_partitions
        .iter()
        .map(|i| (i.partition_id(), i))
        .collect();

    // we assume that we filter out a minimal amount of files, so we can use the same capacity
    let mut result = Vec::with_capacity(parquet_files.len());

    for file in parquet_files {
        if let Some(ingester_partition) = lookup_table.get(&file.partition_id()) {
            if let Some(persisted_max) = ingester_partition.parquet_max_sequence_number() {
                debug!(
                    file_partition_id=%file.partition_id(),
                    file_max_seq_num=%file.max_sequence_number().get(),
                    file_min_seq_num=%file.min_sequence_number().get(),
                    persisted_max=%persisted_max.get(),
                    "Comparing parquet file and ingester parquet max"
                );
                if (file.max_sequence_number() > persisted_max)
                    && (file.min_sequence_number() <= persisted_max)
                {
                    return Err(ReconcileError::CompactorConflict);
                }
                if file.max_sequence_number() > persisted_max {
                    // filter out, file is newer
                    continue;
                }
            } else {
                debug!(
                    file_partition_id=%file.partition_id(),
                    file_max_seq_num=%file.max_sequence_number().get(),
                    file_min_seq_num=%file.min_sequence_number().get(),
                    "ingester thinks it doesn't have data persisted yet"
                );
                // ingester thinks it doesn't have any data persisted yet => can safely ignore file
                continue;
            }
        } else {
            debug!(
                file_partition_id=%file.partition_id(),
                file_max_seq_num=%file.max_sequence_number().get(),
                file_min_seq_num=%file.min_sequence_number().get(),
                "partition was not flagged by the ingester as unpersisted"
            );
            // partition was not flagged by the ingester as "unpersisted", so we can keep the parquet file
        }

        result.push(file);
    }

    Ok(result)
}

/// Generates "exclude" filter for tombstones.
///
/// Since tombstones are sequencer-wide but data persistence is partition-based (which are sub-units of sequencers), we
/// cannot just remove tombstones entirely but need to decide on a per-partition basis. This function generates a lookup
/// table of partition-tombstone tuples that later need to be EXCLUDED/IGNORED when pairing tombstones with chunks.
fn tombstone_exclude_list<I, T>(
    ingester_partitions: &[I],
    tombstones: &[T],
) -> HashSet<(PartitionId, TombstoneId)>
where
    I: IngesterPartitionInfo,
    T: TombstoneInfo,
{
    // Build sequencer-based lookup table.
    let mut lookup_table: HashMap<SequencerId, Vec<&I>> = HashMap::default();
    for partition in ingester_partitions {
        lookup_table
            .entry(partition.sequencer_id())
            .or_default()
            .push(partition);
    }

    let mut exclude = HashSet::new();
    for t in tombstones {
        if let Some(partitions) = lookup_table.get(&t.sequencer_id()) {
            for p in partitions {
                if let Some(persisted_max) = p.tombstone_max_sequence_number() {
                    if t.sequence_number() > persisted_max {
                        // newer than persisted => exclude
                        exclude.insert((p.partition_id(), t.id()));
                    } else {
                        // in persisted range => keep
                    }
                } else {
                    // partition has no persisted data at all => need to exclude tombstone which is too new
                    exclude.insert((p.partition_id(), t.id()));
                }
            }
        }
    }

    exclude
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::SequenceNumber;

    use super::*;

    #[test]
    fn test_filter_parquet_files_empty() {
        let actual =
            filter_parquet_files::<MockIngesterPartitionInfo, MockParquetFileInfo>(&[], vec![])
                .unwrap();
        assert_eq!(actual, vec![]);
    }

    #[test]
    fn test_filter_parquet_files_compactor_conflict() {
        let ingester_partitions = &[MockIngesterPartitionInfo {
            partition_id: PartitionId::new(1),
            sequencer_id: SequencerId::new(1),
            parquet_max_sequence_number: Some(SequenceNumber::new(10)),
            tombstone_max_sequence_number: None,
        }];
        let parquet_files = vec![MockParquetFileInfo {
            partition_id: PartitionId::new(1),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(11),
        }];
        let err = filter_parquet_files(ingester_partitions, parquet_files).unwrap_err();
        assert_matches!(err, ReconcileError::CompactorConflict);
    }

    #[test]
    fn test_filter_parquet_files_many() {
        let ingester_partitions = &[
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(1),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: Some(SequenceNumber::new(10)),
                tombstone_max_sequence_number: None,
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(2),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: None,
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(3),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: Some(SequenceNumber::new(3)),
                tombstone_max_sequence_number: None,
            },
        ];
        let pf11 = MockParquetFileInfo {
            partition_id: PartitionId::new(1),
            min_sequence_number: SequenceNumber::new(3),
            max_sequence_number: SequenceNumber::new(9),
        };
        let pf12 = MockParquetFileInfo {
            partition_id: PartitionId::new(1),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(10),
        };
        // filtered because it was persisted after ingester sent response (11 > 10)
        let pf13 = MockParquetFileInfo {
            partition_id: PartitionId::new(1),
            min_sequence_number: SequenceNumber::new(11),
            max_sequence_number: SequenceNumber::new(20),
        };
        let pf2 = MockParquetFileInfo {
            partition_id: PartitionId::new(2),
            min_sequence_number: SequenceNumber::new(0),
            max_sequence_number: SequenceNumber::new(0),
        };
        let pf31 = MockParquetFileInfo {
            partition_id: PartitionId::new(3),
            min_sequence_number: SequenceNumber::new(1),
            max_sequence_number: SequenceNumber::new(3),
        };
        // filtered because it was persisted after ingester sent response (4 > 3)
        let pf32 = MockParquetFileInfo {
            partition_id: PartitionId::new(3),
            min_sequence_number: SequenceNumber::new(4),
            max_sequence_number: SequenceNumber::new(5),
        };
        // passed because it came from a partition (4) the ingester didn't know about
        let pf4 = MockParquetFileInfo {
            partition_id: PartitionId::new(4),
            min_sequence_number: SequenceNumber::new(0),
            max_sequence_number: SequenceNumber::new(0),
        };
        let parquet_files = vec![
            pf11.clone(),
            pf12.clone(),
            pf13,
            pf2,
            pf31.clone(),
            pf32,
            pf4.clone(),
        ];
        let actual = filter_parquet_files(ingester_partitions, parquet_files).unwrap();
        let expected = vec![pf11, pf12, pf31, pf4];
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_filter_tombstones_empty() {
        let actual =
            tombstone_exclude_list::<MockIngesterPartitionInfo, MockTombstoneInfo>(&[], &[]);
        assert!(actual.is_empty());
    }

    #[test]
    fn test_filter_tombstones_many() {
        let ingester_partitions = &[
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(1),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(10)),
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(2),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: None,
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(3),
                sequencer_id: SequencerId::new(1),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(3)),
            },
            MockIngesterPartitionInfo {
                partition_id: PartitionId::new(4),
                sequencer_id: SequencerId::new(2),
                parquet_max_sequence_number: None,
                tombstone_max_sequence_number: Some(SequenceNumber::new(7)),
            },
        ];
        let tombstones = &[
            MockTombstoneInfo {
                id: TombstoneId::new(1),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(2),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(2),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(3),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(3),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(4),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(4),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(9),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(5),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(10),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(6),
                sequencer_id: SequencerId::new(1),
                sequence_number: SequenceNumber::new(11),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(7),
                sequencer_id: SequencerId::new(2),
                sequence_number: SequenceNumber::new(6),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(8),
                sequencer_id: SequencerId::new(2),
                sequence_number: SequenceNumber::new(7),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(9),
                sequencer_id: SequencerId::new(2),
                sequence_number: SequenceNumber::new(8),
            },
            MockTombstoneInfo {
                id: TombstoneId::new(10),
                sequencer_id: SequencerId::new(3),
                sequence_number: SequenceNumber::new(10),
            },
        ];

        let actual = tombstone_exclude_list(ingester_partitions, tombstones);
        let expected = HashSet::from([
            (PartitionId::new(1), TombstoneId::new(6)),
            (PartitionId::new(2), TombstoneId::new(1)),
            (PartitionId::new(2), TombstoneId::new(2)),
            (PartitionId::new(2), TombstoneId::new(3)),
            (PartitionId::new(2), TombstoneId::new(4)),
            (PartitionId::new(2), TombstoneId::new(5)),
            (PartitionId::new(2), TombstoneId::new(6)),
            (PartitionId::new(3), TombstoneId::new(3)),
            (PartitionId::new(3), TombstoneId::new(4)),
            (PartitionId::new(3), TombstoneId::new(5)),
            (PartitionId::new(3), TombstoneId::new(6)),
            (PartitionId::new(4), TombstoneId::new(9)),
        ]);
        assert_eq!(actual, expected);
    }

    #[derive(Debug)]
    struct MockIngesterPartitionInfo {
        partition_id: PartitionId,
        sequencer_id: SequencerId,
        parquet_max_sequence_number: Option<SequenceNumber>,
        tombstone_max_sequence_number: Option<SequenceNumber>,
    }

    impl IngesterPartitionInfo for MockIngesterPartitionInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn sequencer_id(&self) -> SequencerId {
            self.sequencer_id
        }

        fn parquet_max_sequence_number(&self) -> Option<SequenceNumber> {
            self.parquet_max_sequence_number
        }

        fn tombstone_max_sequence_number(&self) -> Option<SequenceNumber> {
            self.tombstone_max_sequence_number
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct MockParquetFileInfo {
        partition_id: PartitionId,
        min_sequence_number: SequenceNumber,
        max_sequence_number: SequenceNumber,
    }

    impl ParquetFileInfo for MockParquetFileInfo {
        fn partition_id(&self) -> PartitionId {
            self.partition_id
        }

        fn min_sequence_number(&self) -> SequenceNumber {
            self.min_sequence_number
        }

        fn max_sequence_number(&self) -> SequenceNumber {
            self.max_sequence_number
        }
    }

    #[derive(Debug)]
    struct MockTombstoneInfo {
        id: TombstoneId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
    }

    impl TombstoneInfo for MockTombstoneInfo {
        fn id(&self) -> TombstoneId {
            self.id
        }

        fn sequencer_id(&self) -> SequencerId {
            self.sequencer_id
        }

        fn sequence_number(&self) -> SequenceNumber {
            self.sequence_number
        }
    }
}
