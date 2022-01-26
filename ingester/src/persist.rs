//! Persist compacted data to parquet files in object storage

use datafusion::physical_plan::SendableRecordBatchStream;
use iox_catalog::interface::{Catalog, NamespaceId, PartitionId, SequencerId, TableId};
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use snafu::Snafu;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {}

/// A specialized `Error` for Ingester's persistence errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write the given data to the given location in the given object storage
pub fn persist(
    namespace_id: NamespaceId,
    table_id: TableId,
    sequencer_id: SequencerId,
    partition_id: PartitionId,
    data: SendableRecordBatchStream,
    object_store: &ObjectStore,
    iox_catalog: &dyn Catalog,
) -> Result<()> {
    let parquet_file_object_store_id = Uuid::new_v4();

    let mut parquet_file_object_store_path = object_store.new_path();
    parquet_file_object_store_path.push_all_dirs(&[
        namespace_id.to_string().as_str(),
        table_id.to_string().as_str(),
        sequencer_id.to_string().as_str(),
        partition_id.to_string().as_str(),
    ]);
    parquet_file_object_store_path
        .set_file_name(format!("{}.parquet", parquet_file_object_store_id));

    Ok(())
}
