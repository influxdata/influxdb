use std::sync::Arc;

use snafu::{ResultExt, Snafu};

use crate::{
    plan::stringset::{Error as StringSetError, StringSetPlan, StringSetPlanBuilder},
    predicate::Predicate,
    Database, PartitionChunk,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC planner got error making table_name plan for chunk: {}", source))]
    TableNamePlan {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error listing partition keys: {}", source))]
    ListingPartitions {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unsupported predicate in gRPC table_names: {:?}", predicate))]
    UnsupportedPredicate { predicate: Predicate },

    #[snafu(display(
        "gRPC planner got error checking if chunk {} could pass predicate: {}",
        chunk_id,
        source
    ))]
    CheckingChunkPredicate {
        chunk_id: u32,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error creating string set: {}", source))]
    CreatingStringSet { source: StringSetError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Plans queries that originate from the InfluxDB Storage gRPC
/// interface, which are in terms of the InfluxDB Data model (e.g.
/// `ParsedLine`). The query methods on this trait such as
/// `tag_columns` are specific to this data model.
///
/// The IOx storage engine implements this trait to provide Timeseries
/// specific queries, but also provides more generic access to the
/// same underlying data via other frontends (e.g. SQL).
///
/// The InfluxDB data model can be thought of as a relational
/// database table where each column has both a type as well as one of
/// the following categories:
///
/// * Tag (always String type)
/// * Field (Float64, Int64, UInt64, String, or Bool)
/// * Time (Int64)
///
/// While the underlying storage is the same for columns in different
/// categories with the same data type, columns of different
/// categories are treated differently in the different query types.
#[derive(Default, Debug)]
pub struct InfluxRPCPlanner {}

impl InfluxRPCPlanner {
    /// Create a new instance of the RPC planner
    pub fn new() -> Self {
        Self {}
    }

    /// Returns a plan that lists the names of tables in this
    /// database that have at least one row that matches the
    /// conditions listed on `predicate`
    pub async fn table_names<D: Database>(
        &self,
        database: &D,
        predicate: Predicate,
    ) -> Result<StringSetPlan> {
        let mut builder = StringSetPlanBuilder::new();

        for chunk in self.filtered_chunks(database, &predicate).await? {
            let new_table_names = chunk
                .table_names(&predicate, builder.known_strings())
                .await
                .map_err(|e| Box::new(e) as _)
                .context(TableNamePlan)?;

            builder = match new_table_names {
                Some(new_table_names) => builder.append(new_table_names.into()),
                None => {
                    // TODO: General purpose plans for
                    // table_names. For now, if we couldn't figure out
                    // the table names from only metadata, generate an
                    // error
                    return UnsupportedPredicate { predicate }.fail();
                }
            }
        }

        let plan = builder.build().context(CreatingStringSet)?;

        Ok(plan)
    }

    /// Returns a list of chunks across all partitions which may
    /// contain data that pass the predicate
    async fn filtered_chunks<D>(
        &self,
        database: &D,
        predicate: &Predicate,
    ) -> Result<Vec<Arc<<D as Database>::Chunk>>>
    where
        D: Database,
    {
        let mut db_chunks = Vec::new();

        // TODO write the following in a functional style (need to get
        // rid of `await` on `Database::chunks`)

        let partition_keys = database
            .partition_keys()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ListingPartitions)?;

        for key in partition_keys {
            // TODO prune partitions somehow
            let partition_chunks = database.chunks(&key).await;
            for chunk in partition_chunks {
                let could_pass_predicate = chunk
                    .could_pass_predicate(predicate)
                    .map_err(|e| Box::new(e) as _)
                    .context(CheckingChunkPredicate {
                        chunk_id: chunk.id(),
                    })?;

                if could_pass_predicate {
                    db_chunks.push(chunk)
                }
            }
        }
        Ok(db_chunks)
    }
}
