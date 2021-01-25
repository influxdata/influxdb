/// Plans queries that originate from the InfluxDB Storage gRPC
/// interface, which are in terms of the InfluxDB Data model (e.g.
/// `ParsedLine`). The query methods on this trait such as
/// `tag_columns` are specific to this data model.
///
/// The IOx storage engine implements this trait to provide Timeseries
/// specific queries, but also provides more generic access to the
/// same underlying data via other frontends (e.g. SQL).
///
/// The InfluxDB data model can can be thought of as a relational
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
use snafu::{ResultExt, Snafu};

use crate::{exec::StringSetPlan, predicate::Predicate, Database, PartitionChunk};

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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
        let mut plans = Vec::new();

        let partition_keys = database
            .partition_keys()
            .await
            .map_err(|e| Box::new(e) as _)
            .context(ListingPartitions)?;

        for key in partition_keys {
            // TODO prune partitions somehow
            for chunk in &database.chunks(&key).await {
                if chunk.might_pass_predicate(&predicate) {
                    let plan = chunk
                        .table_names(&predicate)
                        .await
                        .map_err(|e| Box::new(e) as _)
                        .context(TableNamePlan)?;

                    plans.push(plan);
                }
            }
        }

        Ok(plans.into())
    }
}
