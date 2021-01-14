//! This module contains the main IOx Database object which has the
//! instances of the immutable buffer, read buffer, and object store

use std::{
    collections::BTreeMap,
    convert::TryFrom,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules};
use mutable_buffer::MutableBufferDb;
use query::{predicate::Predicate, Database, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

mod chunk;
use chunk::DBChunk;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatatbaseNotWriteable {},

    #[snafu(display("Cannot read to this database: no mutable buffer configured"))]
    DatabaseNotReadable {},

    #[snafu(display("Error rolling partition: {}", source))]
    RollingPartition {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error querying mutable buffer: {}", source))]
    MutableBufferRead {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error writing to mutable buffer: {}", source))]
    MutableBufferWrite {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error translating predicate: {}", msg))]
    ReadBufferPredicate { msg: String, pred: Predicate },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Serialize, Deserialize)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,

    #[serde(skip)]
    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    pub mutable_buffer: Option<Arc<MutableBufferDb>>,

    #[serde(skip)]
    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    pub read_buffer: Arc<ReadBufferDb>,

    #[serde(skip)]
    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

    #[serde(skip)]
    sequence: AtomicU64,
}
impl Db {
    pub fn new(
        rules: DatabaseRules,
        mutable_buffer: Option<Arc<MutableBufferDb>>,
        read_buffer: Arc<ReadBufferDb>,
        wal_buffer: Option<Buffer>,
        sequence: AtomicU64,
    ) -> Self {
        let wal_buffer = wal_buffer.map(Mutex::new);
        Self {
            rules,
            mutable_buffer,
            read_buffer,
            wal_buffer,
            sequence,
        }
    }

    /// Rolls over the active chunk in the database's specified partition
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.mutable_buffer.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .await
                .context(RollingPartition)
                .map(|c| Arc::new(DBChunk::MutableBuffer(c)))
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    // Return a list of all chunks in the mutable_buffer (that can
    // potentially be migrated into the read buffer or object store)
    pub async fn mutable_buffer_chunks(&self, partition_key: &str) -> Result<Vec<Arc<DBChunk>>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            mutable_buffer
                .chunks(partition_key)
                .await
                .context(MutableBufferRead)?
                .into_iter()
                .map(|c| Arc::new(DBChunk::MutableBuffer(c)))
                .collect()
        } else {
            vec![]
        };
        Ok(chunks)
    }

    /// Returns the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

#[async_trait]
impl Database for Db {
    type Error = Error;
    type Chunk = DBChunk;

    /// Return a covering set of chunks for a particular partition
    async fn chunks(&self, partition_key: &str) -> Result<Vec<Arc<Self::Chunk>>, Self::Error> {
        // return a coverting set of chunks. TODO include read buffer
        // chunks and take them preferentially from the read buffer.
        let mutable_chunk_iter = self.mutable_buffer_chunks(partition_key).await?.into_iter();

        let chunks: BTreeMap<_, _> = mutable_chunk_iter
            .map(|chunk| (chunk.id(), chunk))
            .collect();

        // inserting into the map will have removed any dupes
        let chunks: Vec<_> = chunks.into_iter().map(|(_id, chunk)| chunk).collect();

        Ok(chunks)
    }

    // Note that most of the functions below will eventually be removed from
    // this trait. For now, pass them directly on to the local store

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    async fn table_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn tag_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .tag_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn field_column_names(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::FieldListPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .field_column_names(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn column_values(
        &self,
        column_name: &str,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::StringSetPlan, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .column_values(column_name, predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_series(
        &self,
        predicate: query::predicate::Predicate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_series(predicate)
            .await
            .context(MutableBufferRead)
    }

    async fn query_groups(
        &self,
        predicate: query::predicate::Predicate,
        gby_agg: query::group_by::GroupByAndAggregate,
    ) -> Result<query::exec::SeriesSetPlans, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .query_groups(predicate, gby_agg)
            .await
            .context(MutableBufferRead)
    }

    async fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .await
            .context(MutableBufferRead)
    }

    async fn table_names_for_partition(
        &self,
        partition_key: &str,
    ) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .table_names_for_partition(partition_key)
            .await
            .context(MutableBufferRead)
    }
}

pub fn to_read_buffer_predicate(predicate: &Predicate) -> Result<read_buffer::Predicate, Error> {
    // Try to convert non-time column expressions into binary expressions
    // that are compatible with the read buffer.
    match predicate
        .exprs
        .iter()
        .map(read_buffer::BinaryExpr::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(exprs) => {
            // Construct a `ReadBuffer` predicate with or without
            // InfluxDB-specific expressions on the time column.
            Ok(match predicate.range {
                Some(range) => {
                    read_buffer::Predicate::with_time_range(&exprs, range.start, range.end)
                }
                None => read_buffer::Predicate::new(exprs),
            })
        }
        Err(e) => Err(Error::ReadBufferPredicate {
            msg: e,
            pred: predicate.clone(),
        }),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use arrow_deps::datafusion::logical_plan::Expr;
    use arrow_deps::datafusion::scalar::ScalarValue;

    use query::predicate::PredicateBuilder;
    use read_buffer::BinaryExpr as RBBinaryExpr;
    use read_buffer::Predicate as RBPredicate;

    #[test]
    fn into_read_buffer_predicate() {
        let cases = vec![
            // empty predicate
            (PredicateBuilder::default().build(), RBPredicate::default()),
            // just a time range
            (
                PredicateBuilder::default()
                    .timestamp_range(100, 2000)
                    .build(),
                RBPredicate::with_time_range(&[], 100, 2000),
            ),
            // just a single non-time-range expression
            (
                PredicateBuilder::default()
                    .add_expr(Expr::Column("track".to_owned()).eq(Expr::Literal(
                        ScalarValue::Utf8(Some("Star Roving".to_owned())),
                    )))
                    .build(),
                RBPredicate::new(vec![RBBinaryExpr::from(("track", "=", "Star Roving"))]),
            ),
            // multiple non-time-range expressions
            (
                PredicateBuilder::default()
                    .add_expr(Expr::Column("track".to_owned()).eq(Expr::Literal(
                        ScalarValue::Utf8(Some("Star Roving".to_owned())),
                    )))
                    .add_expr(
                        Expr::Column("counter".to_owned())
                            .gt(Expr::Literal(ScalarValue::Int64(Some(2992)))),
                    )
                    .build(),
                RBPredicate::new(vec![
                    RBBinaryExpr::from(("track", "=", "Star Roving")),
                    RBBinaryExpr::from(("counter", ">", 2992_i64)),
                ]),
            ),
            // a bit of everything
            (
                PredicateBuilder::default()
                    .timestamp_range(100, 2000)
                    .add_expr(Expr::Column("track".to_owned()).eq(Expr::Literal(
                        ScalarValue::Utf8(Some("Star Roving".to_owned())),
                    )))
                    .add_expr(
                        Expr::Column("counter".to_owned())
                            .gt(Expr::Literal(ScalarValue::Int64(Some(2992)))),
                    )
                    .build(),
                RBPredicate::with_time_range(
                    &[
                        RBBinaryExpr::from(("track", "=", "Star Roving")),
                        RBBinaryExpr::from(("counter", ">", 2992_i64)),
                    ],
                    100,
                    2000,
                ),
            ),
        ];

        for (predicate, exp) in cases {
            assert_eq!(to_read_buffer_predicate(&predicate).unwrap(), exp);
        }

        let cases = vec![
            // not a binary expression
            (
                PredicateBuilder::default()
                    .add_expr(Expr::Literal(ScalarValue::Int64(Some(100_i64))))
                    .build(),
                "unsupported expression type Int64(100)",
            ),
            // left side must be a column
            (
                PredicateBuilder::default()
                    .add_expr(
                        Expr::Literal(ScalarValue::Utf8(Some("The Stove &".to_owned()))).eq(
                            Expr::Literal(ScalarValue::Utf8(Some("The Toaster".to_owned()))),
                        ),
                    )
                    .build(),
                "unsupported left expression Utf8(\"The Stove &\")",
            ),
            // unsupported operator LIKE
            (
                PredicateBuilder::default()
                    .add_expr(Expr::Column("track".to_owned()).like(Expr::Literal(
                        ScalarValue::Utf8(Some("Star Roving".to_owned())),
                    )))
                    .build(),
                "unsupported operator Like",
            ),
            // right side must be a literal
            (
                PredicateBuilder::default()
                    .add_expr(Expr::Column("Intermezzo 1".to_owned()).eq(Expr::Wildcard))
                    .build(),
                "unsupported right expression *",
            ),
            // binary expression like foo = NULL not supported
            (
                PredicateBuilder::default()
                    .add_expr(
                        Expr::Column("track".to_owned()).eq(Expr::Literal(ScalarValue::Utf8(None))),
                    )
                    .build(),
                "NULL literal not supported",
            ),
        ];

        for (predicate, exp) in cases {
            match to_read_buffer_predicate(&predicate).unwrap_err() {
                Error::ReadBufferPredicate { msg, pred: _ } => {
                    assert_eq!(msg, exp.to_owned());
                }
                _ => panic!("fail"),
            }
        }
    }
}
