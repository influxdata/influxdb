use std::sync::Arc;

use async_trait::async_trait;
use client_util::connection;
use observability_deps::tracing::debug;
use predicate::Predicate;
use query::{QueryChunk, QueryChunkMeta};
use schema::Schema;
use snafu::{ResultExt, Snafu};

use crate::{QuerierFlightClient, QuerierFlightError};

use self::test_util::MockIngesterConnection;

mod test_util;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns { source: schema::Error },

    #[snafu(display("Failed to connect to ingester '{}': {}", ingester_address, source))]
    Connecting {
        ingester_address: String,
        source: connection::Error,
    },

    #[snafu(display("Failed ingester handshake '{}': {}", ingester_address, source))]
    Handshake {
        ingester_address: String,
        source: QuerierFlightError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Create a new connection given an `ingester_address` such as
/// "http://127.0.0.1:8083"
pub fn create_ingester_connection(ingester_address: String) -> Arc<dyn IngesterConnection> {
    Arc::new(IngesterConnectionImpl::new(ingester_address))
}

/// Create a new ingester suitable for testing
pub fn create_ingester_connection_for_testing() -> Arc<dyn IngesterConnection> {
    Arc::new(MockIngesterConnection::new())
}

/// Handles communicating with the ingester(s) to retrieve
/// data that is not yet persisted
#[async_trait]
pub trait IngesterConnection: std::fmt::Debug + Send + Sync + 'static {
    /// Returns all partitions ingester(s) know about for the specified table.
    async fn partitions(
        &self,
        namespace_name: Arc<str>,
        table_name: Arc<str>,
        columns: Vec<String>,
        predicate: &Predicate,
        expected_schema: &Schema,
    ) -> Result<Vec<Arc<IngesterPartition>>>;
}

// IngesterConnection that communicates with an ingester.
#[allow(missing_copy_implementations)]
#[derive(Debug)]
pub(crate) struct IngesterConnectionImpl {
    ingester_address: String,
}

impl IngesterConnectionImpl {
    /// Create a new connection given an `ingester_address` such as
    /// "http://127.0.0.1:8083"
    pub fn new(ingester_address: String) -> Self {
        Self { ingester_address }
    }
}

#[async_trait]
impl IngesterConnection for IngesterConnectionImpl {
    /// Retrieve chunks from the ingester for the particular table and
    /// predicate
    async fn partitions(
        &self,
        _namespace_name: Arc<str>,
        table_name: Arc<str>,
        _columns: Vec<String>,
        _predicate: &Predicate,
        _expected_schema: &Schema,
    ) -> Result<Vec<Arc<IngesterPartition>>> {
        let ingester_address = &self.ingester_address;
        debug!(%ingester_address, %table_name, "Connecting to ingester");

        // TODO maybe cache this connection
        let connection = connection::Builder::new()
            .build(&self.ingester_address)
            .await
            .context(ConnectingSnafu { ingester_address })?;

        let mut client = QuerierFlightClient::new(connection);

        // make contact with the ingester
        client
            .handshake()
            .await
            .context(HandshakeSnafu { ingester_address })?;

        // TODO Coming Soon: create the actual IngesterPartition and return them here.

        Ok(vec![])
    }
}

/// A wrapper around the unpersisted data in a partition returned by
/// the ingester that (will) implement the `QueryChunk` interface
///
/// Given the catalog heirarchy:
///
/// ```text
/// (Catalog) Sequencer -> (Catalog) Table --> (Catalog) Partition
/// ```
///
/// An IngesterPartition contains the unpersisted data for a catalog
/// partition from a sequencer. Thus, there can be more than one
/// IngesterPartition for each table the ingester knows about.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone)]
pub struct IngesterPartition {}

impl QueryChunkMeta for IngesterPartition {
    fn summary(&self) -> Option<&data_types2::TableSummary> {
        todo!()
    }

    fn schema(&self) -> Arc<Schema> {
        todo!()
    }

    fn sort_key(&self) -> Option<&schema::sort::SortKey> {
        todo!()
    }

    fn delete_predicates(&self) -> &[Arc<data_types2::DeletePredicate>] {
        todo!()
    }
}

impl QueryChunk for IngesterPartition {
    fn id(&self) -> data_types2::ChunkId {
        todo!()
    }

    fn addr(&self) -> data_types2::ChunkAddr {
        todo!()
    }

    fn table_name(&self) -> &str {
        todo!()
    }

    fn may_contain_pk_duplicates(&self) -> bool {
        todo!()
    }

    fn apply_predicate_to_metadata(
        &self,
        _predicate: &Predicate,
    ) -> Result<predicate::PredicateMatch, query::QueryChunkError> {
        todo!()
    }

    fn column_names(
        &self,
        _ctx: query::exec::IOxSessionContext,
        _predicate: &Predicate,
        _columns: schema::selection::Selection<'_>,
    ) -> Result<Option<query::exec::stringset::StringSet>, query::QueryChunkError> {
        todo!()
    }

    fn column_values(
        &self,
        _ctx: query::exec::IOxSessionContext,
        _column_name: &str,
        _predicate: &Predicate,
    ) -> Result<Option<query::exec::stringset::StringSet>, query::QueryChunkError> {
        todo!()
    }

    fn read_filter(
        &self,
        _ctx: query::exec::IOxSessionContext,
        _predicate: &Predicate,
        _selection: schema::selection::Selection<'_>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, query::QueryChunkError> {
        todo!()
    }

    fn chunk_type(&self) -> &str {
        todo!()
    }

    fn order(&self) -> data_types2::ChunkOrder {
        todo!()
    }
}
