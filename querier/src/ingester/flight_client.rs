use arrow::{datatypes::Schema, record_batch::RecordBatch};
use async_trait::async_trait;
use client_util::connection::{self, Connection};
use data_types::PartitionId;
use generated_types::ingester::IngesterQueryRequest;
use influxdb_iox_client::flight::{
    generated_types as proto,
    low_level::{Client as LowLevelFlightClient, LowLevelMessage, PerformQuery},
};
use observability_deps::tracing::debug;
use snafu::{ResultExt, Snafu};
use std::{collections::HashMap, fmt::Debug, ops::DerefMut, sync::Arc};

pub use influxdb_iox_client::flight::Error as FlightError;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Failed to connect to ingester '{}': {}", ingester_address, source))]
    Connecting {
        ingester_address: String,
        source: connection::Error,
    },

    #[snafu(display("Failed ingester handshake '{}': {}", ingester_address, source))]
    Handshake {
        ingester_address: String,
        source: FlightError,
    },

    #[snafu(display("Internal error creating flight request : {}", source))]
    CreatingRequest {
        source: influxdb_iox_client::google::FieldViolation,
    },

    #[snafu(display("Failed to perform flight request: {}", source))]
    Flight { source: FlightError },

    #[snafu(display("Cannot find schema in flight response"))]
    SchemaMissing,
}

/// Abstract Flight client.
///
/// May use an internal connection pool.
#[async_trait]
pub trait FlightClient: Debug + Send + Sync + 'static {
    /// Send query to given ingester.
    async fn query(
        &self,
        ingester_address: Arc<str>,
        request: IngesterQueryRequest,
    ) -> Result<Box<dyn QueryData>, Error>;
}

/// Default [`FlightClient`] implementation that uses a real connection
#[derive(Debug, Default)]
pub struct FlightClientImpl {
    /// Cached connections
    /// key: ingester_address (e.g. "http://ingester-1:8082")
    /// value: CachedConnection
    ///
    /// Note: Use sync (parking_log) mutex because it is always held
    /// for a very short period of time, and any actual connection (and
    /// waiting) is done in CachedConnection
    connections: parking_lot::Mutex<HashMap<String, CachedConnection>>,
}

impl FlightClientImpl {
    /// Create new client.
    pub fn new() -> Self {
        Self::default()
    }

    /// Establish connection to given addr and perform handshake.
    async fn connect(&self, ingester_address: Arc<str>) -> Result<Connection, Error> {
        let cached_connection = {
            let mut connections = self.connections.lock();
            if let Some(cached_connection) = connections.get(ingester_address.as_ref()) {
                cached_connection.clone()
            } else {
                // need to make a new one;
                let cached_connection = CachedConnection::new(&ingester_address);
                connections.insert(ingester_address.to_string(), cached_connection.clone());
                cached_connection
            }
        };
        cached_connection.connect().await
    }
}

#[async_trait]
impl FlightClient for FlightClientImpl {
    async fn query(
        &self,
        ingester_addr: Arc<str>,
        request: IngesterQueryRequest,
    ) -> Result<Box<dyn QueryData>, Error> {
        let connection = self.connect(Arc::clone(&ingester_addr)).await?;

        let mut client = LowLevelFlightClient::<proto::IngesterQueryRequest>::new(connection);

        debug!(%ingester_addr, ?request, "Sending request to ingester");
        let request: proto::IngesterQueryRequest =
            request.try_into().context(CreatingRequestSnafu)?;

        let mut perform_query = client.perform_query(request).await.context(FlightSnafu)?;
        let (schema, app_metadata) = match perform_query.next().await.context(FlightSnafu)? {
            Some((LowLevelMessage::Schema(schema), app_metadata)) => (schema, app_metadata),
            _ => {
                return Err(Error::SchemaMissing);
            }
        };
        Ok(Box::new(PerformQueryAdapter {
            inner: perform_query,
            schema,
            app_metadata,
            batch_counter: 0,
            state: Default::default(),
        }))
    }
}

/// Data that is returned by an ingester gRPC query.
///
/// This is mostly the same as [`PerformQuery`] but allows some easier mocking.
#[async_trait]
pub trait QueryData: Debug + Send + 'static {
    /// Returns the next [`LowLevelMessage`] available for this query, or `None` if
    /// there are no further results available.
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError>;
}

#[async_trait]
impl<T> QueryData for Box<T>
where
    T: QueryData + ?Sized,
{
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError> {
        self.deref_mut().next().await
    }
}

/// Protocol state.
///
/// ```text
///
/// [NoPartitionYet]
///      |
///      +-----------------------o
///      |                       |
///      V                       |
/// [NewPartition]<-----------o  |
///      |                    |  |
///      V                    |  |
/// [PartitionAnnounced]<--o  |  |
///      |                 |  |  |
///      V                 |  |  |
/// [SchemaAnnounced]      |  |  |
///      |                 |  |  |
///      V                 |  |  |
/// [BatchTransmitted]     |  |  |
///      |                 |  |  |
///      +-----------------+--+  |
///      |                       |
///      | o---------------------o
///      | |
///      V V
///     [End]<--o
///       |     |
///       o-----o
///
/// ```
#[derive(Debug)]
enum PerformQueryAdapterState {
    NoPartitionYet,
    NewPartition {
        partition_id: PartitionId,
        batch: RecordBatch,
    },
    PartitionAnnounced {
        partition_id: PartitionId,
        batch: RecordBatch,
    },
    SchemaAnnounced {
        partition_id: PartitionId,
        batch: RecordBatch,
    },
    BatchTransmitted {
        partition_id: PartitionId,
    },
    End,
}

impl Default for PerformQueryAdapterState {
    fn default() -> Self {
        Self::NoPartitionYet
    }
}

#[derive(Debug)]
struct PerformQueryAdapter {
    inner: PerformQuery<proto::IngesterQueryResponseMetadata>,
    app_metadata: proto::IngesterQueryResponseMetadata,
    schema: Arc<Schema>,
    batch_counter: usize,
    state: PerformQueryAdapterState,
}

impl PerformQueryAdapter {
    /// Get next batch from underlying [`PerformQuery`] alongside with a batch index (which in turn can be used to
    /// look up the batch partition).
    ///
    /// Returns `Ok(None)` if the stream has ended.
    async fn next_batch(&mut self) -> Result<Option<(RecordBatch, usize)>, FlightError> {
        loop {
            match self.inner.next().await? {
                None => {
                    return Ok(None);
                }
                Some((LowLevelMessage::RecordBatch(batch), _)) => {
                    let pos = self.batch_counter;
                    self.batch_counter += 1;
                    return Ok(Some((batch, pos)));
                }
                // ignore all other message types for now
                Some((LowLevelMessage::None | LowLevelMessage::Schema(_), _)) => (),
            }
        }
    }

    /// Announce new partition (id and its persistence status).
    fn announce_partition(
        &mut self,
        partition_id: PartitionId,
    ) -> (LowLevelMessage, proto::IngesterQueryResponseMetadata) {
        let meta = proto::IngesterQueryResponseMetadata {
            partition_id: partition_id.get(),
            status: Some(
                self.app_metadata
                    .unpersisted_partitions
                    .remove(&partition_id.get())
                    .unwrap(),
            ),
            ..Default::default()
        };
        (LowLevelMessage::None, meta)
    }
}

#[async_trait]
impl QueryData for PerformQueryAdapter {
    async fn next(
        &mut self,
    ) -> Result<Option<(LowLevelMessage, proto::IngesterQueryResponseMetadata)>, FlightError> {
        loop {
            match std::mem::take(&mut self.state) {
                PerformQueryAdapterState::End => {
                    if let Some(partition_id) =
                        self.app_metadata.unpersisted_partitions.keys().next()
                    {
                        let partition_id = PartitionId::new(*partition_id);
                        return Ok(Some(self.announce_partition(partition_id)));
                    } else {
                        return Ok(None);
                    }
                }
                PerformQueryAdapterState::NoPartitionYet => {
                    let (batch, pos) = if let Some(x) = self.next_batch().await? {
                        x
                    } else {
                        self.state = PerformQueryAdapterState::End;
                        continue;
                    };
                    let partition_id = PartitionId::new(self.app_metadata.batch_partition_ids[pos]);

                    self.state = PerformQueryAdapterState::NewPartition {
                        partition_id,
                        batch,
                    };
                    continue;
                }
                PerformQueryAdapterState::NewPartition {
                    partition_id,
                    batch,
                } => {
                    self.state = PerformQueryAdapterState::PartitionAnnounced {
                        partition_id,
                        batch,
                    };

                    return Ok(Some(self.announce_partition(partition_id)));
                }
                PerformQueryAdapterState::PartitionAnnounced {
                    partition_id,
                    batch,
                } => {
                    self.state = PerformQueryAdapterState::SchemaAnnounced {
                        partition_id,
                        batch,
                    };

                    let meta = Default::default();
                    return Ok(Some((
                        LowLevelMessage::Schema(Arc::clone(&self.schema)),
                        meta,
                    )));
                }
                PerformQueryAdapterState::SchemaAnnounced {
                    partition_id,
                    batch,
                } => {
                    self.state = PerformQueryAdapterState::BatchTransmitted { partition_id };

                    let meta = Default::default();
                    return Ok(Some((LowLevelMessage::RecordBatch(batch), meta)));
                }
                PerformQueryAdapterState::BatchTransmitted { partition_id } => {
                    let (batch, pos) = if let Some(x) = self.next_batch().await? {
                        x
                    } else {
                        self.state = PerformQueryAdapterState::End;
                        continue;
                    };
                    let partition_id2 =
                        PartitionId::new(self.app_metadata.batch_partition_ids[pos]);

                    if partition_id == partition_id2 {
                        self.state = PerformQueryAdapterState::PartitionAnnounced {
                            partition_id,
                            batch,
                        };
                        continue;
                    } else {
                        self.state = PerformQueryAdapterState::NewPartition {
                            partition_id: partition_id2,
                            batch,
                        };
                        continue;
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct CachedConnection {
    ingester_address: Arc<str>,
    /// Real async mutex to
    maybe_connection: Arc<tokio::sync::Mutex<Option<Connection>>>,
}

impl CachedConnection {
    fn new(ingester_address: &Arc<str>) -> Self {
        Self {
            ingester_address: Arc::clone(ingester_address),
            maybe_connection: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Return the underlying connection, creating it if needed
    async fn connect(&self) -> Result<Connection, Error> {
        let mut maybe_connection = self.maybe_connection.lock().await;

        let ingester_address = self.ingester_address.as_ref();

        if let Some(connection) = maybe_connection.as_ref() {
            debug!(%ingester_address, "Reusing connection to ingester");

            Ok(connection.clone())
        } else {
            debug!(%ingester_address, "Connecting to ingester");

            let connection = connection::Builder::new()
                .build(ingester_address)
                .await
                .context(ConnectingSnafu { ingester_address })?;

            // sanity check w/ a handshake
            let mut client =
                LowLevelFlightClient::<proto::IngesterQueryRequest>::new(connection.clone());

            // make contact with the ingester
            client
                .handshake()
                .await
                .context(HandshakeSnafu { ingester_address })?;

            *maybe_connection = Some(connection.clone());
            Ok(connection)
        }
    }
}
