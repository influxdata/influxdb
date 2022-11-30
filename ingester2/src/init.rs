use std::{sync::Arc, time::Duration};

use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use backoff::BackoffConfig;
use generated_types::influxdata::iox::{
    catalog::v1::catalog_service_server::{CatalogService, CatalogServiceServer},
    ingester::v1::write_service_server::{WriteService, WriteServiceServer},
};
use iox_catalog::interface::Catalog;
use thiserror::Error;

use crate::{
    buffer_tree::{
        namespace::name_resolver::{NamespaceNameProvider, NamespaceNameResolver},
        partition::resolver::{CatalogPartitionResolver, PartitionCache, PartitionProvider},
        table::name_resolver::{TableNameProvider, TableNameResolver},
        BufferTree,
    },
    server::grpc::GrpcDelegate,
    timestamp_oracle::TimestampOracle,
    TRANSITION_SHARD_ID,
};

/// Acquire opaque handles to the Ingester RPC service implementations.
///
/// This trait serves as the public crate API boundary - callers external to the
/// Ingester crate utilise this abstraction to acquire type erased handles to
/// the RPC service implementations, hiding internal Ingester implementation
/// details & types.
///
/// Callers can mock out this trait or decorate the returned implementation in
/// order to simulate or modify the behaviour of an ingester in their own tests.
pub trait IngesterRpcInterface: Send + Sync + std::fmt::Debug {
    /// The type of the [`CatalogService`] implementation.
    type CatalogHandler: CatalogService;
    /// The type of the [`WriteService`] implementation.
    type WriteHandler: WriteService;
    /// The type of the [`FlightService`] implementation.
    type FlightHandler: FlightService;

    /// Acquire an opaque handle to the Ingester's [`CatalogService`] RPC
    /// handler implementation.
    fn catalog_service(
        &self,
        catalog: Arc<dyn Catalog>,
    ) -> CatalogServiceServer<Self::CatalogHandler>;

    /// Acquire an opaque handle to the Ingester's [`WriteService`] RPC
    /// handler implementation.
    fn write_service(&self) -> WriteServiceServer<Self::WriteHandler>;

    /// Acquire an opaque handle to the Ingester's Arrow Flight
    /// [`FlightService`] RPC handler implementation, allowing at most
    /// `max_simultaneous_requests` queries to be running at any one time.
    fn query_service(
        &self,
        max_simultaneous_requests: usize,
        metrics: &metric::Registry,
    ) -> FlightServiceServer<Self::FlightHandler>;
}

/// Errors that occur during initialisation of an `ingester2` instance.
#[derive(Debug, Error)]
pub enum InitError {
    /// A catalog error occurred while fetching the most recent partitions for
    /// the internal cache.
    #[error("failed to pre-warm partition cache: {0}")]
    PreWarmPartitions(iox_catalog::interface::Error),
}

/// Initialise a new `ingester2` instance, returning the gRPC service handler
/// implementations to be bound by the caller.
///
/// # Deferred Loading for Persist Operations
///
/// Several items within the ingester's internal state are loaded only when
/// needed at persist time; this includes string name identifiers of namespaces,
/// tables, etc that are embedded within the Parquet file metadata.
///
/// As an optimisation, these deferred loads occur in a background task before
/// the persist action actually needs them, in order to both eliminate the
/// latency of waiting for the value to be fetched, and to avoid persistence of
/// large numbers of partitions operations causing large spike in catalog
/// requests / load.
///
/// These values are loaded a uniformly random duration of time between
/// initialisation, and at most, `persist_background_fetch_time` duration of
/// time later. By increasing this duration value the many loads are spread
/// approximately uniformly over a longer period of time, decreasing the catalog
/// load they cause.
///
/// If the `persist_background_fetch_time` duration is too large, they will not
/// have resolved in the background when a persist operation starts, and they
/// will require demand loading, causing an immediate catalog load spike. This
/// value should be tuned to be slightly less than the interval between persist
/// operations, but not so long that it causes catalog load spikes at persist
/// time (which can be observed by the catalog instrumentation metrics).
pub async fn new(
    catalog: Arc<dyn Catalog>,
    metrics: Arc<metric::Registry>,
    persist_background_fetch_time: Duration,
) -> Result<impl IngesterRpcInterface, InitError> {
    // Initialise the deferred namespace name resolver.
    let namespace_name_provider: Arc<dyn NamespaceNameProvider> =
        Arc::new(NamespaceNameResolver::new(
            persist_background_fetch_time,
            Arc::clone(&catalog),
            BackoffConfig::default(),
        ));

    // Initialise the deferred table name resolver.
    let table_name_provider: Arc<dyn TableNameProvider> = Arc::new(TableNameResolver::new(
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
    ));

    // Read the most recently created partitions for the shards this ingester
    // instance will be consuming from.
    //
    // By caching these hot partitions overall catalog load after an ingester
    // starts up is reduced, and the associated query latency is removed from
    // the (blocking) ingest hot path.
    let recent_partitions = catalog
        .repositories()
        .await
        .partitions()
        .most_recent_n(40_000, &[TRANSITION_SHARD_ID])
        .await
        .map_err(InitError::PreWarmPartitions)?;

    // Build the partition provider, wrapped in the partition cache.
    let partition_provider = CatalogPartitionResolver::new(Arc::clone(&catalog));
    let partition_provider = PartitionCache::new(
        partition_provider,
        recent_partitions,
        persist_background_fetch_time,
        Arc::clone(&catalog),
        BackoffConfig::default(),
    );
    let partition_provider: Arc<dyn PartitionProvider> = Arc::new(partition_provider);

    let buffer = Arc::new(BufferTree::new(
        namespace_name_provider,
        table_name_provider,
        partition_provider,
        metrics,
    ));

    // TODO: replay WAL into buffer
    //
    // TODO: recover next sequence number from WAL
    let timestamp = Arc::new(TimestampOracle::new(0));

    Ok(GrpcDelegate::new(Arc::clone(&buffer), buffer, timestamp))
}
