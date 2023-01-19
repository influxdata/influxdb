//! Ingest handler

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{Shard, ShardIndex, TopicMetadata};
use futures::{
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use generated_types::ingester::IngesterQueryRequest;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, U64Counter};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use tokio::{
    sync::{Semaphore, TryAcquireError},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use trace::span::{Span, SpanRecorder};
use write_buffer::core::WriteBufferReading;
use write_summary::ShardProgress;

use crate::{
    data::IngesterData,
    lifecycle::{run_lifecycle_manager, LifecycleConfig, LifecycleHandleImpl, LifecycleManager},
    poison::PoisonCabinet,
    querier_handler::{prepare_data_to_querier, IngesterQueryResponse},
    stream_handler::{
        handler::SequencedStreamHandler, sink_adaptor::IngestSinkAdaptor,
        sink_instrumentation::SinkInstrumentation, PeriodicWatermarkFetcher,
    },
};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Write buffer error: {}", source))]
    WriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },
    #[snafu(display("error initialising ingester: {}", source))]
    IngesterInit { source: crate::data::InitError },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An [`IngestHandler`] handles all ingest requests from persistence and queries
#[async_trait]
pub trait IngestHandler: Send + Sync {
    /// Return results from the in-memory data that match this query
    async fn query(
        &self,
        request: IngesterQueryRequest,
        span: Option<Span>,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error>;

    /// Return shard progress for the requested shard indexes
    async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress>;

    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    async fn join(&self);

    /// Shut down background workers.
    fn shutdown(&self);

    /// Persist everything immediately.
    async fn persist_all(&self);
}

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Implementation of the `IngestHandler` trait to ingest from shards and manage
/// persistence and answer queries
#[derive(Debug)]
pub struct IngestHandlerImpl<T = SystemProvider> {
    /// Topic assigned to this ingester
    #[allow(dead_code)]
    topic: TopicMetadata,

    /// Future that resolves when the background worker exits
    join_handles: Vec<(String, SharedJoinHandle)>,

    lifecycle_handle: LifecycleHandleImpl,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// The cache and buffered data for the ingester
    data: Arc<IngesterData>,

    time_provider: T,

    /// Query execution duration distribution for successes.
    query_duration_success: DurationHistogram,

    /// Query execution duration distribution for "not found" errors
    query_duration_error_not_found: DurationHistogram,

    /// Query execution duration distribution for all other errors
    query_duration_error_other: DurationHistogram,

    /// Query request rejected due to concurrency limits
    query_request_limit_rejected: U64Counter,

    // A request limiter to restrict the number of simultaneous requests this
    // ingester services.
    //
    // This allows the ingester to drop a portion of requests when experiencing an
    // unusual flood of requests
    request_sem: Semaphore,
}

impl IngestHandlerImpl {
    /// Initialize the Ingester
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        lifecycle_config: LifecycleConfig,
        topic: TopicMetadata,
        shard_states: BTreeMap<ShardIndex, Shard>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        write_buffer: Arc<dyn WriteBufferReading>,
        exec: Arc<Executor>,
        metric_registry: Arc<metric::Registry>,
        skip_to_oldest_available: bool,
        max_requests: usize,
    ) -> Result<Self> {
        let data = Arc::new(
            IngesterData::new(
                object_store,
                catalog,
                shard_states.clone().into_iter().map(|(idx, s)| (s.id, idx)),
                exec,
                BackoffConfig::default(),
                Arc::clone(&metric_registry),
            )
            .await
            .context(IngesterInitSnafu)?,
        );

        let ingester_data = Arc::clone(&data);
        let topic_name = topic.name.clone();

        // start the lifecycle manager
        let persister = Arc::clone(&data);
        let lifecycle_manager = LifecycleManager::new(
            lifecycle_config,
            Arc::clone(&metric_registry),
            Arc::new(SystemProvider::new()),
        );
        let lifecycle_handle = lifecycle_manager.handle();
        let shutdown = CancellationToken::new();
        let handle = tokio::task::spawn(run_lifecycle_manager(
            lifecycle_manager,
            persister,
            shutdown.clone(),
            Arc::new(PoisonCabinet::new()),
        ));
        info!(
            "ingester handler and lifecycle started with config {:?}",
            lifecycle_config
        );

        let mut join_handles = Vec::with_capacity(shard_states.len() + 1);
        join_handles.push(("lifecycle manager".to_owned(), shared_handle(handle)));

        for (shard_index, shard) in shard_states {
            let metric_registry = Arc::clone(&metric_registry);

            // Acquire a write buffer stream and seek it to the last
            // definitely-already-persisted op
            let mut op_stream = write_buffer
                .stream_handler(shard_index)
                .await
                .context(WriteBufferSnafu)?;
            info!(
                shard_index = shard_index.get(),
                min_unpersisted_sequence_number = shard.min_unpersisted_sequence_number.get(),
                "Seek stream",
            );
            op_stream
                .seek(shard.min_unpersisted_sequence_number)
                .await
                .context(WriteBufferSnafu)?;

            // Initialise the DmlSink stack.
            let watermark_fetcher = PeriodicWatermarkFetcher::new(
                Arc::clone(&write_buffer),
                shard.shard_index,
                Duration::from_secs(10),
                &metric_registry,
            );
            // Wrap the IngesterData in a DmlSink adapter
            let sink = IngestSinkAdaptor::new(
                Arc::clone(&ingester_data),
                lifecycle_handle.clone(),
                shard.id,
            );
            // Emit metrics when ops flow through the sink
            let sink = SinkInstrumentation::new(
                sink,
                watermark_fetcher,
                topic_name.clone(),
                shard.shard_index,
                &metric_registry,
            );

            // Spawn a task to stream in ops from the op_stream and push them
            // into the sink
            let handle = tokio::task::spawn({
                let shutdown = shutdown.child_token();
                let lifecycle_handle = lifecycle_handle.clone();
                let topic_name = topic_name.clone();
                async move {
                    let handler = SequencedStreamHandler::new(
                        op_stream,
                        shard.min_unpersisted_sequence_number,
                        sink,
                        lifecycle_handle,
                        topic_name,
                        shard.shard_index,
                        shard.id,
                        &metric_registry,
                        skip_to_oldest_available,
                    );

                    handler.run(shutdown).await
                }
            });

            let worker_name = format!("stream handler for shard index {}", shard_index.get());
            join_handles.push((worker_name, shared_handle(handle)));
        }

        // Record query duration metrics, broken down by query execution result
        let query_duration: Metric<DurationHistogram> = metric_registry.register_metric(
            "ingester_flight_query_duration",
            "flight request query execution duration",
        );
        let query_duration_success = query_duration.recorder(&[("result", "success")]);
        let query_duration_error_not_found =
            query_duration.recorder(&[("result", "error_not_found")]);
        let query_duration_error_other = query_duration.recorder(&[("result", "error_other")]);

        let query_request_limit_rejected = metric_registry
            .register_metric::<U64Counter>(
                "query_request_limit_rejected",
                "number of query requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);

        Ok(Self {
            data,
            topic,
            join_handles,
            lifecycle_handle,
            shutdown,
            query_duration_success,
            query_duration_error_not_found,
            query_duration_error_other,
            query_request_limit_rejected,
            request_sem: Semaphore::new(max_requests),
            time_provider: Default::default(),
        })
    }
}

#[async_trait]
impl IngestHandler for IngestHandlerImpl {
    async fn query(
        &self,
        request: IngesterQueryRequest,
        span: Option<Span>,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error> {
        let span_recorder = SpanRecorder::new(span);

        // TODO(4567): move this into a instrumented query delegate

        // Acquire and hold a permit for the duration of this request, or return
        // a 503 if the existing requests have already exhausted the allocation.
        //
        // Our goal is to limit the number of concurrently executing queries as a
        // rough way of ensuring we don't explode memory by trying to do too much at
        // the same time.
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                error!("simultaneous request limit exceeded - dropping request");
                self.query_request_limit_rejected.inc(1);
                return Err(crate::querier_handler::Error::RequestLimit);
            }
            Err(e) => panic!("request limiter error: {}", e),
        };

        // TEMP(alamb): Log details about what was requested
        // temporarily so we can track down potentially "killer"
        // requests from the querier to ingester
        info!(
            namespace_id=%request.namespace_id,
            table_id=%request.table_id,
            columns=?request.columns,
            predicate=?request.predicate,
            "handling querier request"
        );

        let t = self.time_provider.now();
        let request = Arc::new(request);
        let res = prepare_data_to_querier(
            &self.data,
            &request,
            span_recorder.child_span("ingester prepare data to querier"),
        )
        .await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.query_duration_success.record(delta),
                Err(crate::querier_handler::Error::TableNotFound { .. })
                | Err(crate::querier_handler::Error::NamespaceNotFound { .. }) => {
                    self.query_duration_error_not_found.record(delta)
                }
                Err(_) => self.query_duration_error_other.record(delta),
            };
        }

        res
    }

    async fn join(&self) {
        // Need to poll handlers unordered to detect early exists of any worker in the list.
        let mut unordered: FuturesUnordered<_> = self
            .join_handles
            .iter()
            .cloned()
            .map(|(name, handle)| async move { handle.await.map(|_| name) })
            .collect();

        while let Some(e) = unordered.next().await {
            let name = e.unwrap();

            if !self.shutdown.is_cancelled() {
                panic!("Background worker '{name}' exited early!");
            }
        }

        self.data.exec().join().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
        self.data.exec().shutdown();
    }

    /// Return the ingestion progress from each shard
    async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress> {
        self.data.progresses(shard_indexes).await
    }

    /// Persist everything immediately. This is called by the `PersistService` gRPC API in tests
    /// asserting on persisted data, and should probably not be used in production. May behave in
    /// unexpected ways if used concurrently with writes or lifecycle persists.
    async fn persist_all(&self) {
        self.lifecycle_handle.state.lock().persist_everything_now = true;
    }
}

impl<T> Drop for IngestHandlerImpl<T> {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("IngestHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }

        for (worker_name, handle) in &self.join_handles {
            if handle.clone().now_or_never().is_none() {
                warn!(
                    worker_name = worker_name.as_str(),
                    "IngestHandlerImpl dropped without waiting for worker termination",
                );
            }
        }

        // `self.data.exec` implements `Drop`, so we don't need to do anything
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use data_types::{Namespace, NamespaceId, PartitionKey, SequenceNumber, TableId};
    use dml::DmlWrite;
    use iox_catalog::mem::MemCatalog;
    use object_store::memory::InMemory;
    use test_helpers::maybe_start_logging;
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    use super::*;
    use crate::test_util::make_write_op;

    #[tokio::test]
    async fn test_shutdown() {
        let (ingester, _, _) = ingester_test_setup(vec![], 0, true).await;

        // does not exit w/o shutdown
        tokio::select! {
            _ = ingester.join() => panic!("ingester finished w/o shutdown"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };

        ingester.shutdown();

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Background worker 'bad_task' exited early!")]
    async fn test_join_task_early_shutdown() {
        let (mut ingester, _, _) = ingester_test_setup(vec![], 0, true).await;

        let shutdown_task = tokio::spawn(async {
            // It does nothing! and stops.
        });
        ingester
            .join_handles
            .push(("bad_task".to_string(), shared_handle(shutdown_task)));

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn test_join_task_panic() {
        let (mut ingester, _, _) = ingester_test_setup(vec![], 0, true).await;

        let shutdown_task = tokio::spawn(async {
            panic!("bananas");
        });
        ingester
            .join_handles
            .push(("bad_task".to_string(), shared_handle(shutdown_task)));

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    async fn ingester_test_setup(
        write_operations: Vec<DmlWrite>,
        min_unpersisted_sequence_number: i64,
        skip_to_oldest_available: bool,
    ) -> (IngestHandlerImpl, Shard, Namespace) {
        let metrics: Arc<metric::Registry> = Default::default();
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = txn.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = txn
            .namespaces()
            .create("foo", None, topic.id, query_pool.id)
            .await
            .unwrap();
        let mut shard = txn
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        // update the min unpersisted
        shard.min_unpersisted_sequence_number =
            SequenceNumber::new(min_unpersisted_sequence_number);
        // this probably isn't necessary, but just in case something changes later
        txn.shards()
            .update_min_unpersisted_sequence_number(
                shard.id,
                SequenceNumber::new(min_unpersisted_sequence_number),
            )
            .await
            .unwrap();

        let mut shard_states = BTreeMap::new();
        shard_states.insert(shard_index, shard);

        let write_buffer_state =
            MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        for write_operation in write_operations {
            write_buffer_state.push_write(write_operation);
        }
        txn.commit().await.unwrap();

        let reading: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());
        let object_store = Arc::new(InMemory::new());

        let lifecycle_config = LifecycleConfig::new(
            1000000,
            1000,
            1000,
            Duration::from_secs(10),
            Duration::from_secs(10),
            100000000,
        );
        let ingester = IngestHandlerImpl::new(
            lifecycle_config,
            topic.clone(),
            shard_states,
            Arc::clone(&catalog),
            object_store,
            reading,
            Arc::new(Executor::new_testing()),
            Arc::clone(&metrics),
            skip_to_oldest_available,
            1,
        )
        .await
        .unwrap();

        (ingester, shard, namespace)
    }

    fn dml_write(table_name: &str, sequence_number: i64) -> DmlWrite {
        let partition_key = PartitionKey::from("1970-01-01");
        let shard_index = ShardIndex::new(0);
        let namespace_id = NamespaceId::new(1);
        let table_id = TableId::new(1);
        let timestamp = 42;

        make_write_op(
            &partition_key,
            shard_index,
            namespace_id,
            table_name,
            table_id,
            sequence_number,
            &format!(
                "{} foo=1 {}\n{} foo=2 {}",
                table_name,
                timestamp,
                table_name,
                timestamp + 10
            ),
        )
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn sequence_number_no_longer_exists() {
        maybe_start_logging();

        let write_operations = vec![dml_write("cpu", 10)];
        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 2, false).await;

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(
        expected = "attempted to seek to offset 10, but current high watermark for partition 0 is 2"
    )]
    async fn sequence_number_after_watermark() {
        maybe_start_logging();

        let write_operations = vec![dml_write("cpu", 2)];

        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 10, false).await;

        tokio::time::timeout(Duration::from_millis(1100), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(
        expected = "attempted to seek to offset 10, but current high watermark for partition 0 is 2"
    )]
    async fn sequence_number_after_watermark_skip_to_oldest_available() {
        maybe_start_logging();

        let write_operations = vec![dml_write("cpu", 2)];

        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 10, true).await;

        tokio::time::timeout(Duration::from_millis(1100), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn limits_concurrent_queries() {
        let (mut ingester, _, _) = ingester_test_setup(vec![], 0, true).await;
        let request = IngesterQueryRequest {
            namespace_id: NamespaceId::new(42),
            table_id: TableId::new(24),
            columns: vec!["asdf".to_string()],
            predicate: None,
        };

        let res = ingester.query(request.clone(), None).await.unwrap_err();
        assert!(matches!(
            res,
            crate::querier_handler::Error::NamespaceNotFound { .. }
        ));

        ingester.request_sem = Semaphore::new(0);
        let res = ingester.query(request, None).await.unwrap_err();
        assert!(matches!(res, crate::querier_handler::Error::RequestLimit));
    }
}
