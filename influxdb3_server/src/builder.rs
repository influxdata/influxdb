use std::sync::Arc;

use crate::{CommonServerState, Server, auth::DefaultAuthorizer, http::HttpApi};
use authz::Authorizer;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_write::{WriteBuffer, persister::Persister};
use iox_time::TimeProvider;
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct ServerBuilder<W, Q, P, T, L, E> {
    common_state: CommonServerState,
    time_provider: T,
    max_request_size: usize,
    write_buffer: W,
    query_executor: Q,
    persister: P,
    listener: L,
    processing_engine: E,
    authorizer: Arc<dyn Authorizer>,
}

impl
    ServerBuilder<
        NoWriteBuf,
        NoQueryExec,
        NoPersister,
        NoTimeProvider,
        NoListener,
        NoProcessingEngine,
    >
{
    pub fn new(common_state: CommonServerState) -> Self {
        Self {
            common_state,
            time_provider: NoTimeProvider,
            max_request_size: usize::MAX,
            write_buffer: NoWriteBuf,
            query_executor: NoQueryExec,
            persister: NoPersister,
            listener: NoListener,
            authorizer: Arc::new(DefaultAuthorizer),
            processing_engine: NoProcessingEngine,
        }
    }
}

impl<W, Q, P, T, L, E> ServerBuilder<W, Q, P, T, L, E> {
    pub fn max_request_size(mut self, max_request_size: usize) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    pub fn authorizer(mut self, a: Arc<dyn Authorizer>) -> Self {
        self.authorizer = a;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NoWriteBuf;
#[derive(Debug)]
pub struct WithWriteBuf(Arc<dyn WriteBuffer>);
#[derive(Clone, Copy, Debug)]
pub struct NoQueryExec;
#[derive(Debug)]
pub struct WithQueryExec(Arc<dyn QueryExecutor>);
#[derive(Clone, Copy, Debug)]
pub struct NoPersister;
#[derive(Debug)]
pub struct WithPersister(Arc<Persister>);
#[derive(Clone, Copy, Debug)]
pub struct NoTimeProvider;
#[derive(Debug)]
pub struct WithTimeProvider(Arc<dyn TimeProvider>);
#[derive(Clone, Copy, Debug)]
pub struct NoListener;
#[derive(Debug)]
pub struct WithListener(TcpListener);

#[derive(Clone, Copy, Debug)]
pub struct NoProcessingEngine;
#[derive(Debug)]
pub struct WithProcessingEngine(Arc<ProcessingEngineManagerImpl>);

impl<Q, P, T, L, E> ServerBuilder<NoWriteBuf, Q, P, T, L, E> {
    pub fn write_buffer(
        self,
        wb: Arc<dyn WriteBuffer>,
    ) -> ServerBuilder<WithWriteBuf, Q, P, T, L, E> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: WithWriteBuf(wb),
            query_executor: self.query_executor,
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
            processing_engine: self.processing_engine,
        }
    }
}

impl<W, P, T, L, E> ServerBuilder<W, NoQueryExec, P, T, L, E> {
    pub fn query_executor(
        self,
        qe: Arc<dyn QueryExecutor>,
    ) -> ServerBuilder<W, WithQueryExec, P, T, L, E> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: WithQueryExec(qe),
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
            processing_engine: self.processing_engine,
        }
    }
}

impl<W, Q, T, L, E> ServerBuilder<W, Q, NoPersister, T, L, E> {
    pub fn persister(self, p: Arc<Persister>) -> ServerBuilder<W, Q, WithPersister, T, L, E> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: WithPersister(p),
            listener: self.listener,
            authorizer: self.authorizer,
            processing_engine: self.processing_engine,
        }
    }
}

impl<W, Q, P, L, E> ServerBuilder<W, Q, P, NoTimeProvider, L, E> {
    pub fn time_provider(
        self,
        tp: Arc<dyn TimeProvider>,
    ) -> ServerBuilder<W, Q, P, WithTimeProvider, L, E> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: WithTimeProvider(tp),
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
            processing_engine: self.processing_engine,
        }
    }
}

impl<W, Q, P, T, E> ServerBuilder<W, Q, P, T, NoListener, E> {
    pub fn tcp_listener(self, listener: TcpListener) -> ServerBuilder<W, Q, P, T, WithListener, E> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: self.persister,
            listener: WithListener(listener),
            authorizer: self.authorizer,
            processing_engine: self.processing_engine,
        }
    }
}

impl<W, Q, P, T, L> ServerBuilder<W, Q, P, T, L, NoProcessingEngine> {
    pub fn processing_engine(
        self,
        processing_engine: Arc<ProcessingEngineManagerImpl>,
    ) -> ServerBuilder<W, Q, P, T, L, WithProcessingEngine> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
            processing_engine: WithProcessingEngine(processing_engine),
        }
    }
}

impl
    ServerBuilder<
        WithWriteBuf,
        WithQueryExec,
        WithPersister,
        WithTimeProvider,
        WithListener,
        WithProcessingEngine,
    >
{
    pub async fn build(self) -> Server {
        let persister = Arc::clone(&self.persister.0);
        let authorizer = Arc::clone(&self.authorizer);
        let processing_engine = Arc::clone(&self.processing_engine.0);

        Arc::clone(&processing_engine)
            .start_triggers()
            .await
            .expect("failed to start processing engine triggers");

        self.write_buffer
            .0
            .wal()
            .add_file_notifier(Arc::clone(&processing_engine) as _);
        let http = Arc::new(HttpApi::new(
            self.common_state.clone(),
            Arc::clone(&self.time_provider.0),
            Arc::clone(&self.write_buffer.0),
            Arc::clone(&self.query_executor.0),
            processing_engine,
            self.max_request_size,
            Arc::clone(&authorizer),
        ));
        Server {
            common_state: self.common_state,
            http,
            persister,
            authorizer,
            listener: self.listener.0,
        }
    }
}
