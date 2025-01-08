use std::sync::Arc;

use crate::{auth::DefaultAuthorizer, http::HttpApi, CommonServerState, Server};
use authz::Authorizer;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_write::{persister::Persister, WriteBuffer};
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct ServerBuilder<W, Q, P, T, L> {
    common_state: CommonServerState,
    time_provider: T,
    max_request_size: usize,
    write_buffer: W,
    query_executor: Q,
    persister: P,
    listener: L,
    authorizer: Arc<dyn Authorizer>,
}

impl ServerBuilder<NoWriteBuf, NoQueryExec, NoPersister, NoTimeProvider, NoListener> {
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
        }
    }
}

impl<W, Q, P, T, L> ServerBuilder<W, Q, P, T, L> {
    pub fn max_request_size(mut self, max_request_size: usize) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    pub fn authorizer(mut self, a: Arc<dyn Authorizer>) -> Self {
        self.authorizer = a;
        self
    }
}

#[derive(Debug)]
pub struct NoWriteBuf;
#[derive(Debug)]
pub struct WithWriteBuf(Arc<dyn WriteBuffer>);
#[derive(Debug)]
pub struct NoQueryExec;
#[derive(Debug)]
pub struct WithQueryExec(Arc<dyn QueryExecutor>);
#[derive(Debug)]
pub struct NoPersister;
#[derive(Debug)]
pub struct WithPersister(Arc<Persister>);
#[derive(Debug)]
pub struct NoTimeProvider;
#[derive(Debug)]
pub struct WithTimeProvider<T>(Arc<T>);
#[derive(Debug)]
pub struct NoListener;
#[derive(Debug)]
pub struct WithListener(TcpListener);

impl<Q, P, T, L> ServerBuilder<NoWriteBuf, Q, P, T, L> {
    pub fn write_buffer(self, wb: Arc<dyn WriteBuffer>) -> ServerBuilder<WithWriteBuf, Q, P, T, L> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: WithWriteBuf(wb),
            query_executor: self.query_executor,
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
        }
    }
}

impl<W, P, T, L> ServerBuilder<W, NoQueryExec, P, T, L> {
    pub fn query_executor(
        self,
        qe: Arc<dyn QueryExecutor>,
    ) -> ServerBuilder<W, WithQueryExec, P, T, L> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: WithQueryExec(qe),
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
        }
    }
}

impl<W, Q, T, L> ServerBuilder<W, Q, NoPersister, T, L> {
    pub fn persister(self, p: Arc<Persister>) -> ServerBuilder<W, Q, WithPersister, T, L> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: WithPersister(p),
            listener: self.listener,
            authorizer: self.authorizer,
        }
    }
}

impl<W, Q, P, L> ServerBuilder<W, Q, P, NoTimeProvider, L> {
    pub fn time_provider<T>(self, tp: Arc<T>) -> ServerBuilder<W, Q, P, WithTimeProvider<T>, L> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: WithTimeProvider(tp),
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: self.persister,
            listener: self.listener,
            authorizer: self.authorizer,
        }
    }
}

impl<W, Q, P, T> ServerBuilder<W, Q, P, T, NoListener> {
    pub fn tcp_listener(self, listener: TcpListener) -> ServerBuilder<W, Q, P, T, WithListener> {
        ServerBuilder {
            common_state: self.common_state,
            time_provider: self.time_provider,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: self.persister,
            listener: WithListener(listener),
            authorizer: self.authorizer,
        }
    }
}

impl<T>
    ServerBuilder<WithWriteBuf, WithQueryExec, WithPersister, WithTimeProvider<T>, WithListener>
{
    pub fn build(self) -> Server<T> {
        let persister = Arc::clone(&self.persister.0);
        let authorizer = Arc::clone(&self.authorizer);
        let http = Arc::new(HttpApi::new(
            self.common_state.clone(),
            Arc::clone(&self.time_provider.0),
            Arc::clone(&self.write_buffer.0),
            Arc::clone(&self.query_executor.0),
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
