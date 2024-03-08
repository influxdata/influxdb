use std::sync::Arc;

use authz::Authorizer;

use crate::{auth::DefaultAuthorizer, http::HttpApi, CommonServerState, Server};

#[derive(Debug)]
pub struct ServerBuilder<W, Q, P> {
    common_state: CommonServerState,
    max_request_size: usize,
    write_buffer: W,
    query_executor: Q,
    persister: P,
    authorizer: Arc<dyn Authorizer>,
}

impl ServerBuilder<NoWriteBuf, NoQueryExec, NoPersister> {
    pub fn new(common_state: CommonServerState) -> Self {
        Self {
            common_state,
            max_request_size: usize::MAX,
            write_buffer: NoWriteBuf,
            query_executor: NoQueryExec,
            persister: NoPersister,
            authorizer: Arc::new(DefaultAuthorizer),
        }
    }
}

impl<W, Q, P> ServerBuilder<W, Q, P> {
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
pub struct WithWriteBuf<W>(Arc<W>);
#[derive(Debug)]
pub struct NoQueryExec;
#[derive(Debug)]
pub struct WithQueryExec<Q>(Arc<Q>);
#[derive(Debug)]
pub struct NoPersister;
#[derive(Debug)]
pub struct WithPersister<P>(Arc<P>);

impl<Q, P> ServerBuilder<NoWriteBuf, Q, P> {
    pub fn write_buffer<W>(self, wb: Arc<W>) -> ServerBuilder<WithWriteBuf<W>, Q, P> {
        ServerBuilder {
            common_state: self.common_state,
            max_request_size: self.max_request_size,
            write_buffer: WithWriteBuf(wb),
            query_executor: self.query_executor,
            persister: self.persister,
            authorizer: self.authorizer,
        }
    }
}

impl<W, P> ServerBuilder<W, NoQueryExec, P> {
    pub fn query_executor<Q>(self, qe: Arc<Q>) -> ServerBuilder<W, WithQueryExec<Q>, P> {
        ServerBuilder {
            common_state: self.common_state,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: WithQueryExec(qe),
            persister: self.persister,
            authorizer: self.authorizer,
        }
    }
}

impl<W, Q> ServerBuilder<W, Q, NoPersister> {
    pub fn persister<P>(self, p: Arc<P>) -> ServerBuilder<W, Q, WithPersister<P>> {
        ServerBuilder {
            common_state: self.common_state,
            max_request_size: self.max_request_size,
            write_buffer: self.write_buffer,
            query_executor: self.query_executor,
            persister: WithPersister(p),
            authorizer: self.authorizer,
        }
    }
}

impl<W, Q, P> ServerBuilder<WithWriteBuf<W>, WithQueryExec<Q>, WithPersister<P>> {
    pub fn build(self) -> Server<W, Q, P> {
        let persister = Arc::clone(&self.persister.0);
        let authorizer = Arc::clone(&self.authorizer);
        let http = Arc::new(HttpApi::new(
            self.common_state.clone(),
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
        }
    }
}
