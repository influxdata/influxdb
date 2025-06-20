use std::{path::PathBuf, sync::Arc};

use crate::{CommonServerState, Server, http::HttpApi};
use influxdb3_authz::{AuthProvider, NoAuthAuthenticator};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_write::{WriteBuffer, persister::Persister};
use iox_time::TimeProvider;
use rustls::SupportedProtocolVersion;
use tokio::net::TcpListener;

#[derive(Debug)]
pub struct ServerBuilder {
    common_state: CommonServerState,
    time_provider: Option<Arc<dyn TimeProvider>>,
    max_request_size: usize,
    write_buffer: Option<Arc<dyn WriteBuffer>>,
    query_executor: Option<Arc<dyn QueryExecutor>>,
    persister: Option<Arc<Persister>>,
    listener: Option<TcpListener>,
    processing_engine: Option<Arc<ProcessingEngineManagerImpl>>,
    authorizer: Arc<dyn AuthProvider>,
}

impl ServerBuilder {
    pub fn new(common_state: CommonServerState) -> Self {
        Self {
            common_state,
            time_provider: None,
            max_request_size: usize::MAX,
            write_buffer: None,
            query_executor: None,
            persister: None,
            listener: None,
            processing_engine: None,
            authorizer: Arc::new(NoAuthAuthenticator),
        }
    }

    pub fn max_request_size(mut self, max_request_size: usize) -> Self {
        self.max_request_size = max_request_size;
        self
    }

    pub fn authorizer(mut self, authorizer: Arc<dyn AuthProvider>) -> Self {
        self.authorizer = authorizer;
        self
    }

    pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
        self.time_provider = Some(time_provider);
        self
    }

    pub fn write_buffer(mut self, write_buffer: Arc<dyn WriteBuffer>) -> Self {
        self.write_buffer = Some(write_buffer);
        self
    }

    pub fn query_executor(mut self, query_executor: Arc<dyn QueryExecutor>) -> Self {
        self.query_executor = Some(query_executor);
        self
    }

    pub fn persister(mut self, persister: Arc<Persister>) -> Self {
        self.persister = Some(persister);
        self
    }

    pub fn tcp_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    pub fn processing_engine(mut self, processing_engine: Arc<ProcessingEngineManagerImpl>) -> Self {
        self.processing_engine = Some(processing_engine);
        self
    }

    pub async fn build<'a>(
        self,
        cert_file: Option<PathBuf>,
        key_file: Option<PathBuf>,
        tls_minimum_version: &'a [&'static SupportedProtocolVersion],
    ) -> Result<Server<'a>, &'static str> {
        let time_provider = self.time_provider.ok_or("time_provider is required")?;
        let write_buffer = self.write_buffer.ok_or("write_buffer is required")?;
        let query_executor = self.query_executor.ok_or("query_executor is required")?;
        let persister = self.persister.ok_or("persister is required")?;
        let listener = self.listener.ok_or("listener is required")?;
        let processing_engine = self.processing_engine.ok_or("processing_engine is required")?;

        Arc::clone(&processing_engine)
            .start_triggers()
            .await
            .expect("failed to start processing engine triggers");

        write_buffer.wal().add_file_notifier(Arc::clone(&processing_engine) as _);

        let http = Arc::new(HttpApi::new(
            self.common_state.clone(),
            Arc::clone(&time_provider),
            Arc::clone(&write_buffer),
            Arc::clone(&query_executor),
            Arc::clone(&processing_engine),
            self.max_request_size,
            Arc::clone(&self.authorizer),
        ));

        Ok(Server {
            common_state: self.common_state,
            http,
            cert_file,
            key_file,
            tls_minimum_version,
            persister,
            authorizer: self.authorizer,
            listener,
        })
    }
}
