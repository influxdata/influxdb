use async_trait::async_trait;
use generated_types::influxdata::iox::ingester::v1::{
    write_service_client::WriteServiceClient, WriteRequest,
};

use super::RpcWriteError;

/// An abstract RPC client that pushes `op` to an opaque receiver.
#[async_trait]
pub(super) trait WriteClient: Send + Sync + std::fmt::Debug {
    /// Write `op` and wait for a response.
    async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError>;
}

/// An implementation of [`WriteClient`] for the tonic gRPC client.
#[async_trait]
impl WriteClient for WriteServiceClient<client_util::connection::GrpcConnection> {
    async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
        WriteServiceClient::write(&mut self.clone(), op).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use std::{collections::VecDeque, sync::Arc};

    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, Default)]
    struct State {
        calls: Vec<WriteRequest>,
        ret: VecDeque<Result<(), RpcWriteError>>,
    }

    /// A mock implementation of the [`WriteClient`] for testing purposes.
    #[derive(Debug, Default)]
    pub(crate) struct MockWriteClient {
        state: Mutex<State>,
    }

    impl MockWriteClient {
        pub(crate) fn calls(&self) -> Vec<WriteRequest> {
            self.state.lock().calls.clone()
        }

        pub(crate) fn with_ret(self, ret: impl Into<VecDeque<Result<(), RpcWriteError>>>) -> Self {
            self.state.lock().ret = ret.into();
            self
        }
    }

    #[async_trait]
    impl WriteClient for Arc<MockWriteClient> {
        async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
            let mut guard = self.state.lock();
            guard.calls.push(op);
            guard.ret.pop_front().unwrap_or(Ok(()))
        }
    }
}
