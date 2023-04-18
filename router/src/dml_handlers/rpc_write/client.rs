//! Abstraction over RPC client

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
impl WriteClient for WriteServiceClient<tonic::transport::Channel> {
    async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
        WriteServiceClient::write(&mut self.clone(), op).await?;
        Ok(())
    }
}

/// Mocks for testing
pub mod mock {
    use super::*;
    use parking_lot::Mutex;
    use std::{fmt::Debug, iter, sync::Arc};

    struct State {
        calls: Vec<WriteRequest>,
        ret: Box<dyn Iterator<Item = Result<(), RpcWriteError>> + Send + Sync>,
    }

    /// A mock implementation of the [`WriteClient`] for testing purposes.
    ///
    /// An instance yielded by the [`Default`] implementation will always return
    /// [`Ok(())`] for write calls.
    pub struct MockWriteClient {
        state: Mutex<State>,
    }

    impl Debug for MockWriteClient {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockWriteClient").finish()
        }
    }

    impl Default for MockWriteClient {
        fn default() -> Self {
            Self {
                state: Mutex::new(State {
                    calls: Default::default(),
                    ret: Box::new(iter::repeat_with(|| Ok(()))),
                }),
            }
        }
    }

    impl MockWriteClient {
        /// Retrieve the requests that this mock received.
        pub fn calls(&self) -> Vec<WriteRequest> {
            self.state.lock().calls.clone()
        }

        /// Read values off of the provided iterator and return them for calls
        /// to [`Self::write()`].
        #[cfg(test)]
        pub(crate) fn with_ret<T, U>(self, ret: T) -> Self
        where
            T: IntoIterator<IntoIter = U>,
            U: Iterator<Item = Result<(), RpcWriteError>> + Send + Sync + 'static,
        {
            self.state.lock().ret = Box::new(ret.into_iter());
            self
        }
    }

    #[async_trait]
    impl WriteClient for Arc<MockWriteClient> {
        async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
            let mut guard = self.state.lock();
            guard.calls.push(op);
            guard.ret.next().expect("no mock response")
        }
    }
}
