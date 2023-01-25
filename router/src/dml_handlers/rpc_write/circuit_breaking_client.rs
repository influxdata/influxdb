use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use generated_types::influxdata::iox::ingester::v1::WriteRequest;

use super::{circuit_breaker::CircuitBreaker, client::WriteClient, RpcWriteError};

/// An internal abstraction over the health probing & result recording
/// functionality of a circuit breaker.
pub(super) trait CircuitBreakerState: Send + Sync + Debug {
    /// Returns `true` if this client can be used to make a request with an
    /// expectation of success.
    fn is_healthy(&self) -> bool;
    /// Returns `true` if this client should be used to attempt a request,
    /// expecting failure.
    fn should_probe(&self) -> bool;
    /// Record the result of a request made by this client.
    fn observe<T, E>(&self, r: &Result<T, E>);
}

impl CircuitBreakerState for CircuitBreaker {
    fn is_healthy(&self) -> bool {
        self.is_healthy()
    }
    fn should_probe(&self) -> bool {
        self.should_probe()
    }
    fn observe<T, E>(&self, r: &Result<T, E>) {
        self.observe(r)
    }
}

/// A thin composite type decorating the [`WriteClient`] functionality of `T`,
/// with circuit breaking logic from [`CircuitBreaker`].
#[derive(Debug)]
pub(super) struct CircuitBreakingClient<T, C = CircuitBreaker> {
    /// The underlying [`WriteClient`] implementation.
    inner: T,
    /// The circuit-breaking logic.
    state: C,

    /// A string description of the endpoint this [`CircuitBreakingClient`]
    /// dispatches requests to.
    ///
    /// Used for metric context only.
    endpoint_name: Arc<str>,
}

impl<T> CircuitBreakingClient<T> {
    pub(super) fn new(inner: T, endpoint_name: impl Into<Arc<str>>) -> Self {
        let endpoint_name = endpoint_name.into();
        let state = CircuitBreaker::new(Arc::clone(&endpoint_name));
        state.set_healthy();
        Self {
            inner,
            state,
            endpoint_name,
        }
    }
}

impl<T, C> CircuitBreakingClient<T, C> {
    /// Returns the name of the endpoint assigned at initialisation.
    pub(crate) fn endpoint_name(&self) -> Arc<str> {
        Arc::clone(&self.endpoint_name)
    }
}

impl<T, C> CircuitBreakingClient<T, C>
where
    C: CircuitBreakerState,
{
    /// Returns `true` if this client can be used to make a request with an
    /// expectation of success.
    pub(super) fn is_healthy(&self) -> bool {
        self.state.is_healthy()
    }
    pub(super) fn should_probe(&self) -> bool {
        self.state.should_probe()
    }

    #[cfg(test)]
    pub(super) fn with_circuit_breaker<U>(self, breaker: U) -> CircuitBreakingClient<T, U> {
        CircuitBreakingClient {
            inner: self.inner,
            endpoint_name: self.endpoint_name,
            state: breaker,
        }
    }
}

#[async_trait]
impl<T, C> WriteClient for &CircuitBreakingClient<T, C>
where
    T: WriteClient,
    C: CircuitBreakerState,
{
    async fn write(&self, op: WriteRequest) -> Result<(), RpcWriteError> {
        let res = self.inner.write(op).await;
        self.state.observe(&res);
        res
    }
}

#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Debug, Default)]
    pub(crate) struct MockCircuitBreaker {
        is_healthy: AtomicBool,
        is_healthy_calls: AtomicUsize,
        should_probe: AtomicBool,
        should_probe_calls: AtomicUsize,
        ok: AtomicUsize,
        err: AtomicUsize,
    }

    impl MockCircuitBreaker {
        pub(crate) fn set_healthy(&self, healthy: bool) {
            self.is_healthy.store(healthy, Ordering::Relaxed);
        }
        pub(crate) fn set_should_probe(&self, should_probe: bool) {
            self.should_probe.store(should_probe, Ordering::Relaxed);
        }
        pub(crate) fn ok_count(&self) -> usize {
            self.ok.load(Ordering::Relaxed)
        }
        pub(crate) fn err_count(&self) -> usize {
            self.err.load(Ordering::Relaxed)
        }
        pub(crate) fn is_healthy_count(&self) -> usize {
            self.is_healthy_calls.load(Ordering::Relaxed)
        }
        pub(crate) fn should_probe_count(&self) -> usize {
            self.should_probe_calls.load(Ordering::Relaxed)
        }
    }

    impl CircuitBreakerState for Arc<MockCircuitBreaker> {
        fn is_healthy(&self) -> bool {
            self.is_healthy_calls.fetch_add(1, Ordering::Relaxed);
            self.is_healthy.load(Ordering::Relaxed)
        }

        fn observe<T, E>(&self, r: &Result<T, E>) {
            match r {
                Ok(_) => &self.ok,
                Err(_) => &self.err,
            }
            .fetch_add(1, Ordering::Relaxed);
        }

        fn should_probe(&self) -> bool {
            self.should_probe_calls.fetch_add(1, Ordering::Relaxed);
            self.should_probe.load(Ordering::Relaxed)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, sync::Arc};

    use crate::dml_handlers::rpc_write::client::mock::MockWriteClient;

    use super::{mock::MockCircuitBreaker, *};

    #[tokio::test]
    async fn test_healthy() {
        let circuit_breaker = Arc::new(MockCircuitBreaker::default());
        let wrapper = CircuitBreakingClient::new(MockWriteClient::default(), "bananas")
            .with_circuit_breaker(Arc::clone(&circuit_breaker));

        circuit_breaker.set_healthy(true);
        assert_eq!(wrapper.is_healthy(), circuit_breaker.is_healthy());
        circuit_breaker.set_healthy(false);
        assert_eq!(wrapper.is_healthy(), circuit_breaker.is_healthy());
        circuit_breaker.set_healthy(true);
        assert_eq!(wrapper.is_healthy(), circuit_breaker.is_healthy());

        circuit_breaker.set_should_probe(true);
        assert_eq!(wrapper.should_probe(), circuit_breaker.should_probe());
        circuit_breaker.set_should_probe(false);
        assert_eq!(wrapper.should_probe(), circuit_breaker.should_probe());
        circuit_breaker.set_should_probe(true);
        assert_eq!(wrapper.should_probe(), circuit_breaker.should_probe());
    }

    #[tokio::test]
    async fn test_observe() {
        let circuit_breaker = Arc::new(MockCircuitBreaker::default());
        let mock_client = Arc::new(
            MockWriteClient::default()
                .with_ret(vec![Ok(()), Err(RpcWriteError::DeletesUnsupported)]),
        );
        let wrapper = CircuitBreakingClient::new(Arc::clone(&mock_client), "bananas")
            .with_circuit_breaker(Arc::clone(&circuit_breaker));

        assert_eq!(circuit_breaker.ok_count(), 0);
        assert_eq!(circuit_breaker.err_count(), 0);

        wrapper
            .borrow()
            .write(WriteRequest::default())
            .await
            .expect("wrapper should return Ok mock value");
        assert_eq!(circuit_breaker.ok_count(), 1);
        assert_eq!(circuit_breaker.err_count(), 0);

        wrapper
            .borrow()
            .write(WriteRequest::default())
            .await
            .expect_err("wrapper should return Err mock value");
        assert_eq!(circuit_breaker.ok_count(), 1);
        assert_eq!(circuit_breaker.err_count(), 1);
    }
}
