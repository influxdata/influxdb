//! HTTP Write V1 and V2 implementation logic for both single, and multi-tenant
//! operational modes.

pub mod v1;
pub mod v2;

pub mod multi_tenant;
pub mod single_tenant;

mod params;
pub use params::*;

pub mod mock {
    use async_trait::async_trait;
    use iox_http_util::Request;
    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub enum MockUnifyingParseCall {
        V1,
        V2,
    }

    struct State {
        calls: Vec<MockUnifyingParseCall>,
        ret: Box<dyn Iterator<Item = Result<WriteParams, WriteParseError>> + Send + Sync>,
    }

    impl Default for State {
        fn default() -> Self {
            Self {
                calls: Default::default(),
                ret: Box::new(std::iter::empty()),
            }
        }
    }

    impl std::fmt::Debug for State {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("State").field("calls", &self.calls).finish()
        }
    }

    #[derive(Debug, Default)]
    pub struct MockWriteRequestUnifier {
        state: Mutex<State>,
    }

    impl MockWriteRequestUnifier {
        pub fn with_ret<T, U>(self, ret: T) -> Self
        where
            T: IntoIterator<IntoIter = U>,
            U: Iterator<Item = Result<WriteParams, WriteParseError>> + Send + Sync + 'static,
        {
            self.state.lock().ret = Box::new(ret.into_iter());
            self
        }

        pub fn calls(&self) -> Vec<MockUnifyingParseCall> {
            self.state.lock().calls.clone()
        }
    }

    #[async_trait]
    impl WriteRequestUnifier for MockWriteRequestUnifier {
        async fn parse_v1(&self, _req: &Request) -> Result<WriteParams, WriteParseError> {
            let mut guard = self.state.lock();
            guard.calls.push(MockUnifyingParseCall::V1);
            guard.ret.next().unwrap()
        }

        async fn parse_v2(&self, _req: &Request) -> Result<WriteParams, WriteParseError> {
            let mut guard = self.state.lock();
            guard.calls.push(MockUnifyingParseCall::V2);
            guard.ret.next().unwrap()
        }
    }
}
