use async_trait::async_trait;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, Registry};

use super::{Authorizer, Error, Permission};

const AUTHZ_DURATION_METRIC: &str = "authz_permission_check_duration";

/// An instrumentation decorator over a [`Authorizer`] implementation.
///
/// This wrapper captures the latency distribution of the decorated
/// [`Authorizer::permissions()`] call, faceted by success/error result.
#[derive(Debug)]
pub struct AuthorizerInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// Permissions-check duration distribution for successesful rpc, but not authorized.
    ioxauth_rpc_duration_success_unauth: DurationHistogram,

    /// Permissions-check duration distribution for successesful rpc + authorized.
    ioxauth_rpc_duration_success_auth: DurationHistogram,

    /// Permissions-check duration distribution for errors.
    ioxauth_rpc_duration_error: DurationHistogram,
}

impl<T> AuthorizerInstrumentation<T> {
    /// Record permissions-check duration metrics, broken down by result.
    pub fn new(registry: &Registry, inner: T) -> Self {
        let metric: Metric<DurationHistogram> =
            registry.register_metric(AUTHZ_DURATION_METRIC, "duration of authz permissions check");

        let ioxauth_rpc_duration_success_unauth =
            metric.recorder(&[("result", "success"), ("auth_state", "unauthorised")]);
        let ioxauth_rpc_duration_success_auth =
            metric.recorder(&[("result", "success"), ("auth_state", "authorised")]);
        let ioxauth_rpc_duration_error =
            metric.recorder(&[("result", "error"), ("auth_state", "unauthorised")]);

        Self {
            inner,
            time_provider: Default::default(),
            ioxauth_rpc_duration_success_unauth,
            ioxauth_rpc_duration_success_auth,
            ioxauth_rpc_duration_error,
        }
    }
}

#[async_trait]
impl<T> Authorizer for AuthorizerInstrumentation<T>
where
    T: Authorizer,
{
    async fn permissions(
        &self,
        token: Option<Vec<u8>>,
        perms: &[Permission],
    ) -> Result<Vec<Permission>, Error> {
        let t = self.time_provider.now();
        let res = self.inner.permissions(token, perms).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.ioxauth_rpc_duration_success_auth.record(delta),
                Err(Error::Forbidden) | Err(Error::InvalidToken) => {
                    self.ioxauth_rpc_duration_success_unauth.record(delta)
                }
                Err(Error::Verification { .. }) => self.ioxauth_rpc_duration_error.record(delta),
                Err(Error::NoToken) => {} // rpc was not made
            };
        }

        res
    }
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;

    use metric::{assert_histogram, Attributes, Registry};
    use parking_lot::Mutex;

    use super::*;
    use crate::{Action, Resource};

    #[derive(Debug, Default)]
    struct MockAuthorizerState {
        ret: VecDeque<Result<Vec<Permission>, Error>>,
    }

    #[derive(Debug, Default)]
    struct MockAuthorizer {
        state: Mutex<MockAuthorizerState>,
    }

    impl MockAuthorizer {
        pub(crate) fn with_permissions_return(
            self,
            ret: impl Into<VecDeque<Result<Vec<Permission>, Error>>>,
        ) -> Self {
            self.state.lock().ret = ret.into();
            self
        }
    }

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            _token: Option<Vec<u8>>,
            _perms: &[Permission],
        ) -> Result<Vec<Permission>, Error> {
            self.state
                .lock()
                .ret
                .pop_front()
                .expect("no mock sink value to return")
        }
    }

    macro_rules! assert_metric_counts {
        (
            $metrics:ident,
            expected_success = $expected_success:expr,
            expected_rpc_success_unauth = $expected_rpc_success_unauth:expr,
            expected_rpc_failures = $expected_rpc_failures:expr,
        ) => {
            let histogram = &$metrics
                .get_instrument::<Metric<DurationHistogram>>(AUTHZ_DURATION_METRIC)
                .expect("failed to read metric");

            let success_labels =
                Attributes::from(&[("result", "success"), ("auth_state", "authorised")]);
            let histogram_success = &histogram
                .get_observer(&success_labels)
                .expect("failed to find metric with provided attributes")
                .fetch();

            assert_histogram!(
                $metrics,
                DurationHistogram,
                AUTHZ_DURATION_METRIC,
                labels = success_labels,
                samples = $expected_success,
                sum = histogram_success.total,
            );

            let success_unauth_labels =
                Attributes::from(&[("result", "success"), ("auth_state", "unauthorised")]);
            let histogram_success_unauth = &histogram
                .get_observer(&success_unauth_labels)
                .expect("failed to find metric with provided attributes")
                .fetch();

            assert_histogram!(
                $metrics,
                DurationHistogram,
                AUTHZ_DURATION_METRIC,
                labels = success_unauth_labels,
                samples = $expected_rpc_success_unauth,
                sum = histogram_success_unauth.total,
            );

            let rpc_error_labels =
                Attributes::from(&[("result", "error"), ("auth_state", "unauthorised")]);
            let histogram_rpc_error = &histogram
                .get_observer(&rpc_error_labels)
                .expect("failed to find metric with provided attributes")
                .fetch();

            assert_histogram!(
                $metrics,
                DurationHistogram,
                AUTHZ_DURATION_METRIC,
                labels = rpc_error_labels,
                samples = $expected_rpc_failures,
                sum = histogram_rpc_error.total,
            );
        };
    }

    macro_rules! test_authorizer_metric {
        (
            $name:ident,
            rpc_response = $rpc_response:expr,
            will_pass_auth = $will_pass_auth:expr,
            expected_success_cnt = $expected_success_cnt:expr,
            expected_success_unauth_cnt = $expected_success_unauth_cnt:expr,
            expected_error_cnt = $expected_error_cnt:expr,
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_authorizer_metric_ $name>]() {
                    let metrics = Registry::default();
                    let decorated_authz = AuthorizerInstrumentation::new(
                        &metrics,
                        MockAuthorizer::default().with_permissions_return([$rpc_response])
                    );

                    let token = "any".as_bytes().to_vec();
                    let got = decorated_authz
                        .permissions(Some(token), &[])
                        .await;
                    assert_eq!(got.is_ok(), $will_pass_auth);
                    assert_metric_counts!(
                        metrics,
                        expected_success = $expected_success_cnt,
                        expected_rpc_success_unauth = $expected_success_unauth_cnt,
                        expected_rpc_failures = $expected_error_cnt,
                    );
                }
            }
        };
    }

    test_authorizer_metric!(
        ok,
        rpc_response = Ok(vec![Permission::ResourceAction(
            Resource::Database("foo".to_string()),
            Action::Write,
        )]),
        will_pass_auth = true,
        expected_success_cnt = 1,
        expected_success_unauth_cnt = 0,
        expected_error_cnt = 0,
    );

    test_authorizer_metric!(
        will_record_failure_if_rpc_fails,
        rpc_response = Err(Error::verification("test", "test error")),
        will_pass_auth = false,
        expected_success_cnt = 0,
        expected_success_unauth_cnt = 0,
        expected_error_cnt = 1,
    );

    test_authorizer_metric!(
        will_record_success_if_rpc_pass_but_auth_fails,
        rpc_response = Err(Error::InvalidToken),
        will_pass_auth = false,
        expected_success_cnt = 0,
        expected_success_unauth_cnt = 1,
        expected_error_cnt = 0,
    );
}
