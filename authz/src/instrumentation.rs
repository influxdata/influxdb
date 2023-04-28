use async_trait::async_trait;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, Registry};

use super::{Authorizer, Error, Permission};

const AUTHZ_DURATION_METRIC: &str = "authz_permissions_duration";

/// An instrumentation decorator over a [`Authorizer`] implementation.
///
/// This wrapper captures the latency distribution of the decorated
/// [`Authorizer::permissions()`] call, faceted by success/error result.
#[derive(Debug)]
pub struct AuthorizerInstrumentation<T, P = SystemProvider> {
    inner: T,
    time_provider: P,

    /// Permissions-check duration distribution for successes.
    ioxauth_rpc_duration_success: DurationHistogram,

    /// Permissions-check duration distribution for errors.
    ioxauth_rpc_duration_error: DurationHistogram,
}

impl<T> AuthorizerInstrumentation<T> {
    /// Record permissions-check duration metrics, broken down by result.
    pub fn new(registry: &Registry, inner: T) -> Self {
        let metric: Metric<DurationHistogram> =
            registry.register_metric(AUTHZ_DURATION_METRIC, "duration of authz permissions check");

        let ioxauth_rpc_duration_success = metric.recorder(&[("result", "success")]);
        let ioxauth_rpc_duration_error = metric.recorder(&[("result", "error")]);

        Self {
            inner,
            time_provider: Default::default(),
            ioxauth_rpc_duration_success,
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
                Ok(_) => self.ioxauth_rpc_duration_success.record(delta),
                Err(_) => self.ioxauth_rpc_duration_error.record(delta),
            };
        }

        res
    }
}

mod test {
    use metric::{Attributes, Registry};

    use super::*;
    use crate::{Action, Resource};

    enum IoxAuthPermissions {
        Good,
        Err,
        NoPerms,
    }
    impl From<IoxAuthPermissions> for Vec<u8> {
        fn from(value: IoxAuthPermissions) -> Self {
            match value {
                IoxAuthPermissions::Good => b"GOOD".to_vec(),
                IoxAuthPermissions::NoPerms => b"IoxAuthReturnsOkWithNoPerms".to_vec(),
                IoxAuthPermissions::Err => b"IoxAuthReturnsErr".to_vec(),
            }
        }
    }
    impl TryFrom<Vec<u8>> for IoxAuthPermissions {
        type Error = ();
        fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
            match value.as_slice() {
                b"GOOD" => Ok(Self::Good),
                b"IoxAuthReturnsOkWithNoPerms" => Ok(Self::NoPerms),
                b"IoxAuthReturnsErr" => Ok(Self::Err),
                _ => Err(()),
            }
        }
    }

    #[derive(Debug, Default)]
    struct MockAuthorizer;

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            token: Option<Vec<u8>>,
            _perms: &[Permission],
        ) -> Result<Vec<Permission>, Error> {
            match token {
                Some(token) => match IoxAuthPermissions::try_from(token) {
                    Ok(IoxAuthPermissions::Good) => Ok(vec![Permission::ResourceAction(
                        Resource::Database("foo".to_string()),
                        Action::Write,
                    )]),
                    Ok(IoxAuthPermissions::NoPerms) => Ok(vec![]),
                    Ok(IoxAuthPermissions::Err) | Err(_) => {
                        Err(Error::verification("test", "test error"))
                    }
                },
                None => Err(Error::NoToken),
            }
        }
    }

    #[allow(dead_code)]
    fn assert_metric_counts(metrics: &Registry, expected_success: u64, expected_err: u64) -> bool {
        let histogram = &metrics
            .get_instrument::<Metric<DurationHistogram>>(AUTHZ_DURATION_METRIC)
            .expect("failed to read metric");

        assert_eq!(
            histogram
                .get_observer(&Attributes::from(&[("result", "success"),]))
                .expect("failed to get observer")
                .fetch()
                .sample_count(),
            expected_success,
            "success counts did not match"
        );
        assert_eq!(
            histogram
                .get_observer(&Attributes::from(&[("result", "error"),]))
                .expect("failed to get observer")
                .fetch()
                .sample_count(),
            expected_err,
            "error counts did not match"
        );
        true
    }

    #[tokio::test]
    async fn test_authz_metric_record_for_all_exposed_interfaces() {
        let metrics = Registry::default();
        let decorated_authz = AuthorizerInstrumentation::new(&metrics, MockAuthorizer::default());

        let token_good: Vec<u8> = IoxAuthPermissions::Good.into();
        let got = decorated_authz
            .permissions(Some(token_good.clone()), &[])
            .await;
        assert!(got.is_ok());
        assert!(
            assert_metric_counts(&metrics, 1, 0),
            "Authorizer::permissions() calls are recorded"
        );

        let got = decorated_authz
            .require_any_permission(Some(token_good), &[])
            .await;
        assert!(got.is_ok());
        assert!(
            assert_metric_counts(&metrics, 2, 0),
            "Authorizer::require_any_permission() calls are recorded"
        );

        let token_err_perms: Vec<u8> = IoxAuthPermissions::Err.into();
        let err = decorated_authz
            .require_any_permission(Some(token_err_perms), &[])
            .await;
        assert!(err.is_err());
        assert!(assert_metric_counts(&metrics, 2, 1), "errors are recorded");
    }

    #[tokio::test]
    async fn test_metric_records_only_rpc_errors() {
        let metrics = Registry::default();
        let decorated_authz = AuthorizerInstrumentation::new(&metrics, MockAuthorizer::default());

        let token_err_rpc: Vec<u8> = IoxAuthPermissions::Err.into();
        let err = decorated_authz
            .require_any_permission(Some(token_err_rpc), &[])
            .await;
        assert!(err.is_err());
        assert!(
            assert_metric_counts(&metrics, 0, 1),
            "rpc errors are recorded"
        );

        let token_rpc_fine_only_missing_perms: Vec<u8> = IoxAuthPermissions::NoPerms.into();
        let err = decorated_authz
            .require_any_permission(Some(token_rpc_fine_only_missing_perms), &[])
            .await;
        assert!(
            err.is_err(),
            "post-rpc check of permission, should return error"
        );
        assert!(
            assert_metric_counts(&metrics, 1, 1),
            "no rpc error recorded"
        );
    }
}
