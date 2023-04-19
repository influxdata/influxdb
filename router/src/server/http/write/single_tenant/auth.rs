//! Authorization of HTTP requests using the authz service client.

use std::sync::Arc;

use authz::{
    self, http::AuthorizationHeaderExtension, Action, Authorizer, Error, Permission, Resource,
};
use base64::{prelude::BASE64_STANDARD, Engine};
use data_types::NamespaceName;
use hyper::{header::HeaderValue, Body, Request};

fn extract_header_token(header_value: &'_ HeaderValue) -> Option<Vec<u8>> {
    let mut parts = header_value.as_bytes().splitn(2, |&v| v == b' ');
    let token = match parts.next()? {
        b"Token" | b"Bearer" => parts.next()?.to_vec(),
        b"Basic" => parts
            .next()
            .and_then(|v| BASE64_STANDARD.decode(v).ok())?
            .splitn(2, |&v| v == b':')
            .nth(1)?
            .to_vec(),
        _ => return None,
    };
    if token.is_empty() {
        return None;
    }
    Some(token)
}

pub(crate) async fn authorize(
    authz: &Arc<dyn Authorizer>,
    req: &Request<Body>,
    namespace: &NamespaceName<'_>,
    query_param_token: Option<String>,
) -> Result<(), Error> {
    let token = req
        .extensions()
        .get::<AuthorizationHeaderExtension>()
        .and_then(|v| v.as_ref())
        .and_then(|v| {
            extract_header_token(v).or_else(|| query_param_token.map(|t| t.into_bytes()))
        });

    let perms = [Permission::ResourceAction(
        Resource::Database(namespace.to_string()),
        Action::Write,
    )];

    authz
        .require_any_permission(token.as_deref(), &perms)
        .await?;
    Ok(())
}

#[cfg(test)]
pub mod mock {
    use async_trait::async_trait;

    use super::*;

    pub const MOCK_AUTH_VALID_TOKEN: &str = "GOOD";
    pub const MOCK_AUTH_INVALID_TOKEN: &str = "UGLY";
    pub const MOCK_AUTH_NO_PERMS_TOKEN: &str = "BAD";

    #[derive(Debug, Default)]
    pub struct MockAuthorizer {}

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn permissions(
            &self,
            token: Option<&[u8]>,
            perms: &[Permission],
        ) -> Result<Vec<Permission>, authz::Error> {
            match token {
                Some(b"GOOD") => Ok(perms.to_vec()),
                Some(b"UGLY") => Err(authz::Error::verification("test", "test error")),
                Some(_) => Ok(vec![]),
                None => Err(authz::Error::NoToken),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use data_types::NamespaceId;
    use hyper::header::HeaderValue;

    use super::{mock::*, *};
    use crate::{
        dml_handlers::mock::{MockDmlHandler, MockDmlHandlerCall},
        namespace_resolver::mock::MockNamespaceResolver,
        server::http::{
            self,
            write::single_tenant::{SingleTenantExtractError, SingleTenantRequestUnifier},
            HttpDelegate,
        },
    };

    const MAX_BYTES: usize = 1024;

    #[tokio::test]
    async fn test_authz_service_integration() {
        static NAMESPACE_NAME: &str = "test";
        let mock_namespace_resolver =
            MockNamespaceResolver::default().with_mapping(NAMESPACE_NAME, NamespaceId::new(42));

        let dml_handler = Arc::new(MockDmlHandler::default().with_write_return([Ok(())]));
        let metrics = Arc::new(metric::Registry::default());
        let authz = Arc::new(MockAuthorizer::default());
        let delegate = HttpDelegate::new(
            MAX_BYTES,
            1,
            mock_namespace_resolver,
            Arc::clone(&dml_handler),
            &metrics,
            Box::new(SingleTenantRequestUnifier::new(authz)),
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str(format!("Token {MOCK_AUTH_VALID_TOKEN}").as_str()).unwrap(),
            )))
            .body(Body::from("platanos,tag1=A,tag2=B val=42i 123456"))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(got, Ok(_));

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str(format!("Token {MOCK_AUTH_NO_PERMS_TOKEN}").as_str())
                    .unwrap(),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::Forbidden)
            ))
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::NoToken)
            ))
        );

        let request = Request::builder()
            .uri("https://bananas.example/api/v2/write?org=bananas&bucket=test")
            .method("POST")
            .extension(AuthorizationHeaderExtension::new(Some(
                HeaderValue::from_str(format!("Token {MOCK_AUTH_INVALID_TOKEN}").as_str()).unwrap(),
            )))
            .body(Body::from(""))
            .unwrap();

        let got = delegate.route(request).await;
        assert_matches!(
            got,
            Err(http::Error::SingleTenantError(
                SingleTenantExtractError::Authorizer(authz::Error::Verification { .. })
            ))
        );

        let calls = dml_handler.calls();
        assert_matches!(calls.as_slice(), [MockDmlHandlerCall::Write{namespace, ..}] => {
            assert_eq!(namespace, NAMESPACE_NAME);
        })
    }

    macro_rules! test_authorize {
        (
            $name:ident,
            header_value = $header_value:expr,       // If present, set as header
            query_param_token = $query_token:expr,   // Optional token provided as ?q=<token>
            want = $($want:tt)+                      // A pattern match for assert_matches!
        ) => {
            paste::paste! {
                #[tokio::test]
                async fn [<test_authorize_ $name>]() {
                    let authz: Arc<dyn Authorizer> = Arc::new(MockAuthorizer::default());
                    let namespace = NamespaceName::new("test").unwrap();

                    let request = Request::builder()
                        .uri(format!("https://any.com/ignored"))
                        .method("POST")
                        .extension(AuthorizationHeaderExtension::new(Some(
                            HeaderValue::from_str($header_value).unwrap(),
                        )))
                        .body(Body::from(""))
                        .unwrap();

                    let got = authorize(&authz, &request, &namespace, $query_token).await;
                    assert_matches!(got, $($want)+);
                }
            }
        };
    }

    fn encode_basic_header(token: String) -> String {
        format!("Basic {}", BASE64_STANDARD.encode(token))
    }

    test_authorize!(
        token_header_ok,
        header_value = format!("Token {MOCK_AUTH_VALID_TOKEN}").as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        token_header_rejected,
        header_value = format!("Token {MOCK_AUTH_INVALID_TOKEN}").as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        token_header_forbidden,
        header_value = format!("Token {MOCK_AUTH_NO_PERMS_TOKEN}").as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        token_header_missing,
        header_value = "Token ",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        token_header_missing_whitespace,
        header_value = "Token",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        token_header_missing_whitespace_match_next,
        header_value = "Token",
        query_param_token = Some(MOCK_AUTH_VALID_TOKEN.to_string()),
        want = Ok(())
    );

    test_authorize!(
        bearer_header_ok,
        header_value = format!("Bearer {MOCK_AUTH_VALID_TOKEN}").as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        bearer_header_missing,
        header_value = "Bearer ",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_ok,
        header_value = encode_basic_header(format!("ignore:{MOCK_AUTH_VALID_TOKEN}")).as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Ok(())
    );

    test_authorize!(
        basic_header_missing,
        header_value = encode_basic_header("".to_string()).as_str(),
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_missing_part,
        header_value = encode_basic_header("ignore:".to_string()).as_str(),
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );

    test_authorize!(
        basic_header_rejected,
        header_value = encode_basic_header(format!("ignore:{MOCK_AUTH_INVALID_TOKEN}")).as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        basic_header_forbidden,
        header_value = encode_basic_header(format!("ignore:{MOCK_AUTH_NO_PERMS_TOKEN}")).as_str(),
        query_param_token = Some("ignore".to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        query_param_token_ok,
        header_value = "",
        query_param_token = Some(MOCK_AUTH_VALID_TOKEN.to_string()),
        want = Ok(())
    );

    test_authorize!(
        query_param_token_rejected,
        header_value = "",
        query_param_token = Some(MOCK_AUTH_INVALID_TOKEN.to_string()),
        want = Err(authz::Error::Verification { .. })
    );

    test_authorize!(
        query_param_token_forbidden,
        header_value = "",
        query_param_token = Some(MOCK_AUTH_NO_PERMS_TOKEN.to_string()),
        want = Err(authz::Error::Forbidden)
    );

    test_authorize!(
        everything_missing,
        header_value = "",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );
}
