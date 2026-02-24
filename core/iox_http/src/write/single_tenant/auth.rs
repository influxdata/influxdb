//! Authorization of HTTP requests using the authz service client.

use std::sync::Arc;

use authz::{
    self, Action, Authorizer, Error, Permission, Resource, Target, extract_token,
    http::AuthorizationHeaderExtension,
};
use data_types::NamespaceName;
use iox_http_util::Request;

pub(crate) async fn authorize(
    authz: &Arc<dyn Authorizer>,
    req: &Request,
    namespace: &NamespaceName<'_>,
    query_param_token: Option<String>,
) -> Result<(), Error> {
    let token = extract_token(
        req.extensions()
            .get::<AuthorizationHeaderExtension>()
            .and_then(|v| v.as_ref()),
    )
    .or_else(|| query_param_token.map(|t| t.into_bytes()));

    let perms = [Permission::ResourceAction(
        Resource::Database(Target::ResourceName(namespace.to_string())),
        Action::Write,
    )];

    authz.authorize(token, &perms).await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod mock {
    use async_trait::async_trait;
    use authz::{Authorization, Authorizer, Permission};

    pub(crate) const MOCK_AUTH_VALID_TOKEN: &str = "GOOD";
    pub(crate) const MOCK_AUTH_INVALID_TOKEN: &str = "UGLY";
    pub(crate) const MOCK_AUTH_NO_PERMS_TOKEN: &str = "BAD";

    #[derive(Debug, Default, Copy, Clone)]
    pub(crate) struct MockAuthorizer {}

    #[async_trait]
    impl Authorizer for MockAuthorizer {
        async fn authorize(
            &self,
            token: Option<Vec<u8>>,
            perms: &[Permission],
        ) -> Result<Authorization, authz::Error> {
            match token {
                Some(token) => match (&token as &dyn AsRef<[u8]>).as_ref() {
                    b"GOOD" => Ok(Authorization::new(
                        Some("GOOD user".to_owned()),
                        perms.to_vec(),
                    )),
                    b"BAD" => Err(authz::Error::Forbidden {
                        authorization: Authorization::new(Some("BAD user".to_owned()), vec![]),
                    }),
                    b"UGLY" => Err(authz::Error::verification("test", "test error")),
                    _ => panic!("unexpected token"),
                },
                None => Err(authz::Error::NoToken),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use hyper::header::HeaderValue;
    use iox_http_util::{RequestBuilder, empty_request_body};

    use super::mock::*;
    use super::*;

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

                    let request = RequestBuilder::new()
                        .uri(format!("https://any.com/ignored"))
                        .method("POST")
                        .extension(AuthorizationHeaderExtension::new(Some(
                            HeaderValue::from_str($header_value).unwrap(),
                        )))
                        .body(empty_request_body())
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
        want = Err(authz::Error::Forbidden { .. })
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
        want = Err(authz::Error::Forbidden { .. })
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
        want = Err(authz::Error::Forbidden { .. })
    );

    test_authorize!(
        everything_missing,
        header_value = "",
        query_param_token = None,
        want = Err(authz::Error::NoToken)
    );
}
