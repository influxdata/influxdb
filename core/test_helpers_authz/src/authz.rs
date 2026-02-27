use futures::FutureExt;
use generated_types::{
    Request, Response, Status,
    influxdata::iox::authz::v1::{
        AuthorizeRequest, AuthorizeResponse, Permission, ResourceActionPermission,
        iox_authorizer_service_server::{IoxAuthorizerService, IoxAuthorizerServiceServer},
        permission::PermissionOneOf,
        resource_action_permission::{Action, ResourceType, Target},
    },
    transport::{self, Server, server::TcpIncoming},
};
use rand::{Rng, distr::Alphanumeric, rng};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{JoinHandle, spawn},
};
use tracing::{error, info};

#[derive(Debug)]
pub struct Authorizer {
    tokens: Arc<Mutex<HashMap<Vec<u8>, Vec<Permission>>>>,
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), transport::Error>>,
    // Whether to use new or old token format
    // true - use name-only token format (legacy)
    // false - use name-and-id token format
    legacy_token_mode: bool,
}

impl Authorizer {
    pub async fn create() -> Self {
        let listener = TcpListener::bind("localhost:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        info!("****************");
        info!(local_addr = %addr, "Authorizer started");
        info!("****************");

        let incoming = TcpIncoming::from(listener).with_nodelay(Some(false));
        let (stop, stop_rx) = oneshot::channel();
        let tokens = Arc::new(Mutex::new(HashMap::new()));
        let router = Server::builder().add_service(IoxAuthorizerServiceServer::new(
            AuthorizerService::new(Arc::clone(&tokens)),
        ));
        let handle = spawn(router.serve_with_incoming_shutdown(incoming, stop_rx.map(drop)));

        Self {
            tokens,
            addr,
            stop: Some(stop),
            handle,
            legacy_token_mode: false,
        }
    }

    /// Whether to use new or old token format
    /// true - use name-only token format (legacy)
    /// false - use name-and-id token format
    pub fn use_legacy_tokens(mut self, legacy_tokens: bool) -> Self {
        self.legacy_token_mode = legacy_tokens;
        self
    }

    pub async fn close(mut self) {
        info!("****************");
        info!(local_addr = %self.addr, "Stopping authorizer");
        info!("****************");
        if let Some(stop) = self.stop.take() {
            stop.send(()).expect("Error stopping authorizer");
        };
        match self.handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!(error = %e, "Error stopping authorizer"),
            Err(e) => error!(error = %e, "Error stopping authorizer"),
        }
    }

    /// Create a new token with the requested permissions.
    ///
    /// Actions are specified by name, the currently supported actions are:
    ///  - `"ACTION_READ_SCHEMA"`
    ///  - `"ACTION_READ"`
    ///  - `"ACTION_WRITE"`
    ///  - `"ACTION_CREATE"`
    ///  - `"ACTION_DELETE"`
    pub fn create_token_for(
        &mut self,
        namespace_name: &str,
        namespace_id: &str,
        actions: &[&str],
    ) -> String {
        let perms = actions
            .iter()
            .filter_map(|a| Action::from_str_name(a))
            .flat_map(|a| {
                let targets = if self.legacy_token_mode {
                    vec![Target::ResourceName(namespace_name.to_string())]
                } else {
                    vec![
                        Target::ResourceName(namespace_name.to_string()),
                        Target::ResourceId(namespace_id.to_string()),
                    ]
                };
                targets.into_iter().map(move |t| Permission {
                    permission_one_of: Some(PermissionOneOf::ResourceAction(
                        ResourceActionPermission {
                            resource_type: ResourceType::Database.into(),
                            target: Some(t),
                            action: a.into(),
                        },
                    )),
                })
            })
            .collect();
        let token = format!(
            "{namespace_name}_{namespace_id}_{}",
            rng()
                .sample_iter(&Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>()
        );
        self.tokens
            .lock()
            .unwrap()
            .insert(token.clone().into_bytes(), perms);
        token
    }

    /// Get the address the server is listening at.
    pub fn addr(&self) -> String {
        format!("http://{}", self.addr)
    }
}

/// Test implementation of the IoxAuthorizationService.
#[derive(Debug)]
struct AuthorizerService {
    tokens: Arc<Mutex<HashMap<Vec<u8>, Vec<Permission>>>>,
}

impl AuthorizerService {
    /// Create new TestAuthorizationService.
    fn new(tokens: Arc<Mutex<HashMap<Vec<u8>, Vec<Permission>>>>) -> Self {
        Self { tokens }
    }
}

#[generated_types::async_trait]
impl IoxAuthorizerService for AuthorizerService {
    async fn authorize(
        &self,
        request: Request<AuthorizeRequest>,
    ) -> Result<Response<AuthorizeResponse>, Status> {
        let request = request.into_inner();
        let recognized = self
            .tokens
            .lock()
            .map_err(|e| Status::internal(e.to_string()))?
            .get(&request.token)
            .cloned();
        let valid = recognized.is_some();
        let perms = recognized.unwrap_or_default();

        Ok(Response::new(AuthorizeResponse {
            valid,
            subject: None,
            permissions: request
                .permissions
                .iter()
                .filter(|p| perms.contains(p))
                .cloned()
                .collect(),
        }))
    }
}
