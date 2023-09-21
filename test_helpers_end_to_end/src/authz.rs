use futures::FutureExt;
use generated_types::influxdata::iox::authz::v1::{
    iox_authorizer_service_server::{IoxAuthorizerService, IoxAuthorizerServiceServer},
    permission::PermissionOneOf,
    resource_action_permission::{Action, ResourceType},
    AuthorizeRequest, AuthorizeResponse, Permission, ResourceActionPermission,
};
use observability_deps::tracing::{error, info};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{spawn, JoinHandle},
};
use tonic::transport::{server::TcpIncoming, Server};

#[derive(Debug)]
pub struct Authorizer {
    tokens: Arc<Mutex<HashMap<Vec<u8>, Vec<Permission>>>>,
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Authorizer {
    pub async fn create() -> Self {
        let listener = TcpListener::bind("localhost:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        info!("****************");
        info!(local_addr = %addr, "Authorizer started");
        info!("****************");

        let incoming = TcpIncoming::from_listener(listener, false, None).unwrap();
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
        }
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
    pub fn create_token_for(&mut self, namespace_name: &str, actions: &[&str]) -> String {
        let perms = actions
            .iter()
            .filter_map(|a| Action::from_str_name(a))
            .map(|a| Permission {
                permission_one_of: Some(PermissionOneOf::ResourceAction(
                    ResourceActionPermission {
                        resource_type: ResourceType::Database.into(),
                        resource_id: Some(namespace_name.to_string()),
                        action: a.into(),
                    },
                )),
            })
            .collect();
        let token = format!(
            "{namespace_name}_{}",
            thread_rng()
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

#[tonic::async_trait]
impl IoxAuthorizerService for AuthorizerService {
    async fn authorize(
        &self,
        request: tonic::Request<AuthorizeRequest>,
    ) -> Result<tonic::Response<AuthorizeResponse>, tonic::Status> {
        let request = request.into_inner();
        let recognized = self
            .tokens
            .lock()
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .get(&request.token)
            .cloned();
        let valid = recognized.is_some();
        let perms = recognized.unwrap_or_default();

        Ok(tonic::Response::new(AuthorizeResponse {
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
