use data_types::server_id::ServerId;
use std::collections::BTreeMap;

/// A RemoteTemplate string is a remote connection template string.
/// Occurrences of the substring "{id}" in the template will be replaced
/// by the server ID.
#[derive(Debug)]
pub struct RemoteTemplate {
    template: String,
}

impl RemoteTemplate {
    pub fn new(template: impl Into<String>) -> Self {
        let template = template.into();
        Self { template }
    }
}

/// A gRPC connection string.
pub type GrpcConnectionString = String;

/// The Resolver provides a mapping between ServerId and GRpcConnectionString
#[derive(Debug)]
pub struct Resolver {
    /// Map between remote IOx server IDs and management API connection strings.
    remotes: BTreeMap<ServerId, GrpcConnectionString>,

    /// Static map between remote server IDs and hostnames based on a template
    remote_template: Option<RemoteTemplate>,
}

impl Resolver {
    pub fn new(remote_template: Option<RemoteTemplate>) -> Self {
        Self {
            remotes: Default::default(),
            remote_template,
        }
    }

    /// Get all registered remote servers.
    pub fn remotes_sorted(&self) -> Vec<(ServerId, String)> {
        self.remotes.iter().map(|(&a, b)| (a, b.clone())).collect()
    }

    /// Update given remote server.
    pub fn update_remote(&mut self, id: ServerId, addr: GrpcConnectionString) {
        self.remotes.insert(id, addr);
    }

    /// Delete remote server by ID.
    pub fn delete_remote(&mut self, id: ServerId) -> Option<GrpcConnectionString> {
        self.remotes.remove(&id)
    }
}
