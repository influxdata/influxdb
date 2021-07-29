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

    fn get(&self, id: &ServerId) -> GrpcConnectionString {
        self.template.replace("{id}", &format!("{}", id.get_u32()))
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

    /// Get remote server by ID.
    pub fn resolve_remote(&self, id: ServerId) -> Option<GrpcConnectionString> {
        self.remotes
            .get(&id)
            .cloned()
            .or_else(|| self.remote_template.as_ref().map(|t| t.get(&id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    #[test]
    fn resolve_remote() {
        let resolver = Resolver::new(Some(RemoteTemplate::new("http://iox-query-{id}:8082")));

        let server_id = ServerId::new(NonZeroU32::new(42).unwrap());
        let remote = resolver.resolve_remote(server_id);
        assert_eq!(
            remote,
            Some(GrpcConnectionString::from("http://iox-query-42:8082"))
        );

        let server_id = ServerId::new(NonZeroU32::new(24).unwrap());
        let remote = resolver.resolve_remote(server_id);
        assert_eq!(
            remote,
            Some(GrpcConnectionString::from("http://iox-query-24:8082"))
        );
    }
}
