use data_types::server_id::ServerId;
use parking_lot::RwLock;
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

    fn get(&self, id: &ServerId) -> String {
        self.template.replace("{id}", &format!("{}", id.get_u32()))
    }
}

/// The Resolver provides a mapping between ServerId and GRpcConnectionString
#[derive(Debug)]
pub struct Resolver {
    /// Map between remote IOx server IDs and management API connection strings.
    remotes: RwLock<BTreeMap<ServerId, String>>,

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

    /// Get all registered remote servers, sorted by server ID.
    pub fn remotes(&self) -> Vec<(ServerId, String)> {
        self.remotes
            .read()
            .iter()
            .map(|(&a, b)| (a, b.clone()))
            .collect()
    }

    /// Update given remote server.
    pub fn update_remote(&self, id: ServerId, addr: String) -> bool {
        self.remotes.write().insert(id, addr).is_some()
    }

    /// Delete remote server by ID.
    pub fn delete_remote(&self, id: ServerId) -> bool {
        self.remotes.write().remove(&id).is_some()
    }

    /// Get remote server by ID.
    pub fn resolve_remote(&self, id: ServerId) -> Option<String> {
        self.remotes
            .read()
            .get(&id)
            .cloned()
            .or_else(|| self.remote_template.as_ref().map(|t| t.get(&id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_crud() {
        let resolver = Resolver::new(None);

        let id1 = ServerId::try_from(1).unwrap();
        let id2 = ServerId::try_from(2).unwrap();

        // no remotes
        assert_eq!(resolver.remotes().len(), 0);
        assert!(!resolver.delete_remote(id1));

        // add remotes
        assert!(!resolver.update_remote(id2, String::from("bar")));
        assert!(!resolver.update_remote(id1, String::from("foo")));
        let remotes = resolver.remotes();
        assert_eq!(remotes.len(), 2);
        assert_eq!(&remotes[0], &(id1, String::from("foo")));
        assert_eq!(&remotes[1], &(id2, String::from("bar")));

        // update remote
        assert!(resolver.update_remote(id1, String::from(":)")));
        let remotes = resolver.remotes();
        assert_eq!(remotes.len(), 2);
        assert_eq!(&remotes[0], &(id1, String::from(":)")));
        assert_eq!(&remotes[1], &(id2, String::from("bar")));

        // delete remotes
        assert!(resolver.delete_remote(id1));
        let remotes = resolver.remotes();
        assert_eq!(remotes.len(), 1);
        assert_eq!(&remotes[0], &(id2, String::from("bar")));
        assert!(!resolver.delete_remote(id1));
    }

    #[test]
    fn resolve_remote() {
        let resolver = Resolver::new(Some(RemoteTemplate::new("http://iox-query-{id}:8082")));
        resolver.update_remote(
            ServerId::try_from(1).unwrap(),
            String::from("http://iox-foo:1234"),
        );
        resolver.update_remote(
            ServerId::try_from(2).unwrap(),
            String::from("http://iox-bar:5678"),
        );

        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(1).unwrap()),
            Some(String::from("http://iox-foo:1234"))
        );
        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(2).unwrap()),
            Some(String::from("http://iox-bar:5678"))
        );
        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(42).unwrap()),
            Some(String::from("http://iox-query-42:8082"))
        );
        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(24).unwrap()),
            Some(String::from("http://iox-query-24:8082"))
        );
    }

    #[test]
    fn resolve_remote_without_template() {
        let resolver = Resolver::new(None);
        resolver.update_remote(
            ServerId::try_from(1).unwrap(),
            String::from("http://iox-foo:1234"),
        );

        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(1).unwrap()),
            Some(String::from("http://iox-foo:1234"))
        );
        assert_eq!(
            resolver.resolve_remote(ServerId::try_from(42).unwrap()),
            None,
        );
    }
}
