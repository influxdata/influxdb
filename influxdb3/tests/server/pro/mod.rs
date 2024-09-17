use influxdb3_pro_clap_blocks::serve::BufferMode;

use crate::{ConfigProvider, TestServer};

mod replicas;

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfigPro {
    auth_token: Option<(String, String)>,
    host_id: Option<String>,
    replicas: Vec<String>,
    replication_interval: Option<String>,
    mode: Option<BufferMode>,
    object_store_path: Option<String>,
    compactor_id: Option<String>,
}

impl ConfigProvider for TestConfigPro {
    fn as_args(&self) -> Vec<String> {
        let mut args = vec![];
        if let Some((token, _)) = &self.auth_token {
            args.append(&mut vec!["--bearer-token".to_string(), token.to_owned()]);
        }
        args.push("--host-id".to_string());
        if let Some(host) = &self.host_id {
            args.push(host.to_owned());
        } else {
            args.push("test-server".to_string());
        }
        if let Some(mode) = self.mode {
            args.append(&mut vec!["--mode".to_string(), mode.to_string()]);
        }
        if !self.replicas.is_empty() {
            args.append(&mut vec!["--replicas".to_string(), self.replicas.join(",")])
        }
        if let Some(compactor_id) = &self.compactor_id {
            args.append(&mut vec![
                "--compactor-id".to_string(),
                compactor_id.to_owned(),
            ])
        }
        if let Some(path) = &self.object_store_path {
            args.append(&mut vec![
                "--object-store".to_string(),
                "file".to_string(),
                "--data-dir".to_string(),
                path.to_owned(),
            ])
        }
        args
    }

    fn auth_token(&self) -> Option<&str> {
        self.auth_token.as_ref().map(|(_, t)| t.as_str())
    }
}

impl TestConfigPro {
    /// Set the auth token for this [`TestServer`]
    pub fn with_auth_token<S: Into<String>, R: Into<String>>(
        mut self,
        hashed_token: S,
        raw_token: R,
    ) -> Self {
        self.auth_token = Some((hashed_token.into(), raw_token.into()));
        self
    }

    /// Set a host identifier prefix on the spawned [`TestServer`]
    pub fn with_host_id<S: Into<String>>(mut self, host_id: S) -> Self {
        self.host_id = Some(host_id.into());
        self
    }

    /// Set the compactor id for the spawned server
    pub fn with_compactor_id<S: Into<String>>(mut self, compactor_id: S) -> Self {
        self.compactor_id = Some(compactor_id.into());
        self
    }

    pub fn with_object_store<S: Into<String>>(mut self, path: S) -> Self {
        self.object_store_path = Some(path.into());
        self
    }

    /// Set the buffer mode for the spawned server
    pub fn with_mode(mut self, mode: BufferMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Give a set of host identifier prefixes to be replicated by this server
    pub fn with_replicas(mut self, replicas: impl IntoIterator<Item: Into<String>>) -> Self {
        self.replicas.extend(replicas.into_iter().map(Into::into));
        self
    }

    /// Specify a replication interval in a "human time", e.g., "1ms", "10ms", etc.
    pub fn with_replication_interval<S: Into<String>>(mut self, interval: S) -> Self {
        self.replication_interval = Some(interval.into());
        self
    }
}

impl TestServer {
    pub fn configure_pro() -> TestConfigPro {
        TestConfigPro::default()
    }
}

/// Get a temporary directory path as a string
pub fn tmp_dir() -> String {
    test_helpers::tmp_dir()
        .expect("unable to get a temporary directory")
        .path()
        .to_str()
        .expect("could not convert tmp dir path to a string")
        .to_owned()
}
