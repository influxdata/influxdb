use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use reqwest::Response;

use crate::server::{ConfigProvider, TestServer};

use super::TestConfig;

pub mod compactor;
mod file_index;
mod query;
mod replicas;

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfigEnterprise {
    core: TestConfig,
    cluster_id: Option<String>,
    replication_interval: Option<String>,
    mode: Option<Vec<BufferMode>>,
}

impl ConfigProvider for TestConfigEnterprise {
    fn as_args(&self) -> Vec<String> {
        let mut args = self.core.as_args();
        args.push("--cluster-id".to_string());
        if let Some(cluster_id) = &self.cluster_id {
            args.push(cluster_id.to_owned());
        } else {
            args.push("test-cluster".to_string());
        }
        if let Some(mode) = &self.mode {
            args.append(&mut vec![
                "--mode".to_string(),
                mode.iter()
                    .map(|bm| bm.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ]);
        }
        if let Some(replication_interval) = &self.replication_interval {
            args.append(&mut vec![
                "--replication-interval".to_string(),
                replication_interval.to_owned(),
            ])
        }
        args
    }

    fn auth_token(&self) -> Option<&str> {
        self.core.auth_token.as_ref().map(|(_, t)| t.as_str())
    }
}

impl TestConfigEnterprise {
    /// Set the auth token for this [`TestServer`]
    pub fn with_auth_token<S: Into<String>, R: Into<String>>(
        mut self,
        hashed_token: S,
        raw_token: R,
    ) -> Self {
        self.core = self.core.with_auth_token(hashed_token, raw_token);
        self
    }

    pub fn with_cluster_id<S: Into<String>>(mut self, cluster_id: S) -> Self {
        self.cluster_id = Some(cluster_id.into());
        self
    }

    /// Set a node identifier prefix on the spawned [`TestServer`]
    pub fn with_node_id<S: Into<String>>(mut self, node_id: S) -> Self {
        self.core = self.core.with_node_id(node_id);
        self
    }

    pub fn with_object_store<S: Into<String>>(mut self, path: S) -> Self {
        self.core = self.core.with_object_store_dir(path);
        self
    }

    /// Set the buffer mode for the spawned server
    pub fn with_mode(mut self, mode: Vec<BufferMode>) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Specify a replication interval in a "human time", e.g., "1ms", "10ms", etc.
    pub fn with_replication_interval<S: Into<String>>(mut self, interval: S) -> Self {
        self.replication_interval = Some(interval.into());
        self
    }
}

impl From<TestConfig> for TestConfigEnterprise {
    fn from(core: TestConfig) -> Self {
        Self {
            core,
            ..Default::default()
        }
    }
}

impl TestServer {
    pub fn configure_enterprise() -> TestConfigEnterprise {
        TestConfigEnterprise::default()
    }

    pub async fn api_v3_configure_file_index_create(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .post(format!(
                "{base}/api/v3/enterprise/configure/file_index",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to create file index")
    }

    pub async fn api_v3_configure_file_index_delete(
        &self,
        request: &serde_json::Value,
    ) -> Response {
        self.http_client
            .delete(format!(
                "{base}/api/v3/enterprise/configure/file_index",
                base = self.client_addr()
            ))
            .json(request)
            .send()
            .await
            .expect("failed to send request to delete file index")
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
