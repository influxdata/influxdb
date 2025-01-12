use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use reqwest::Response;

use crate::server::{ConfigProvider, TestServer};

pub mod compactor;
mod file_index;
mod query;
mod replicas;

/// Configuration for a [`TestServer`]
#[derive(Debug, Default)]
pub struct TestConfigEnterprise {
    auth_token: Option<(String, String)>,
    writer_id: Option<String>,
    read_from_writer_ids: Vec<String>,
    compact_from_writer_ids: Vec<String>,
    replication_interval: Option<String>,
    mode: Option<BufferMode>,
    object_store_path: Option<String>,
    compactor_id: Option<String>,
}

impl ConfigProvider for TestConfigEnterprise {
    fn as_args(&self) -> Vec<String> {
        let mut args = vec![];
        if let Some((token, _)) = &self.auth_token {
            args.append(&mut vec!["--bearer-token".to_string(), token.to_owned()]);
        }
        args.push("--writer-id".to_string());
        if let Some(writer_id) = &self.writer_id {
            args.push(writer_id.to_owned());
        } else {
            args.push("test-server".to_string());
        }
        if let Some(mode) = self.mode {
            args.append(&mut vec!["--mode".to_string(), mode.to_string()]);
        }
        if !self.read_from_writer_ids.is_empty() {
            args.append(&mut vec![
                "--read-from-writer-ids".to_string(),
                self.read_from_writer_ids.join(","),
            ])
        }
        if !self.compact_from_writer_ids.is_empty() {
            args.append(&mut vec![
                "--compact-from-writer-ids".to_string(),
                self.compact_from_writer_ids.join(","),
            ])
        }
        if let Some(compactor_id) = &self.compactor_id {
            args.append(&mut vec![
                "--compactor-id".to_string(),
                compactor_id.to_owned(),
            ]);
            args.append(&mut vec!["--run-compactions".to_string()]);
        }
        if let Some(path) = &self.object_store_path {
            args.append(&mut vec![
                "--object-store".to_string(),
                "file".to_string(),
                "--data-dir".to_string(),
                path.to_owned(),
            ])
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
        self.auth_token.as_ref().map(|(_, t)| t.as_str())
    }
}

impl TestConfigEnterprise {
    /// Set the auth token for this [`TestServer`]
    pub fn with_auth_token<S: Into<String>, R: Into<String>>(
        mut self,
        hashed_token: S,
        raw_token: R,
    ) -> Self {
        self.auth_token = Some((hashed_token.into(), raw_token.into()));
        self
    }

    /// Set a writer identifier prefix on the spawned [`TestServer`]
    pub fn with_writer_id<S: Into<String>>(mut self, writer_id: S) -> Self {
        self.writer_id = Some(writer_id.into());
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
    pub fn with_read_from_writer_ids(
        mut self,
        read_from_writer_ids: impl IntoIterator<Item: Into<String>>,
    ) -> Self {
        self.read_from_writer_ids
            .extend(read_from_writer_ids.into_iter().map(Into::into));
        self
    }

    /// Specify a replication interval in a "human time", e.g., "1ms", "10ms", etc.
    pub fn with_replication_interval<S: Into<String>>(mut self, interval: S) -> Self {
        self.replication_interval = Some(interval.into());
        self
    }

    pub fn with_compact_from_writer_ids(
        mut self,
        compact_from_writer_ids: impl IntoIterator<Item: Into<String>>,
    ) -> Self {
        self.compact_from_writer_ids
            .extend(compact_from_writer_ids.into_iter().map(Into::into));
        self
    }
}

impl TestServer {
    pub fn configure_pro() -> TestConfigEnterprise {
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
