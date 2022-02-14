use generated_types::influxdata::iox::management;

use async_trait::async_trait;
use data_types::server_id::ServerId;
use generated_types::google::FieldViolation;
use generated_types::influxdata::iox::management::v1::OwnerInfo;
use server::{
    config::{ConfigProvider, Result as ConfigResult},
    rules::ProvidedDatabaseRules,
};
use snafu::{OptionExt, ResultExt, Snafu};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("error fetching server config from file: {}", source))]
    FetchBytes { source: std::io::Error },

    #[snafu(display("error decoding server config from file: {}", source))]
    Decode { source: serde_json::Error },

    #[snafu(display("invalid database config: {}", source))]
    InvalidDatabaseConfig { source: FieldViolation },

    #[snafu(display("database with UUID {} not found in config file", uuid))]
    DatabaseNotFound { uuid: Uuid },

    #[snafu(display("database rules \"{}\" not found in config file", rules))]
    DatabaseRulesNotFound { rules: String },

    #[snafu(display("invalid UUID in server config file: {}", source))]
    InvalidUUID { source: uuid::Error },

    #[snafu(display("config is immutable"))]
    ImmutableConfig,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

fn parse_uuid(uuid: &str) -> Result<Uuid> {
    std::str::FromStr::from_str(uuid).context(InvalidUUIDSnafu)
}

/// A loader for [`ServerConfigFile`]
#[derive(Debug)]
pub struct ServerConfigFile {
    path: String,
}

impl ServerConfigFile {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    async fn load(&self) -> Result<management::v1::ServerConfigFile> {
        let bytes = tokio::fs::read(&self.path).await.context(FetchBytesSnafu)?;
        serde_json::from_slice(bytes.as_slice()).context(DecodeSnafu)
    }
}

#[async_trait]
impl ConfigProvider for ServerConfigFile {
    async fn fetch_server_config(&self, _server_id: ServerId) -> ConfigResult<Vec<(String, Uuid)>> {
        let config = self.load().await?;

        config
            .databases
            .into_iter()
            .map(|config| Ok((config.name, parse_uuid(&config.uuid)?)))
            .collect()
    }

    async fn store_server_config(
        &self,
        _server_id: ServerId,
        _config: &[(String, Uuid)],
    ) -> ConfigResult<()> {
        Err(Error::ImmutableConfig.into())
    }

    async fn fetch_rules(&self, uuid: Uuid) -> ConfigResult<ProvidedDatabaseRules> {
        // We load the file each time to pick up changes
        let server_config = self.load().await?;
        let uuid_str = uuid.to_string();

        // Lookup the database name and rules based on UUID
        let config = server_config
            .databases
            .into_iter()
            .find(|config| config.uuid == uuid_str)
            .context(DatabaseNotFoundSnafu { uuid })?;

        // Lookup the rules for this database
        let rules = server_config
            .rules
            .into_iter()
            .find(|r| r.name == config.rules)
            .context(DatabaseRulesNotFoundSnafu {
                rules: config.rules,
            })?;

        // Parse rules into [`ProvidedDatabaseRules`]
        let rules = ProvidedDatabaseRules::new_rules(management::v1::DatabaseRules {
            name: config.name,
            ..rules
        })
        .context(InvalidDatabaseConfigSnafu)?;

        Ok(rules)
    }

    async fn store_rules(&self, _uuid: Uuid, _rules: &ProvidedDatabaseRules) -> ConfigResult<()> {
        Err(Error::ImmutableConfig.into())
    }

    async fn fetch_owner_info(&self, server_id: ServerId, _uuid: Uuid) -> ConfigResult<OwnerInfo> {
        Ok(OwnerInfo {
            id: server_id.get_u32(),
            location: "NONE".to_string(),
            transactions: vec![],
        })
    }

    async fn update_owner_info(
        &self,
        _server_id: Option<ServerId>,
        _uuid: Uuid,
    ) -> ConfigResult<()> {
        Err(Error::ImmutableConfig.into())
    }

    async fn create_owner_info(&self, _server_id: ServerId, _uuid: Uuid) -> ConfigResult<()> {
        Err(Error::ImmutableConfig.into())
    }
}
