use super::Result as ConfigResult;
use crate::{
    config::{
        owner::{
            create_owner_info, fetch_owner_info, update_owner_info, OwnerInfoCreateError,
            OwnerInfoFetchError, OwnerInfoUpdateError,
        },
        ConfigProvider,
    },
    PersistedDatabaseRules, ProvidedDatabaseRules,
};
use async_trait::async_trait;
use data_types::server_id::ServerId;
use generated_types::database_rules::encode_persisted_database_rules;
use generated_types::google::FieldViolation;
use generated_types::influxdata::iox::management;
use generated_types::influxdata::iox::management::v1::OwnerInfo;
use iox_object_store::IoxObjectStore;
use object_store::ObjectStore;
use snafu::{ensure, ResultExt, Snafu};
use std::sync::Arc;
use time::TimeProvider;
use uuid::Uuid;

/// Error enumeration for [`ConfigProviderObjectStorage`]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("error saving server config to object storage: {}", source))]
    StoreServer { source: object_store::Error },

    #[snafu(display("error getting server config from object storage: {}", source))]
    FetchServer { source: object_store::Error },

    #[snafu(display("error deserializing server config: {}", source))]
    DeserializeServer {
        source: generated_types::DecodeError,
    },

    #[snafu(display("error serializing server config: {}", source))]
    SerializeServer {
        source: generated_types::EncodeError,
    },

    #[snafu(display(
        "UUID mismatch reading server config from object storage, expected {}, got {}",
        expected,
        actual
    ))]
    UuidMismatch { expected: Uuid, actual: Uuid },

    #[snafu(display(
        "invalid database uuid in server config while finding location: {}",
        source
    ))]
    InvalidDatabaseLocation { source: uuid::Error },

    #[snafu(display("Error saving rules for {}: {}", db_name, source))]
    StoreRules {
        db_name: String,
        source: object_store::Error,
    },

    #[snafu(display("error getting database rules from object storage: {}", source))]
    RulesFetch { source: object_store::Error },

    #[snafu(display("error deserializing database rules: {}", source))]
    DeserializeRules {
        source: generated_types::DecodeError,
    },

    #[snafu(display("error serializing database rules: {}", source))]
    SerializeRules {
        source: generated_types::EncodeError,
    },

    #[snafu(display("error converting to database rules: {}", source))]
    ConvertingRules { source: FieldViolation },

    #[snafu(display("error creating database owner info: {}", source))]
    CreatingOwnerInfo { source: OwnerInfoCreateError },

    #[snafu(display("error getting database owner info: {}", source))]
    FetchingOwnerInfo { source: OwnerInfoFetchError },

    #[snafu(display("error updating database owner info: {}", source))]
    UpdatingOwnerInfo { source: OwnerInfoUpdateError },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Parse the UUID from an object storage path
///
/// TODO: Encode this data directly in server config
fn parse_location(location: &str) -> Result<Uuid> {
    // Strip trailing / if any
    let location = location.strip_suffix('/').unwrap_or(location);
    let uuid = location.rsplit('/').next().unwrap();

    std::str::FromStr::from_str(uuid).context(InvalidDatabaseLocationSnafu)
}

#[derive(Debug)]
pub struct ConfigProviderObjectStorage {
    object_store: Arc<ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,
}

impl ConfigProviderObjectStorage {
    pub fn new(object_store: Arc<ObjectStore>, time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            object_store,
            time_provider,
        }
    }

    fn iox_object_store(&self, uuid: Uuid) -> IoxObjectStore {
        let root_path = IoxObjectStore::root_path_for(&self.object_store, uuid);
        IoxObjectStore::existing(Arc::clone(&self.object_store), root_path)
    }
}

#[async_trait]
impl ConfigProvider for ConfigProviderObjectStorage {
    async fn fetch_server_config(&self, server_id: ServerId) -> ConfigResult<Vec<(String, Uuid)>> {
        let fetch_result =
            IoxObjectStore::get_server_config_file(&self.object_store, server_id).await;

        let server_config_bytes = match fetch_result {
            Ok(bytes) => bytes,
            // If this is the first time starting up this server and there is no config file yet,
            // this isn't a problem. Start an empty server config.
            Err(object_store::Error::NotFound { .. }) => bytes::Bytes::new(),
            Err(source) => return Err(Error::FetchServer { source }.into()),
        };

        let server_config =
            generated_types::server_config::decode_persisted_server_config(server_config_bytes)
                .context(DeserializeServerSnafu)?;

        let config = server_config
            .databases
            .into_iter()
            .map(|(name, location)| Ok((name, parse_location(&location)?)))
            .collect::<Result<Vec<_>>>()?;

        self.store_server_config(server_id, &config).await?;
        Ok(config)
    }

    async fn store_server_config(
        &self,
        server_id: ServerId,
        config: &[(String, Uuid)],
    ) -> ConfigResult<()> {
        let databases = config
            .iter()
            .map(|(name, database)| {
                (
                    name.to_string(),
                    IoxObjectStore::root_path_for(&self.object_store, *database).to_string(),
                )
            })
            .collect();

        let data = management::v1::ServerConfig { databases };

        let mut encoded = bytes::BytesMut::new();
        generated_types::server_config::encode_persisted_server_config(&data, &mut encoded)
            .context(SerializeServerSnafu)?;

        let bytes = encoded.freeze();

        IoxObjectStore::put_server_config_file(&self.object_store, server_id, bytes)
            .await
            .context(StoreServerSnafu)?;

        Ok(())
    }

    async fn fetch_rules(&self, uuid: Uuid) -> ConfigResult<ProvidedDatabaseRules> {
        let bytes = IoxObjectStore::load_database_rules(Arc::clone(&self.object_store), uuid)
            .await
            .context(RulesFetchSnafu)?;

        let proto: management::v1::PersistedDatabaseRules =
            generated_types::database_rules::decode_persisted_database_rules(bytes)
                .context(DeserializeRulesSnafu)?;

        let rules: PersistedDatabaseRules = proto.try_into().context(ConvertingRulesSnafu)?;

        ensure!(
            uuid == rules.uuid(),
            UuidMismatchSnafu {
                expected: uuid,
                actual: rules.uuid()
            }
        );

        Ok(rules.into_inner().1)
    }

    async fn store_rules(&self, uuid: Uuid, rules: &ProvidedDatabaseRules) -> ConfigResult<()> {
        let persisted_database_rules = management::v1::PersistedDatabaseRules {
            uuid: uuid.as_bytes().to_vec(),
            // Note we save the original version
            rules: Some(rules.original().clone()),
        };

        let mut data = bytes::BytesMut::new();
        encode_persisted_database_rules(&persisted_database_rules, &mut data)
            .context(SerializeRulesSnafu)?;

        self.iox_object_store(uuid)
            .put_database_rules_file(data.freeze())
            .await
            .context(StoreRulesSnafu {
                db_name: rules.db_name(),
            })?;

        Ok(())
    }

    async fn fetch_owner_info(&self, _server_id: ServerId, uuid: Uuid) -> ConfigResult<OwnerInfo> {
        let config = fetch_owner_info(&self.iox_object_store(uuid))
            .await
            .context(FetchingOwnerInfoSnafu)?;

        Ok(config)
    }

    async fn update_owner_info(&self, server_id: Option<ServerId>, uuid: Uuid) -> ConfigResult<()> {
        let path = server_id.map(|server_id| {
            IoxObjectStore::server_config_path(&self.object_store, server_id).to_string()
        });

        update_owner_info(
            server_id,
            path,
            self.time_provider.now(),
            &self.iox_object_store(uuid),
        )
        .await
        .context(UpdatingOwnerInfoSnafu)?;

        Ok(())
    }

    async fn create_owner_info(&self, server_id: ServerId, uuid: Uuid) -> ConfigResult<()> {
        let path = IoxObjectStore::server_config_path(&self.object_store, server_id).to_string();
        create_owner_info(server_id, path, &self.iox_object_store(uuid))
            .await
            .context(CreatingOwnerInfoSnafu)?;

        Ok(())
    }
}
