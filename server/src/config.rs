/// This module contains code for managing the configuration of the server.
use crate::{db::Db, Error, Result};
use data_types::{
    database_rules::{DatabaseRules, HostGroup, HostGroupId},
    DatabaseName,
};
use mutable_buffer::MutableBufferDb;
use object_store::path::ObjectStorePath;
use read_buffer::Database as ReadBufferDb;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

pub(crate) const DB_RULES_FILE_NAME: &str = "rules.json";

/// The Config tracks the configuration od databases and their rules along
/// with host groups for replication. It is used as an in-memory structure
/// that can be loaded incrementally from objet storage.
#[derive(Default, Debug)]
pub(crate) struct Config {
    state: RwLock<ConfigState>,
}

impl Config {
    pub(crate) fn create_db(
        &self,
        name: DatabaseName<'static>,
        rules: DatabaseRules,
    ) -> Result<CreateDatabaseHandle<'_>> {
        let mut state = self.state.write().expect("mutex poisoned");
        if state.reservations.contains(&name) || state.databases.contains_key(&name) {
            return Err(Error::DatabaseAlreadyExists {
                db_name: name.to_string(),
            });
        }

        let mutable_buffer = if rules.store_locally {
            Some(MutableBufferDb::new(name.to_string()))
        } else {
            None
        };

        let read_buffer = ReadBufferDb::new();

        let wal_buffer = rules.wal_buffer_config.as_ref().map(Into::into);
        let db = Arc::new(Db::new(rules, mutable_buffer, read_buffer, wal_buffer));

        state.reservations.insert(name.clone());
        Ok(CreateDatabaseHandle {
            db,
            config: &self,
            name,
        })
    }

    pub(crate) fn db(&self, name: &DatabaseName<'_>) -> Option<Arc<Db>> {
        let state = self.state.read().expect("mutex poisoned");
        state.databases.get(name).cloned()
    }

    pub(crate) fn create_host_group(&self, host_group: HostGroup) {
        let mut state = self.state.write().expect("mutex poisoned");
        state
            .host_groups
            .insert(host_group.id.clone(), Arc::new(host_group));
    }

    pub(crate) fn host_group(&self, host_group_id: &str) -> Option<Arc<HostGroup>> {
        let state = self.state.read().expect("mutex poinsoned");
        state.host_groups.get(host_group_id).cloned()
    }

    fn commit(&self, name: &DatabaseName<'static>, db: Arc<Db>) {
        let mut state = self.state.write().expect("mutex poisoned");
        let name = state
            .reservations
            .take(name)
            .expect("reservation doesn't exist");
        assert!(state.databases.insert(name, db).is_none())
    }

    fn rollback(&self, name: &DatabaseName<'static>) {
        let mut state = self.state.write().expect("mutex poisoned");
        state.reservations.remove(name);
    }
}

pub fn object_store_path_for_database_config(
    root: &ObjectStorePath,
    name: &DatabaseName<'_>,
) -> ObjectStorePath {
    let mut path = root.clone();
    path.push_dir(name.to_string());
    path.set_file_name(DB_RULES_FILE_NAME);
    path
}

#[derive(Default, Debug)]
struct ConfigState {
    reservations: BTreeSet<DatabaseName<'static>>,
    databases: BTreeMap<DatabaseName<'static>, Arc<Db>>,
    host_groups: BTreeMap<HostGroupId, Arc<HostGroup>>,
}

/// CreateDatabaseHandle is retunred when a call is made to `create_db` on
/// the Config struct. The handle can be used to hold a reservation for the
/// database name. Calling `commit` on the handle will consume the struct
/// and move the database from reserved to being in the config.
///
/// The goal is to ensure that database names can be reserved with
/// minimal time holding a write lock on the config state. This allows
/// the caller (the server) to reserve the database name, persist its
/// configuration and then commit the change in-memory after it has been
/// persisted.
#[derive(Debug)]
pub(crate) struct CreateDatabaseHandle<'a> {
    pub db: Arc<Db>,
    pub name: DatabaseName<'static>,
    config: &'a Config,
}

impl<'a> CreateDatabaseHandle<'a> {
    pub(crate) fn commit(self) {
        self.config.commit(&self.name, self.db.clone())
    }
}

impl<'a> Drop for CreateDatabaseHandle<'a> {
    fn drop(&mut self) {
        self.config.rollback(&self.name);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use object_store::path::cloud::CloudConverter;

    #[test]
    fn create_db() {
        let name = DatabaseName::new("foo").unwrap();
        let config = Config::default();
        let rules = DatabaseRules::default();

        {
            let _db_reservation = config.create_db(name.clone(), rules.clone()).unwrap();
            let err = config.create_db(name.clone(), rules.clone()).unwrap_err();
            assert!(matches!(err, Error::DatabaseAlreadyExists{ .. }));
        }

        let db_reservation = config.create_db(name.clone(), rules).unwrap();
        db_reservation.commit();
        assert!(config.db(&name).is_some());
    }

    #[test]
    fn object_store_path_for_database_config() {
        let path = ObjectStorePath::from_cloud_unchecked("1");
        let name = DatabaseName::new("foo").unwrap();
        let rules_path = super::object_store_path_for_database_config(&path, &name);
        let rules_path = CloudConverter::convert(&rules_path);

        assert_eq!(rules_path, "1/foo/rules.json");
    }
}
