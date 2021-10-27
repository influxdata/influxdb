use crate::influxdata::iox::management::v1 as management;
use data_types::detailed_database::ActiveDatabase;

impl From<ActiveDatabase> for management::DetailedDatabase {
    fn from(database: ActiveDatabase) -> Self {
        let ActiveDatabase { name, uuid } = database;

        Self {
            db_name: name.to_string(),
            uuid: uuid.as_bytes().to_vec(),
        }
    }
}
