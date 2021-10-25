use crate::influxdata::iox::management::v1 as management;
use data_types::detailed_database::DetailedDatabase;

impl From<DetailedDatabase> for management::DetailedDatabase {
    fn from(database: DetailedDatabase) -> Self {
        let DetailedDatabase { name, deleted_at } = database;

        Self {
            db_name: name.to_string(),
            deleted_at: deleted_at.map(Into::into),
        }
    }
}
