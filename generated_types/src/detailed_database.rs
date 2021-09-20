use crate::influxdata::iox::management::v1 as management;
use data_types::detailed_database::DetailedDatabase;

impl From<DetailedDatabase> for management::DetailedDatabase {
    fn from(database: DetailedDatabase) -> Self {
        let DetailedDatabase {
            name,
            generation_id,
            deleted_at,
        } = database;

        Self {
            db_name: name.to_string(),
            generation_id: generation_id.inner as u64,
            deleted_at: deleted_at.map(Into::into),
        }
    }
}
