use crate::influxdata::iox::management::v1 as management;
use data_types::deleted_database::DeletedDatabase;

impl From<DeletedDatabase> for management::DeletedDatabase {
    fn from(deleted: DeletedDatabase) -> Self {
        let DeletedDatabase {
            name,
            generation_id,
            deleted_at,
        } = deleted;

        Self {
            db_name: name.to_string(),
            generation_id: generation_id.inner as u64,
            deleted_at: Some(deleted_at.into()),
        }
    }
}
