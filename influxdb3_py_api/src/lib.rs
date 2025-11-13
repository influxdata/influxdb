#[derive(Debug, thiserror::Error)]
pub enum ExecutePluginError {
    #[error(
        "the process_writes function is not present in the plugin. Should be defined as: process_writes(influxdb3_local, table_batches, args=None)"
    )]
    MissingProcessWritesFunction,

    #[error(
        "the process_request function is not present in the plugin. Should be defined as: process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None) -> Tuple[str, Optional[Dict[str, str]]]"
    )]
    MissingProcessRequestFunction,

    #[error(
        "the process_scheduled_call function is not present in the plugin. Should be defined as: process_scheduled_call(influxdb3_local, call_time, args=None)"
    )]
    MissingProcessScheduledCallFunction,

    #[error("{0}")]
    PluginError(#[from] anyhow::Error),
}

pub mod logging;
pub mod system_py;

#[cfg(test)]
mod tests {
    use crate::system_py::{self, CacheStore, PyCache};
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::DatabaseSchema;
    use influxdb3_id::{DbId, SerdeVecMap, TableId};
    use influxdb3_internal_api::query_executor::{QueryExecutor, UnimplementedQueryExecutor};
    use influxdb3_wal::{TableChunks, WriteBatch};
    use iox_time::{MockProvider, Time, TimeProvider};
    use parking_lot::Mutex;
    use pyo3::prepare_freethreaded_python;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn py_plugin_call_api_exposes_only_allowed_methods() {
        prepare_freethreaded_python();

        let db_name: Arc<str> = Arc::from("test_db");
        let schema = Arc::new(DatabaseSchema::new(DbId::from(0), Arc::clone(&db_name)));

        let write_batch = WriteBatch {
            catalog_sequence: 0,
            database_id: DbId::from(0),
            database_name: Arc::clone(&db_name),
            table_chunks: SerdeVecMap::<TableId, TableChunks>::new(),
            min_time_ns: 0,
            max_time_ns: 0,
        };

        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let cache_store = Arc::new(Mutex::new(CacheStore::new(
            Arc::clone(&time_provider),
            Duration::from_secs(60),
        )));
        let py_cache = PyCache::new_test_cache(cache_store, "method_visibility".to_string());

        let query_executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);

        let plugin = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    allowed = {"info", "warn", "error", "query", "write", "cache", "write_to_db"}
    attrs = {name for name in dir(influxdb3_local) if not name.startswith("__")}
    extras = attrs - allowed
    missing = allowed - attrs
    if extras or missing:
        raise RuntimeError(f"unexpected attributes: extras={sorted(extras)}, missing={sorted(missing)}")
"#;

        let result = system_py::execute_python_with_batch(
            plugin,
            &write_batch,
            schema,
            query_executor,
            None,
            None,
            &None::<HashMap<String, String>>,
            py_cache,
            None,
        );

        assert!(
            result.is_ok(),
            "PyPluginCallApi exposes unexpected Python methods: {result:?}"
        );
    }
}
