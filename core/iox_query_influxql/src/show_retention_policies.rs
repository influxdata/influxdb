use std::collections::HashMap;
use std::fmt::Debug;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use schema::INFLUXQL_METADATA_KEY;

/// Trait for handling the `SHOW RETENTION POLICIES` query
///
/// This allows for optional `SHOW RETENTION POLICIES` handling for systems such as
/// InfluxDB3 Enterprise, without requiring it to be implemented on all systems
#[async_trait::async_trait]
pub trait InfluxQlShowRetentionPolicies: Debug + Send + Sync + 'static {
    /// Produce the Arrow schema for the `SHOW RETENTION POLICIES` InfluxQL query
    fn schema(&self) -> SchemaRef;
    /// Produce a record batch stream containing the results for the `SHOW RETENTION POLICIES` query
    async fn show_retention_policies(&self, db_name: String) -> Result<SendableRecordBatchStream>;
}

/// Generate the default InfluxQL metadata map for producing a `Schema` for the
/// `SHOW RETENTION POLICIES` query.
pub fn generate_metadata(measurement_column_index: u32) -> HashMap<String, String> {
    let md = serde_json::to_string(&InfluxQlMetadata {
        measurement_column_index,
        tag_key_columns: vec![],
    })
    .expect("metadata should serialize as JSON");
    [(INFLUXQL_METADATA_KEY.to_string(), md)]
        .into_iter()
        .collect()
}

pub mod mock {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use arrow::{
        array::{Array, BooleanArray, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_util::MemoryStream;
    use schema::INFLUXQL_MEASUREMENT_COLUMN_NAME;

    use super::*;

    #[derive(Debug)]
    pub struct MockRetentionPolicy {
        name: String,
        duration: Duration,
        shard_group_duration: Duration,
        replica_n: u64,
        future_write_limit: Duration,
        past_write_limit: Duration,
        default: bool,
    }

    impl Default for MockRetentionPolicy {
        fn default() -> Self {
            Self {
                name: "autogen".to_string(),
                duration: Duration::ZERO,
                shard_group_duration: Duration::from_secs(7 * 60 * 60 * 24), // default is 7 days
                replica_n: 1,
                future_write_limit: Duration::ZERO,
                past_write_limit: Duration::ZERO,
                default: true,
            }
        }
    }

    impl MockRetentionPolicy {
        /// Create a named policy that is not the default policy
        pub fn new(name: impl Into<String>) -> Self {
            Self::default().with_name(name).with_default(false)
        }

        fn with_name(mut self, name: impl Into<String>) -> Self {
            self.name = name.into();
            self
        }

        fn with_default(mut self, default: bool) -> Self {
            self.default = default;
            self
        }

        pub fn with_duration(mut self, duration: Duration) -> Self {
            self.duration = duration;
            self
        }
    }

    #[derive(Debug, Default)]
    pub struct MockShowRetentionPolicies {
        retention_policies: BTreeMap<String, Vec<MockRetentionPolicy>>,
    }

    impl MockShowRetentionPolicies {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_default_retention_policy(mut self, db_name: impl Into<String>) -> Self {
            self.retention_policies
                .entry(db_name.into())
                .or_insert_with(|| vec![MockRetentionPolicy::default()]);
            self
        }

        pub fn with_retention_policy(
            mut self,
            db_name: impl Into<String>,
            policy: MockRetentionPolicy,
        ) -> Self {
            self.retention_policies
                .entry(db_name.into())
                .or_default()
                .push(policy);
            self
        }
    }

    /// The implementation of this follows that of InfluxDB v1.12.2's /query API with respect to
    /// the field names provided in the `SHOW RETENTION POLICIES` response schema. As such, this
    /// would be a good reference point for implementing this interface in production.
    ///
    /// One distinction with the v1.12.2 response is that durations are reported there using a more
    /// human-friendly format. For example, 1 hour and 30 minutes would be displayed as "1h30m",
    /// whereas in this implementation, which uses `std::time::Duration`'s pretty formatting, the
    /// same would be displayed as "5400s".
    #[async_trait::async_trait]
    impl InfluxQlShowRetentionPolicies for MockShowRetentionPolicies {
        fn schema(&self) -> SchemaRef {
            Arc::new(
                Schema::new(vec![
                    Field::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
                    Field::new("name", arrow::datatypes::DataType::Utf8, false),
                    Field::new("duration", arrow::datatypes::DataType::Utf8, false),
                    Field::new(
                        "shardGroupDuration",
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),
                    Field::new("replicaN", arrow::datatypes::DataType::UInt64, false),
                    Field::new("futureWriteLimit", arrow::datatypes::DataType::Utf8, false),
                    Field::new("pastWriteLimit", arrow::datatypes::DataType::Utf8, false),
                    Field::new("default", arrow::datatypes::DataType::Boolean, false),
                ])
                .with_metadata(generate_metadata(0)),
            )
        }

        async fn show_retention_policies(
            &self,
            db_name: String,
        ) -> Result<SendableRecordBatchStream> {
            let Some(db) = self.retention_policies.get(&db_name) else {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "database not found: {db_name}"
                )));
            };
            let measurement_array: StringArray = vec!["retention_policies"; db.len()].into();
            let names_array: StringArray = db
                .iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>()
                .into();
            let durations_array: StringArray = db
                .iter()
                .map(|MockRetentionPolicy { duration, .. }| format!("{duration:#?}"))
                .collect::<Vec<_>>()
                .into();
            let shard_group_durations_array: StringArray = db
                .iter()
                .map(
                    |MockRetentionPolicy {
                         shard_group_duration,
                         ..
                     }| format!("{shard_group_duration:#?}"),
                )
                .collect::<Vec<_>>()
                .into();
            let replica_n_array: UInt64Array = db
                .iter()
                .map(|MockRetentionPolicy { replica_n, .. }| *replica_n)
                .collect::<Vec<_>>()
                .into();
            let future_write_limit_array: StringArray = db
                .iter()
                .map(
                    |MockRetentionPolicy {
                         future_write_limit, ..
                     }| format!("{future_write_limit:#?}"),
                )
                .collect::<Vec<_>>()
                .into();
            let past_write_limit_array: StringArray = db
                .iter()
                .map(
                    |MockRetentionPolicy {
                         past_write_limit, ..
                     }| format!("{past_write_limit:#?}"),
                )
                .collect::<Vec<_>>()
                .into();
            let default_array: BooleanArray = db
                .iter()
                .map(|MockRetentionPolicy { default, .. }| *default)
                .collect::<Vec<_>>()
                .into();

            let arrays = vec![
                Arc::new(measurement_array) as Arc<dyn Array>,
                Arc::new(names_array) as Arc<dyn Array>,
                Arc::new(durations_array) as Arc<dyn Array>,
                Arc::new(shard_group_durations_array) as Arc<dyn Array>,
                Arc::new(replica_n_array) as Arc<dyn Array>,
                Arc::new(future_write_limit_array) as Arc<dyn Array>,
                Arc::new(past_write_limit_array) as Arc<dyn Array>,
                Arc::new(default_array) as Arc<dyn Array>,
            ];
            let batch = RecordBatch::try_new(self.schema(), arrays)?;
            Ok(Box::pin(MemoryStream::new(vec![batch])))
        }
    }
}
