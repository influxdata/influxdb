use std::collections::HashMap;
use std::fmt::Debug;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use schema::INFLUXQL_METADATA_KEY;

/// Trait for handling the `SHOW DATABASES` query
///
/// This allows for optional `SHOW DATABASES` handling for systems such as
/// InfluxDB3 Enterprise, without requiring it to be implemented on all systems
#[async_trait::async_trait]
pub trait InfluxQlShowDatabases: Debug + Send + Sync + 'static {
    /// Produce the Arrow schema for the `SHOW DATABASES` InfluxQL query
    fn schema(&self) -> SchemaRef;
    /// Produce a record batch stream containing the results for the `SHOW DATABASES` query
    ///
    /// Accepts `database_names` which represents the list of databases the requestor is
    /// authorized to read. The underlying implementation should only produce the databases listed
    /// in the resulting record batch stream.
    async fn show_databases(
        &self,
        database_names: Vec<String>,
    ) -> Result<SendableRecordBatchStream>;
}

/// Generate the default InfluxQL metadata map for producing a `Schema` for the `SHOW DATABASES`
/// query.
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
    use std::sync::Arc;

    use arrow::{
        array::{Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_util::MemoryStream;
    use schema::INFLUXQL_MEASUREMENT_COLUMN_NAME;

    use super::*;

    #[derive(Debug)]
    pub struct MockShowDatabases {
        database_names: Vec<String>,
    }

    impl MockShowDatabases {
        pub fn new(database_names: impl IntoIterator<Item: Into<String>>) -> Self {
            Self {
                database_names: database_names.into_iter().map(Into::into).collect(),
            }
        }
    }

    #[async_trait::async_trait]
    impl InfluxQlShowDatabases for MockShowDatabases {
        fn schema(&self) -> SchemaRef {
            Arc::new(
                Schema::new(vec![
                    Field::new(INFLUXQL_MEASUREMENT_COLUMN_NAME, DataType::Utf8, false),
                    Field::new("name", arrow::datatypes::DataType::Utf8, false),
                ])
                .with_metadata(generate_metadata(0)),
            )
        }

        async fn show_databases(
            &self,
            database_names: Vec<String>,
        ) -> Result<SendableRecordBatchStream> {
            let names = self
                .database_names
                .iter()
                .filter(|n| database_names.contains(*n))
                .map(String::as_str)
                .collect::<Vec<_>>();
            let measurement_array: StringArray = vec!["databases"; names.len()].into();
            let names_array: StringArray = names.into();
            let arrays = vec![
                Arc::new(measurement_array) as Arc<dyn Array>,
                Arc::new(names_array) as Arc<dyn Array>,
            ];
            let batch = RecordBatch::try_new(self.schema(), arrays)?;
            Ok(Box::pin(MemoryStream::new(vec![batch])))
        }
    }
}
