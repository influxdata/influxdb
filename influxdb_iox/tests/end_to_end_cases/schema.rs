use crate::query_tests2::setups::SETUPS;
use data_types::ColumnType;
use futures::FutureExt;
use observability_deps::tracing::*;
use std::{collections::HashMap, sync::Arc};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
async fn list_all() {
    Arc::new(SchemaTest {
        setup_name: "TwoMeasurements",
        table_name: "cpu",
        expected_columns: HashMap::from([
            ("user".into(), ColumnType::F64),
            ("region".into(), ColumnType::Tag),
            ("time".into(), ColumnType::Time),
        ]),
    })
    .run()
    .await;
}

struct SchemaTest {
    setup_name: &'static str,
    table_name: &'static str,
    expected_columns: HashMap<String, ColumnType>,
}

impl SchemaTest {
    async fn run(self: Arc<Self>) {
        test_helpers::maybe_start_logging();
        let database_url = maybe_skip_integration!();
        let setup_name = self.setup_name;

        info!("Using setup {setup_name}");

        // Set up the cluster  ====================================
        let mut cluster = MiniCluster::create_shared2_never_persist(database_url.clone()).await;

        let setup_steps = SETUPS
            .get(setup_name)
            .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
            .iter();

        let cloned_self = Arc::clone(&self);

        let test_step = Step::Custom(Box::new(move |state: &mut StepTestState| {
            let cloned_self = Arc::clone(&cloned_self);
            async move {
                let mut client = influxdb_iox_client::schema::Client::new(
                    state.cluster().querier().querier_grpc_connection(),
                );

                let response = client
                    .get_schema(state.cluster().namespace())
                    .await
                    .expect("successful response");

                let table = response
                    .tables
                    .get(cloned_self.table_name)
                    .expect("table not found");

                let columns: HashMap<_, _> = table
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), v.column_type().try_into().unwrap()))
                    .collect();

                assert_eq!(cloned_self.expected_columns, columns);
            }
            .boxed()
        }));

        StepTest::new(&mut cluster, setup_steps.chain(std::iter::once(&test_step)))
            .run()
            .await;
    }
}
