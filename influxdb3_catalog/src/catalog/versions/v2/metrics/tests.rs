use std::sync::Arc;

use crate::log::versions::v4::NodeMode;
use influxdb3_process::{ProcessUuidGetter, ProcessUuidWrapper};
use iox_time::{MockProvider, Time};
use metric::{Attributes, Metric, Registry, U64Counter};
use object_store::memory::InMemory;
use schema::{InfluxColumnType, InfluxFieldType};

use super::{
    super::{Catalog, Prompt},
    CATALOG_OPERATION_RETRIES_METRIC_NAME, CATALOG_OPERATIONS_METRIC_NAME,
};

#[tokio::test]
async fn test_catalog_retry_metrics() {
    let os = Arc::new(InMemory::new());
    let tp = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));

    // create two catalogs, both pointing at the same object store, but with their own metric
    // registry/instrument/observer

    // first:
    let m1 = Arc::new(Registry::new());
    let c1 = Catalog::new(
        "node",
        Arc::clone(&os) as _,
        Arc::clone(&tp) as _,
        Arc::clone(&m1),
    )
    .await
    .unwrap();
    let i1 = m1
        .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATION_RETRIES_METRIC_NAME)
        .unwrap();
    let o1 = i1.get_observer(&Attributes::from([])).unwrap();

    // second:
    let m2 = Arc::new(Registry::new());
    let c2 = Catalog::new(
        "node",
        Arc::clone(&os) as _,
        Arc::clone(&tp) as _,
        Arc::clone(&m2),
    )
    .await
    .unwrap();
    let i2 = m2
        .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATION_RETRIES_METRIC_NAME)
        .unwrap();
    let o2 = i2.get_observer(&Attributes::from([])).unwrap();

    // create db on 1, should not require any retries:
    assert_eq!(0, o1.fetch());
    c1.create_database("foo").await.unwrap();
    assert_eq!(0, o1.fetch());

    // create a different db on 2, this will require a retry since the catalog was updated
    // previously on object store, and will succeed...
    assert_eq!(0, o2.fetch());
    c2.create_database("bar").await.unwrap();
    assert_eq!(1, o2.fetch());

    // create the "bar" db on 1 now, this will require a retry since the catalog was updated on
    // object store by 2, but will fail...
    assert_eq!(0, o1.fetch());
    c1.create_database("bar").await.unwrap_err();
    assert_eq!(1, o1.fetch());
}

#[test_log::test(tokio::test)]
async fn test_catalog_operation_counter() {
    let metrics = Arc::new(Registry::new());
    let os = Arc::new(InMemory::new());
    let tp = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let catalog = Catalog::new(
        "node",
        Arc::clone(&os) as _,
        Arc::clone(&tp) as _,
        Arc::clone(&metrics),
    )
    .await
    .unwrap();
    let process_uuid_getter: Arc<dyn ProcessUuidGetter> = Arc::new(ProcessUuidWrapper::new());
    check_metric_empty(&metrics, "create_database");
    catalog.create_database("foo").await.unwrap();
    check_metric(&metrics, "create_database", 1);
    catalog.create_database("bar").await.unwrap();
    check_metric(&metrics, "create_database", 2);

    let mut txn = catalog.begin("baz").unwrap();
    txn.table_or_create("employees").unwrap();
    txn.column_or_create(
        "employees",
        "name",
        InfluxColumnType::Field(InfluxFieldType::String),
    )
    .unwrap();
    txn.column_or_create(
        "employees",
        "job_title",
        InfluxColumnType::Field(InfluxFieldType::String),
    )
    .unwrap();
    txn.column_or_create(
        "employees",
        "hire_date",
        InfluxColumnType::Field(InfluxFieldType::String),
    )
    .unwrap();
    check_metric_empty(&metrics, "create_table");
    check_metric_empty(&metrics, "add_columns");
    let Prompt::Success(_) = catalog.commit(txn).await.unwrap() else {
        panic!("transaction should commit");
    };
    check_metric(&metrics, "create_database", 3);
    check_metric(&metrics, "create_table", 1);
    check_metric(&metrics, "add_columns", 1); // AddColumns are accumulated into a single op

    check_metric_empty(&metrics, "register_node");
    catalog
        .register_node(
            "node2",
            4,
            vec![NodeMode::Core],
            Arc::clone(&process_uuid_getter),
            None,
        )
        .await
        .unwrap();
    check_metric(&metrics, "register_node", 1);

    check_metric_empty(&metrics, "create_admin_token");
    catalog.create_admin_token(false).await.unwrap();
    check_metric(&metrics, "create_admin_token", 1);
}

fn check_metric_empty(registry: &Arc<Registry>, operation_type: &'static str) {
    let instrument = registry
        .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATIONS_METRIC_NAME)
        .unwrap();
    assert!(
        instrument
            .get_observer(&Attributes::from(&[("type", operation_type)]))
            .is_none(),
        "the metric was present for the operation '{operation_type}'"
    );
}

fn check_metric(registry: &Arc<Registry>, operation_type: &'static str, expected: u64) {
    let instrument = registry
        .get_instrument::<Metric<U64Counter>>(CATALOG_OPERATIONS_METRIC_NAME)
        .unwrap();
    let observer = instrument
        .get_observer(&Attributes::from(&[("type", operation_type)]))
        .unwrap();
    assert_eq!(expected, observer.fetch(), "metric check failed");
}
