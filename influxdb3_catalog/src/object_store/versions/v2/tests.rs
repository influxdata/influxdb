use std::sync::Arc;

use object_store::local::LocalFileSystem;

use super::ObjectStoreCatalog;

#[test_log::test(tokio::test)]
async fn load_or_create_catalog_new_catalog() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let store = ObjectStoreCatalog::new("test_host", 10, Arc::new(local_disk));
    let _ = store.load_or_create_catalog().await.unwrap();
    assert!(store.load_catalog().await.unwrap().is_some());
}

#[test_log::test(tokio::test)]
async fn load_or_create_catalog_existing_catalog() {
    let local_disk = LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    let store = ObjectStoreCatalog::new("test_host", 10, Arc::new(local_disk));
    let expected = store.load_or_create_catalog().await.unwrap().catalog_id;

    // initialize again to ensure it uses the same as above
    let actual = store.load_or_create_catalog().await.unwrap().catalog_id;

    // check the instance ids match
    assert_eq!(expected, actual);
}

// TODO: more test cases
// - check that only files until most recent snapshot are laoded on start
// - verify all catalog ops can be serialized and deserialized

// NOTE(trevor/catalog-refactor): the below three tests are all from the persister module in
// influxdb3_write, which I am putting here to preserve, but will need to all be replaced/refactored.

// NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
// This test also doesn't really do anything other than check via unwrap that things dont fail
#[tokio::test]
#[ignore = "this used the old persist_catalog method, so needs to be refactored"]
async fn persist_catalog() {
    unimplemented!();
    // let node_id = Arc::from("sample-host-id");
    // let instance_id = Arc::from("sample-instance-id");
    // let local_disk =
    //     LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    // let persister = Persister::new(
    //     Arc::new(local_disk),
    //     "test_host",
    //     Arc::clone(&time_provider) as _,
    // );
    // let catalog = Catalog::new(node_id, instance_id);
    // let _ = catalog.db_or_create("my_db");

    // persister.persist_catalog(&catalog).await.unwrap();
}

// NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
// old catalog cleanup might not be necessary with the way the new catalog is persisted as a
// log, and only snapshotting semi-regularly
#[tokio::test]
#[ignore = "this used the old persist_catalog method, so needs to be refactored"]
async fn persist_catalog_with_cleanup() {
    unimplemented!();
    // let node_id = Arc::from("sample-host-id");
    // let instance_id = Arc::from("sample-instance-id");
    // let prefix = test_helpers::tmp_dir().unwrap();
    // let local_disk = LocalFileSystem::new_with_prefix(prefix).unwrap();
    // let obj_store: Arc<dyn ObjectStore> = Arc::new(local_disk);
    // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    // let persister = Persister::new(Arc::clone(&obj_store), "test_host", time_provider);
    // let catalog = Catalog::new(Arc::clone(&node_id), instance_id);
    // persister.persist_catalog(&catalog).await.unwrap();
    // let db_schema = catalog.db_or_create("my_db_1").unwrap();
    // persister.persist_catalog(&catalog).await.unwrap();
    // let _ = catalog.db_or_create("my_db_2").unwrap();
    // persister.persist_catalog(&catalog).await.unwrap();
    // let _ = catalog.db_or_create("my_db_3").unwrap();
    // persister.persist_catalog(&catalog).await.unwrap();
    // let _ = catalog.db_or_create("my_db_4").unwrap();
    // persister.persist_catalog(&catalog).await.unwrap();
    // let _ = catalog.db_or_create("my_db_5").unwrap();
    // persister.persist_catalog(&catalog).await.unwrap();

    // let batch = |name: &str, num: u32| {
    //     let _ = catalog.apply_catalog_batch(&CatalogBatch {
    //         database_id: db_schema.id,
    //         database_name: Arc::clone(&db_schema.name),
    //         time_ns: 5000,
    //         ops: vec![CatalogOp::CreateTable(CreateTableLog {
    //             database_id: db_schema.id,
    //             database_name: Arc::clone(&db_schema.name),
    //             table_name: name.into(),
    //             table_id: TableId::from(num),
    //             field_definitions: vec![FieldDefinition {
    //                 name: "column".into(),
    //                 id: ColumnId::from(num),
    //                 data_type: FieldDataType::String,
    //             }],
    //             key: vec![num.into()],
    //         })],
    //     });
    // };

    // batch("table_zero", 0);
    // persister.persist_catalog(&catalog).await.unwrap();
    // batch("table_one", 1);
    // persister.persist_catalog(&catalog).await.unwrap();
    // batch("table_two", 2);
    // persister.persist_catalog(&catalog).await.unwrap();
    // batch("table_three", 3);
    // persister.persist_catalog(&catalog).await.unwrap();

    // // We've persisted the catalog 10 times and nothing has changed
    // // So now we need to persist the catalog two more times and we should
    // // see the first 2 catalogs be dropped.
    // batch("table_four", 4);
    // persister.persist_catalog(&catalog).await.unwrap();
    // batch("table_five", 5);
    // persister.persist_catalog(&catalog).await.unwrap();

    // // Make sure the deletions have all ocurred
    // sleep(Duration::from_secs(2)).await;

    // let mut stream = obj_store.list(None);
    // let mut items = Vec::new();
    // while let Some(item) = stream.next().await {
    //     items.push(item.unwrap());
    // }

    // // Sort by oldest fisrt
    // items.sort_by(|a, b| b.location.cmp(&a.location));

    // assert_eq!(items.len(), 10);
    // // The first path should contain this number meaning we've
    // // eliminated the first two items
    // assert_eq!(18446744073709551613, u64::MAX - 2);

    // // Assert that we have 10 catalogs of decreasing number
    // assert_eq!(
    //     items[0].location,
    //     "test_host/catalogs/18446744073709551613.catalog".into()
    // );
    // assert_eq!(
    //     items[1].location,
    //     "test_host/catalogs/18446744073709551612.catalog".into()
    // );
    // assert_eq!(
    //     items[2].location,
    //     "test_host/catalogs/18446744073709551611.catalog".into()
    // );
    // assert_eq!(
    //     items[3].location,
    //     "test_host/catalogs/18446744073709551610.catalog".into()
    // );
    // assert_eq!(
    //     items[4].location,
    //     "test_host/catalogs/18446744073709551609.catalog".into()
    // );
    // assert_eq!(
    //     items[5].location,
    //     "test_host/catalogs/18446744073709551608.catalog".into()
    // );
    // assert_eq!(
    //     items[6].location,
    //     "test_host/catalogs/18446744073709551607.catalog".into()
    // );
    // assert_eq!(
    //     items[7].location,
    //     "test_host/catalogs/18446744073709551606.catalog".into()
    // );
    // assert_eq!(
    //     items[8].location,
    //     "test_host/catalogs/18446744073709551605.catalog".into()
    // );
    // assert_eq!(
    //     items[9].location,
    //     "test_host/catalogs/18446744073709551604.catalog".into()
    // );
}

// NOTE(trevor/catalog-refactor): this used the old persist_catalog method, so needs to be refactored.
#[tokio::test]
#[ignore = "this used the old persist_catalog method, so needs to be refactored"]
async fn persist_and_load_newest_catalog() {
    unimplemented!();
    // let node_id: Arc<str> = Arc::from("sample-host-id");
    // let instance_id: Arc<str> = Arc::from("sample-instance-id");
    // let local_disk =
    //     LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
    // let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    // let persister = Persister::new(Arc::new(local_disk), "test_host", time_provider);
    // let catalog = Catalog::new(Arc::clone(&node_id), Arc::clone(&instance_id));
    // let _ = catalog.db_or_create("my_db");

    // persister.persist_catalog(&catalog).await.unwrap();

    // let catalog = Catalog::new(Arc::clone(&node_id), Arc::clone(&instance_id));
    // let _ = catalog.db_or_create("my_second_db");

    // persister.persist_catalog(&catalog).await.unwrap();

    // let catalog = persister
    //     .load_catalog()
    //     .await
    //     .expect("loading the catalog did not cause an error")
    //     .expect("there was a catalog to load");

    // // my_second_db
    // assert!(catalog.db_exists(DbId::from(1)));
    // // my_db
    // assert!(!catalog.db_exists(DbId::from(0)));
}
