use observability_deps::tracing::debug;

use crate::{CatalogError, catalog::Catalog, log::FieldDataType};

#[test_log::test(tokio::test)]
async fn test_catalog_update_sub() {
    let catalog = Catalog::new_in_memory("cats").await.unwrap();
    let mut sub = catalog.subscribe_to_updates("test_sub").await;
    let handle = tokio::spawn(async move {
        let mut n_updates = 0;
        while let Some(update) = sub.recv().await {
            debug!(?update, "got an update");
            n_updates += 1;
        }
        n_updates
    });

    catalog.create_database("foo").await.unwrap();
    catalog
        .create_table("foo", "bar", &["tag"], &[("field", FieldDataType::String)])
        .await
        .unwrap();

    // drop the catalog so the channel closes and the handle above doesn't hang...
    drop(catalog);

    let n_updates = handle.await.unwrap();
    assert_eq!(2, n_updates);
}

#[test_log::test(tokio::test)]
async fn test_catalog_channel_stop() {
    let catalog = Catalog::new_in_memory("leafs").await.unwrap();

    // create a valid subscription that listens and ACKs updates:
    let mut sub = catalog.subscribe_to_updates("marner").await;
    tokio::spawn(async move {
        while sub.recv().await.is_some() {
            // do nothing just receive the message to ACK the broadcasted update
        }
    });
    assert!(
        catalog.create_database("foo").await.is_ok(),
        "create database with one active subscription succeeds"
    );

    // create subscription but stop it so that it gets ignored by the broadcast:
    let sub = catalog.subscribe_to_updates("nylander").await;
    sub.stop();
    // NB: do not need to spawn a task to receive the message because the receiver set the
    // stopped state; this will have the sender ignore the subscription...
    assert!(
        catalog.create_database("bar").await.is_ok(),
        "create database with one active subscription and one stopped subscription succeeds"
    );

    // create a subscription but close it so it causes an error during broadcast:
    let mut sub = catalog.subscribe_to_updates("matthews").await;
    sub.close();
    let Err(CatalogError::Subscription(error)) = catalog.create_database("gin").await else {
        panic!("create database with a dropped subscription should fail with subscription error");
    };

    debug!(%error, "subscription error");
}
