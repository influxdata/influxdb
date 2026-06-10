use std::sync::Arc;
use std::time::Duration;

use influxdb3_id::DbId;
use tokio::time::timeout;

use super::{CatalogEvent, CatalogSubscriptions, CatalogUpdate};

fn sample_event() -> CatalogEvent {
    CatalogEvent::DatabaseCreated {
        db_id: DbId::new(1),
    }
}

fn sample_update() -> Arc<CatalogUpdate> {
    Arc::new(CatalogUpdate::new(vec![sample_event()]))
}

#[tokio::test]
async fn subscriber_receives_update() {
    let mut subs = CatalogSubscriptions::default();
    let mut rx = subs.subscribe("a");

    let update = sample_update();
    let broadcast = tokio::spawn({
        let subs = subs;
        let update = Arc::clone(&update);
        async move { subs.send_update(update).await }
    });

    let received = rx.recv().await.expect("should receive update");
    assert_eq!(received.events().count(), 1);
    drop(received);

    broadcast.await.unwrap().unwrap();
}

#[tokio::test]
#[should_panic(expected = "duplicate catalog subscription name: a")]
async fn duplicate_subscribe_panics() {
    let mut subs = CatalogSubscriptions::default();
    let _rx = subs.subscribe("a");
    let _rx2 = subs.subscribe("a");
}

#[tokio::test]
async fn broadcast_waits_for_message_drop_not_recv() {
    let mut subs = CatalogSubscriptions::default();
    let mut rx = subs.subscribe("a");
    let update = sample_update();

    let broadcast = tokio::spawn({
        let update = Arc::clone(&update);
        async move { subs.send_update(update).await }
    });

    let msg = rx.recv().await.expect("should receive update");

    // recv() alone must not ACK — broadcaster should still be waiting.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !broadcast.is_finished(),
        "broadcast completed before subscriber dropped the message",
    );

    drop(msg);

    timeout(Duration::from_millis(100), broadcast)
        .await
        .expect("broadcast should complete once message is dropped")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn fanout_to_multiple_subscribers() {
    let mut subs = CatalogSubscriptions::default();
    let mut rx_a = subs.subscribe("a");
    let mut rx_b = subs.subscribe("b");
    let mut rx_c = subs.subscribe("c");

    let update = sample_update();
    let broadcast = tokio::spawn(async move { subs.send_update(update).await });

    for rx in [&mut rx_a, &mut rx_b, &mut rx_c] {
        let msg = rx.recv().await.expect("subscriber should receive");
        assert_eq!(msg.events().count(), 1);
    }

    broadcast.await.unwrap().unwrap();
}

#[tokio::test]
async fn broadcast_waits_for_every_subscriber_to_drop() {
    let mut subs = CatalogSubscriptions::default();
    let mut rx_a = subs.subscribe("a");
    let mut rx_b = subs.subscribe("b");
    let mut rx_c = subs.subscribe("c");

    let broadcast = tokio::spawn(async move { subs.send_update(sample_update()).await });

    let msg_a = rx_a.recv().await.expect("a receives");
    let msg_b = rx_b.recv().await.expect("b receives");
    let msg_c = rx_c.recv().await.expect("c receives");

    // Drop one at a time and confirm the broadcaster keeps waiting until
    // every subscriber's message has been dropped.
    drop(msg_a);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !broadcast.is_finished(),
        "broadcaster should still be waiting on b and c",
    );

    drop(msg_b);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !broadcast.is_finished(),
        "broadcaster should still be waiting on c",
    );

    drop(msg_c);
    timeout(Duration::from_millis(100), broadcast)
        .await
        .expect("broadcast completes once every subscriber drops")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stop_then_drop_unblocks_broadcaster() {
    let mut subs = CatalogSubscriptions::default();
    let rx = subs.subscribe("a");

    let broadcast = tokio::spawn(async move { subs.send_update(sample_update()).await });

    // The broadcaster filtered before the subscriber stopped, so a message
    // sits in the buffer. Calling stop() wakes the broadcaster via the stop
    // signal; dropping the receiver afterwards is harmless.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !broadcast.is_finished(),
        "broadcaster should still be waiting on ACK",
    );
    rx.stop();
    drop(rx);

    timeout(Duration::from_millis(100), broadcast)
        .await
        .expect("broadcast completes once stop signal fires")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stop_without_drop_unblocks_broadcaster() {
    let mut subs = CatalogSubscriptions::default();
    let rx = subs.subscribe("a");

    let broadcast = tokio::spawn(async move { subs.send_update(sample_update()).await });

    // Buffer the message, then stop *without* dropping the receiver. The
    // stop signal must wake the broadcaster on its own; an undropped
    // receiver leaves the buffered message (and its ACK) intact.
    tokio::time::sleep(Duration::from_millis(20)).await;
    rx.stop();

    timeout(Duration::from_millis(100), broadcast)
        .await
        .expect("broadcast completes via stop signal even without drop")
        .unwrap()
        .unwrap();

    drop(rx);
}

#[tokio::test]
async fn stopped_subscriber_is_skipped() {
    let mut subs = CatalogSubscriptions::default();
    let rx_stopped = subs.subscribe("stopped");
    let mut rx_active = subs.subscribe("active");

    rx_stopped.stop();

    let update = sample_update();
    let broadcast = tokio::spawn(async move { subs.send_update(update).await });

    // Active subscriber still receives.
    let msg = rx_active.recv().await.expect("active subscriber receives");
    assert_eq!(msg.events().count(), 1);
    drop(msg);

    // Broadcast completes without the stopped subscriber consuming anything.
    timeout(Duration::from_millis(100), broadcast)
        .await
        .expect("broadcast should complete")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn closed_subscriber_causes_send_failure() {
    let mut subs = CatalogSubscriptions::default();
    let mut rx = subs.subscribe("closed");
    rx.close();
    drop(rx);

    let err = subs
        .send_update(sample_update())
        .await
        .expect_err("send_update should fail when subscriber channel is closed");
    let msg = format!("{err:#}");
    assert!(msg.contains("closed"), "unexpected error: {msg}");
}

#[tokio::test]
async fn prune_closed_removes_dead_subscriptions() {
    let mut subs = CatalogSubscriptions::default();
    let rx_alive = subs.subscribe("alive");
    let rx_dead = subs.subscribe("dead");
    drop(rx_dead);

    subs.prune_closed();

    // Dead subscription is gone; broadcasting succeeds without it.
    let mut rx_alive = rx_alive;
    let broadcast = tokio::spawn(async move { subs.send_update(sample_update()).await });
    let _ = rx_alive.recv().await.expect("alive subscriber receives");
    broadcast.await.unwrap().unwrap();
}
