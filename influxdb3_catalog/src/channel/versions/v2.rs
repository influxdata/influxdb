use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use futures::future::try_join_all;
use observability_deps::tracing::warn;
use tokio::sync::{mpsc, oneshot};

use crate::channel::{CATALOG_SUBSCRIPTION_BUFFER_SIZE, SubscriptionError};
use crate::{catalog::versions::v2::CatalogUpdate, log::versions::v4::CatalogBatch};

#[derive(Debug, Clone)]
struct CatalogUpdateSender {
    tx: mpsc::Sender<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
}

impl CatalogUpdateSender {
    /// Check the `stopped` state on this channel
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Deref for CatalogUpdateSender {
    type Target = mpsc::Sender<CatalogUpdateMessage>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for CatalogUpdateSender {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

#[derive(Debug)]
pub struct CatalogUpdateReceiver {
    rx: mpsc::Receiver<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
}

impl CatalogUpdateReceiver {
    /// Set the state of this receiver to stopped
    ///
    /// This will have broadcasted messages from the catalog ignore this channel. This is useful
    /// for cases like application shutdown, where the subscriber is closing the receiver, so we
    /// do not want to attempt to send a message to it, because doing so would return an error due
    /// to the channel being closed.
    ///
    /// This allows us to use the `stopped` state to filter out channels that have been
    /// intentionally closed before application shutdown when broadcasting, vs. those that have been
    /// closed unexpectedly, for which we want to have an error appear in the logs when broadcast
    /// fails.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

impl Deref for CatalogUpdateReceiver {
    type Target = mpsc::Receiver<CatalogUpdateMessage>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for CatalogUpdateReceiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

/// A message containing a set of `CatalogUpdate`s that can be handled by subscribers to the
/// `Catalog`.
///
/// The response is sent in the `Drop` implementation of this type, so that the consumer of these
/// messages does not need to worry about sending the response back to the catalog on broadcast.
pub struct CatalogUpdateMessage {
    update: Arc<CatalogUpdate>,
    tx: Option<oneshot::Sender<()>>,
}

impl CatalogUpdateMessage {
    /// Create a new `CatalogUpdateMessage`
    fn new(update: Arc<CatalogUpdate>, tx: oneshot::Sender<()>) -> Self {
        Self {
            update,
            tx: Some(tx),
        }
    }

    /// Iterate over the `CatalogBatch`s in the update
    pub fn batches(&self) -> impl Iterator<Item = &CatalogBatch> {
        self.update.batches()
    }
}

impl Drop for CatalogUpdateMessage {
    /// Send the response to the catalog via the oneshot sender
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx
                .send(())
                .inspect_err(|error| warn!(?error, "unable to send ACK for catalog update"));
        }
    }
}

impl std::fmt::Debug for CatalogUpdateMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogUpdateMessage")
            .field("update", &self.update)
            .finish()
    }
}

#[derive(Debug, Default)]
pub(crate) struct CatalogSubscriptions {
    subscriptions: hashbrown::HashMap<Arc<str>, CatalogUpdateSender>,
}

impl CatalogSubscriptions {
    /// Subscribe to the catalog for updates.
    ///
    /// This allows components in the system to listen for updates made to the catalog
    /// and handle/apply them as needed. A `subscription_name` is provided that identifies the
    /// component subscribing.
    ///
    /// # Panics
    ///
    /// If the provided `subscription_name` has already been used, this will panic.
    pub(crate) fn subscribe(&mut self, subscription_name: &'static str) -> CatalogUpdateReceiver {
        let (tx, rx) = mpsc::channel(CATALOG_SUBSCRIPTION_BUFFER_SIZE);
        let stopped = Arc::new(AtomicBool::new(false));
        assert!(
            self.subscriptions
                .insert(
                    Arc::from(subscription_name),
                    CatalogUpdateSender {
                        tx,
                        stopped: Arc::clone(&stopped)
                    }
                )
                .is_none(),
            "attempted to subscribe to catalog with same component name more than once, \
            name: {subscription_name}"
        );
        CatalogUpdateReceiver {
            rx,
            stopped: Arc::clone(&stopped),
        }
    }

    pub(crate) async fn send_update(
        &self,
        update: Arc<CatalogUpdate>,
    ) -> Result<(), SubscriptionError> {
        let mut responses = vec![];
        for (name, sub) in self
            .subscriptions
            .iter()
            .filter(|(_, sub)| !sub.is_stopped())
            .map(|(n, s)| (Arc::clone(n), s.clone()))
        {
            let update_cloned = Arc::clone(&update);
            responses.push(tokio::spawn(async move {
                let (tx, rx) = oneshot::channel();
                sub.send(CatalogUpdateMessage::new(update_cloned, tx))
                    .await
                    .with_context(|| format!("failed to send update to {name}"))?;
                rx.await
                    .with_context(|| format!("failed to receive response from {name}"))?;
                Ok(())
            }));
        }

        try_join_all(responses)
            .await
            .context("failed to collect responses from catalog subscribers")?
            .into_iter()
            .collect::<Result<Vec<()>, anyhow::Error>>()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
            panic!(
                "create database with a dropped subscription should fail with subscription error"
            );
        };

        debug!(%error, "subscription error");
    }
}
