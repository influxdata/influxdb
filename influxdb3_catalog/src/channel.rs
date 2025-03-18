use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use observability_deps::tracing::warn;
use tokio::sync::{mpsc, oneshot};

use crate::{catalog::CatalogUpdate, log::CatalogBatch};

#[derive(Debug, thiserror::Error)]
#[error("error in catalog update subscribers: {0:?}")]
pub struct SubscriptionError(#[from] anyhow::Error);

const CATALOG_SUBSCRIPTION_BUFFER_SIZE: usize = 10_000;

type CatalogUpdateSender = mpsc::Sender<CatalogUpdateMessage>;
pub type CatalogUpdateReceiver = mpsc::Receiver<CatalogUpdateMessage>;

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
        assert!(
            self.subscriptions
                .insert(Arc::from(subscription_name), tx)
                .is_none(),
            "attempted to subscribe to catalog with same component name more than once, \
            name: {subscription_name}"
        );
        rx
    }

    pub(crate) async fn send_update(
        &self,
        update: Arc<CatalogUpdate>,
    ) -> Result<(), SubscriptionError> {
        let mut responses = vec![];
        for (name, sub) in self
            .subscriptions
            .iter()
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

    pub(crate) fn prune_closed(&mut self) {
        self.subscriptions.retain(|_, s| s.is_closed());
    }
}

#[cfg(test)]
mod tests {
    use observability_deps::tracing::debug;

    use crate::{
        catalog::Catalog,
        log::{CatalogBatch, FieldDataType},
    };

    #[test_log::test(tokio::test)]
    async fn test_catalog_update_sub() {
        let catalog = Catalog::new_in_memory("cats").await.unwrap();
        let mut sub = catalog.subscribe_to_updates("test_sub").await;
        let handle = tokio::spawn(async move {
            let mut n_updates = 0;
            while let Some(update) = sub.recv().await {
                debug!(?update, "got an update");
                for b in update.batches() {
                    match b {
                        CatalogBatch::Node(_) => (),
                        CatalogBatch::Database(_) => (),
                    }
                }
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
}
