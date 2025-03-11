use std::sync::Arc;

use anyhow::Context;
use futures::future::try_join_all;
use tokio::sync::{mpsc, oneshot};

use crate::catalog::CatalogUpdate;

#[derive(Debug, thiserror::Error)]
#[error("error in catalog update subscribers: {0:?}")]
pub struct SubscriptionError(#[from] anyhow::Error);

const CATALOG_SUBSCRIPTION_BUFFER_SIZE: usize = 10_000;

type CatalogUpdateSender = mpsc::Sender<(Arc<CatalogUpdate>, oneshot::Sender<()>)>;
pub type CatalogUpdateReceiver = mpsc::Receiver<(Arc<CatalogUpdate>, oneshot::Sender<()>)>;

#[derive(Debug, Default)]
pub(crate) struct CatalogSubscriptions {
    subscriptions: hashbrown::HashMap<Arc<str>, CatalogUpdateSender>,
}

impl CatalogSubscriptions {
    pub(crate) fn subscribe(
        &mut self,
        subscription_name: impl Into<Arc<str>>,
    ) -> CatalogUpdateReceiver {
        let (tx, rx) = mpsc::channel(CATALOG_SUBSCRIPTION_BUFFER_SIZE);
        self.subscriptions.insert(subscription_name.into(), tx);
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
                sub.send((update_cloned, tx))
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
