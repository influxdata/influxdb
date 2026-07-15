//! Domain events and subscription broadcasting for v3 catalog state changes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Context;
use futures::future::try_join_all;
use hashbrown::HashMap;
use influxdb3_id::{
    DbId, DistinctCacheId, LastCacheId, NodeId, QueryGroupId, RoleId, TableId, TokenId, TriggerId,
    UserId,
};
use thiserror::Error;
use tokio::sync::{Notify, mpsc, oneshot};
use uuid::Uuid;

use crate::format::FeatureLevel;

#[derive(Debug, Error)]
#[error("error in catalog update subscribers: {0:#}")]
pub struct SubscriptionError(#[from] anyhow::Error);

/// `send_update` waits for every subscriber to ACK before returning, so callers
/// invoke it serially per catalog file — at most one message is ever queued
/// per subscriber. A depth of 1 matches that contract; if the apply path is
/// parallelized, this can be increased to allow for orthogonal events to be
/// queued for broadcast simultaneously.
const CATALOG_SUBSCRIPTION_BUFFER_SIZE: usize = 1;

/// A domain event describing a catalog state change.
#[derive(Debug, Clone)]
pub enum CatalogEvent {
    // Cluster feature level has advanced
    FeatureLevelAdvanced {
        committed: FeatureLevel,
    },

    // Database
    DatabaseCreated {
        db_id: DbId,
    },
    DatabaseSoftDeleted {
        db_id: DbId,
    },
    DatabaseHardDeleted {
        db_id: DbId,
    },
    DatabaseRetentionPeriodChanged {
        db_id: DbId,
    },

    // Table
    TableCreated {
        db_id: DbId,
        table_id: TableId,
    },
    TableUpdated {
        db_id: DbId,
        table_id: TableId,
    },
    TableSoftDeleted {
        db_id: DbId,
        table_id: TableId,
    },
    TableHardDeleted {
        db_id: DbId,
        table_id: TableId,
    },
    TableRetentionPeriodChanged {
        db_id: DbId,
        table_id: TableId,
    },

    // Last Cache
    LastCacheCreated {
        db_id: DbId,
        table_id: TableId,
        cache_id: LastCacheId,
    },
    LastCacheDeleted {
        db_id: DbId,
        table_id: TableId,
        cache_id: LastCacheId,
    },

    // Distinct Cache
    DistinctCacheCreated {
        db_id: DbId,
        table_id: TableId,
        cache_id: DistinctCacheId,
    },
    DistinctCacheDeleted {
        db_id: DbId,
        table_id: TableId,
        cache_id: DistinctCacheId,
    },

    // Trigger
    TriggerCreated {
        db_id: DbId,
        trigger_id: TriggerId,
    },
    TriggerEnabled {
        db_id: DbId,
        trigger_id: TriggerId,
    },
    TriggerDisabled {
        db_id: DbId,
        trigger_id: TriggerId,
    },
    TriggerDeleted {
        db_id: DbId,
        trigger_id: TriggerId,
        force: bool,
    },

    // Node
    NodeRegistered {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
        process_uuid: Uuid,
    },
    NodeStopped {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
        process_uuid: Uuid,
    },
    NodeRequestStop {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
        process_uuid: Uuid,
    },
    NodeAckedStop {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
        process_uuid: Uuid,
    },
    NodeRemoveRequested {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
    },
    NodeUnregistered {
        node_id: Arc<str>,
        node_catalog_id: NodeId,
    },

    // Config
    StorageModeChanged,
    GenerationDurationChanged,

    /// A repository's id counter was set explicitly (migration / compaction).
    NextIdSet,

    // Token
    TokenCreated {
        token_id: TokenId,
    },
    TokenRegenerated {
        token_id: TokenId,
    },
    TokenDeleted {
        token_id: TokenId,
    },

    // User
    UserCreated {
        user_id: UserId,
    },
    UserUpdated {
        user_id: UserId,
    },
    UserDeleted {
        user_id: UserId,
    },
    UserRestored {
        user_id: UserId,
    },
    LoginIdentityCreated {
        user_id: UserId,
    },
    LoginIdentityUpdated {
        user_id: UserId,
    },
    LoginIdentityDeleted {
        user_id: UserId,
    },
    RefreshTokenCreated {
        user_id: UserId,
    },
    RefreshTokenRevoked {
        user_id: UserId,
    },
    AllRefreshTokensRevoked {
        user_id: UserId,
    },
    UserRolesUpdated {
        user_id: UserId,
        role_ids: Vec<RoleId>,
    },

    // Query group
    QueryGroupCreated {
        query_group_id: QueryGroupId,
    },
    QueryGroupUpdated {
        query_group_id: QueryGroupId,
    },
    QueryGroupDeleted {
        query_group_id: QueryGroupId,
    },

    // Role
    RoleCreated {
        role_id: RoleId,
    },
    RoleUpdated {
        role_id: RoleId,
    },
    RoleDeleted {
        role_id: RoleId,
    },

    /// The catalog was replaced wholesale from a backup. Subscribers should
    /// drop any cached per-resource state and rebuild from the catalog's
    /// post-restore view, since IDs and content may differ from anything
    /// they held before this message.
    CatalogFullyRestored {
        restore_id: Arc<str>,
    },
}

/// A batch of [`CatalogEvent`]s produced by applying a single catalog file.
#[derive(Debug)]
pub struct CatalogUpdate {
    events: Vec<CatalogEvent>,
}

impl CatalogUpdate {
    pub(crate) fn new(events: Vec<CatalogEvent>) -> Self {
        Self { events }
    }

    pub fn events(&self) -> impl Iterator<Item = &CatalogEvent> {
        self.events.iter()
    }
}

/// Carries the broadcasted update plus the ACK channel back to the
/// broadcaster. Dropping this fires the ACK, i.e., after the subscriber
/// is done processing the events; the broadcaster's `send_update` returns
/// once every non-stopped subscriber has ACK'd.
///
/// Subscribers should drop the message promptly. The broadcaster waits
/// for every ACK before returning, so any inline work between `recv` and
/// drop blocks the next catalog file apply. Hand events off to a queue or
/// mark state stale rather than doing heavy work before drop.
#[derive(Debug)]
pub struct CatalogUpdateMessage {
    update: Arc<CatalogUpdate>,
    ack_tx: Option<oneshot::Sender<()>>,
}

impl CatalogUpdateMessage {
    fn new(update: Arc<CatalogUpdate>, tx: oneshot::Sender<()>) -> Self {
        Self {
            update,
            ack_tx: Some(tx),
        }
    }

    pub fn update(&self) -> &CatalogUpdate {
        &self.update
    }

    pub fn events(&self) -> impl Iterator<Item = &CatalogEvent> {
        self.update.events()
    }

    /// Whether this update was produced by a catalog restore.
    ///
    /// Restores are signalled by a [`CatalogEvent::CatalogFullyRestored`] event in
    /// the update; subscribers gate state-rebuilding work on this so that
    /// per-event handlers don't have to enumerate every entity that may have
    /// changed under a wholesale state swap.
    pub fn is_restore(&self) -> bool {
        self.events()
            .any(|e| matches!(e, CatalogEvent::CatalogFullyRestored { .. }))
    }
}

impl Drop for CatalogUpdateMessage {
    fn drop(&mut self) {
        if let Some(tx) = self.ack_tx.take() {
            // Send may fail if the broadcaster already returned via the
            // stop signal (subscriber stopped while the message was still
            // in flight); in that case the receiver is gone and there is
            // nothing to ACK.
            let _ = tx.send(());
        }
    }
}

#[derive(Debug, Clone)]
struct CatalogUpdateSender {
    tx: mpsc::Sender<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
    stop_signal: Arc<Notify>,
}

impl CatalogUpdateSender {
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    async fn send(
        &self,
        msg: CatalogUpdateMessage,
    ) -> Result<(), mpsc::error::SendError<CatalogUpdateMessage>> {
        self.tx.send(msg).await
    }
}

#[derive(Debug)]
pub struct CatalogUpdateReceiver {
    rx: mpsc::Receiver<CatalogUpdateMessage>,
    stopped: Arc<AtomicBool>,
    stop_signal: Arc<Notify>,
}

impl CatalogUpdateReceiver {
    /// Opt this receiver out of future broadcasts without closing the channel.
    /// Use during orderly shutdown to avoid broadcast errors from races with
    /// channel close.
    ///
    /// Any in-flight broadcast that already queued a message for this receiver
    /// is woken via [`Notify`] and treated as ACK'd, so the broadcaster does
    /// not block on a stopped subscriber that may not drain.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.stop_signal.notify_waiters();
    }

    pub fn close(&mut self) {
        self.rx.close();
    }

    pub async fn recv(&mut self) -> Option<CatalogUpdateMessage> {
        self.rx.recv().await
    }
}

impl futures::Stream for CatalogUpdateReceiver {
    type Item = CatalogUpdateMessage;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[derive(Debug, Default)]
pub(crate) struct CatalogSubscriptions {
    subscriptions: HashMap<&'static str, CatalogUpdateSender>,
}

impl CatalogSubscriptions {
    /// Panics on duplicate `name` — one subscription per named component is
    /// the contract; duplicates should fail loudly at server startup.
    pub(crate) fn subscribe(&mut self, name: &'static str) -> CatalogUpdateReceiver {
        let (tx, rx) = mpsc::channel(CATALOG_SUBSCRIPTION_BUFFER_SIZE);
        let stopped = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::new(Notify::new());
        assert!(
            self.subscriptions
                .insert(
                    name,
                    CatalogUpdateSender {
                        tx,
                        stopped: Arc::clone(&stopped),
                        stop_signal: Arc::clone(&stop_signal),
                    },
                )
                .is_none(),
            "duplicate catalog subscription name: {name}",
        );
        CatalogUpdateReceiver {
            rx,
            stopped,
            stop_signal,
        }
    }

    /// Completes once every subscriber has either dropped its
    /// [`CatalogUpdateMessage`] (firing the ACK) or transitioned to stopped
    /// while the message was in flight.
    pub(crate) async fn send_update(
        &self,
        update: Arc<CatalogUpdate>,
    ) -> Result<(), SubscriptionError> {
        let futures = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| !sub.is_stopped())
            .map(|(name, sub)| {
                let update = Arc::clone(&update);
                let name = *name;
                let sub = sub.clone();
                async move {
                    let (tx, ack_rx) = oneshot::channel();
                    sub.send(CatalogUpdateMessage::new(update, tx))
                        .await
                        .with_context(|| format!("failed to send update to {name}"))?;

                    // Race: subscriber may call `stop()` between the filter
                    // above and now. Register as a `Notified` waiter *before*
                    // re-checking `is_stopped()` so we cover all interleavings:
                    //   - stop happened before `enable()`: the recheck observes
                    //     the flag and we return.
                    //   - stop happens after `enable()`: `notify_waiters()`
                    //     wakes the future inside the `select!`.
                    let stop_signal = sub.stop_signal.notified();
                    tokio::pin!(stop_signal);
                    stop_signal.as_mut().enable();
                    if sub.is_stopped() {
                        return Ok::<(), anyhow::Error>(());
                    }

                    tokio::select! {
                        result = ack_rx => {
                            result.with_context(|| {
                                format!("failed to receive ACK from {name}")
                            })?;
                        }
                        _ = stop_signal => {}
                    }
                    Ok(())
                }
            })
            .collect::<Vec<_>>();

        try_join_all(futures)
            .await
            .map_err(SubscriptionError::from)?;
        Ok(())
    }

    /// Drop the subscription registered under `name`.
    pub(crate) fn unsubscribe(&mut self, name: &str) {
        self.subscriptions.remove(name);
    }

    pub(crate) fn prune_closed(&mut self) {
        self.subscriptions.retain(|_, sub| !sub.is_closed());
    }
}

#[cfg(test)]
mod tests;
