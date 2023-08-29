//! An actor task maintaining [`MerkleSearchTree`] state.
use std::sync::Arc;

use data_types::{NamespaceName, NamespaceSchema};
use merkle_search_tree::{digest::RootHash, MerkleSearchTree};
use observability_deps::tracing::{debug, info, trace};
use tokio::sync::{mpsc, oneshot};

use crate::namespace_cache::{CacheMissErr, NamespaceCache};

use super::handle::AntiEntropyHandle;

/// Requests sent from an [`AntiEntropyHandle`] to an [`AntiEntropyActor`].
#[derive(Debug)]
pub(super) enum Op {
    /// Request the content / merkle tree root hash.
    ContentHash(oneshot::Sender<RootHash>),
}

/// A [`NamespaceCache`] anti-entropy state tracking primitive.
///
/// This actor maintains a [`MerkleSearchTree`] covering the content of
/// [`NamespaceSchema`] provided to it.
///
/// [`NamespaceCache`]: crate::namespace_cache::NamespaceCache
#[derive(Debug)]
pub struct AntiEntropyActor<T> {
    cache: T,
    schema_rx: mpsc::Receiver<NamespaceName<'static>>,
    op_rx: mpsc::Receiver<Op>,

    mst: MerkleSearchTree<NamespaceName<'static>, Arc<NamespaceSchema>>,
}

impl<T> AntiEntropyActor<T>
where
    // A cache lookup in the underlying impl MUST be infallible - it's asking
    // for existing records, and the cache MUST always return them.
    T: NamespaceCache<ReadError = CacheMissErr>,
{
    /// Initialise a new [`AntiEntropyActor`], and return the
    /// [`AntiEntropyHandle`] used to interact with it.
    ///
    /// The provided cache is used to resolve the most up-to-date
    /// [`NamespaceSchema`] for given a [`NamespaceName`], which SHOULD be as
    /// fast as possible and MUST be infallible.
    pub fn new(cache: T) -> (Self, AntiEntropyHandle) {
        // Initialise the queue used to push schema updates.
        //
        // Filling this queue causes dropped updates to the MST, which in turn
        // cause spurious convergence rounds which are expensive relative to a
        // bit of worst-case RAM (network trips, CPU for hashing, gossip
        // traffic).
        //
        // Each queue entry has a 64 byte upper limit (excluding queue & pointer
        // overhead) because the namespace name has a 64 byte upper bound. This
        // means there's an upper (worst case) RAM utilisation bound of 1 MiB
        // for these queue values.
        let (schema_tx, schema_rx) = mpsc::channel(16_000);

        // A "command" channel for non-schema-update messages.
        let (op_tx, op_rx) = mpsc::channel(50);

        (
            Self {
                cache,
                schema_rx,
                op_rx,
                mst: MerkleSearchTree::default(),
            },
            AntiEntropyHandle::new(op_tx, schema_tx),
        )
    }

    /// Block and run the anti-entropy actor until all [`AntiEntropyHandle`]
    /// instances for it have been dropped.
    pub async fn run(mut self) {
        info!("starting anti-entropy state actor");

        loop {
            tokio::select! {
                // Bias towards processing schema updates.
                //
                // This presents the risk of starvation / high latency for
                // "operation" commands (sent over op_rx) under heavy update
                // load (which is unlikely) at the benefit of ensuring all
                // operations occur against an up-to-date MST.
                //
                // This helps avoid spurious convergence rounds by ensuring all
                // updates are always applied as soon as possible.
                biased;

                // Immediately apply the available MST update.
                Some(name) = self.schema_rx.recv() => self.handle_upsert(name).await,

                // Else process an  "operation" command from the actor handle.
                Some(op) = self.op_rx.recv() => self.handle_op(op),

                // And stop if both channels have closed.
                else => {
                    info!("stopping anti-entropy state actor");
                    return
                },
            }
        }
    }

    async fn handle_upsert(&mut self, name: NamespaceName<'static>) {
        let schema = match self.cache.get_schema(&name).await {
            Ok(v) => v,
            Err(CacheMissErr { .. }) => {
                // This is far from ideal as it causes the MST to skip applying
                // an update, effectively causing it to diverge from the actual
                // cache state.
                //
                // This will cause spurious convergence runs between peers that
                // have applied this update to their MST, in turn causing it to
                // converge locally.
                //
                // If no node applied this update, then this value will not be
                // converged between any peers until a subsequent update causes
                // a successful MST update on a peer.
                //
                // Instead the bounds require the only allowable error to be a
                // cache miss error (no I/O error or other problem) - this can't
                // ever happen, because state updates are enqueued for existing
                // schemas, so there MUST always be an entry returned.
                panic!("cache miss for namespace schema {}", name);
            }
        };

        trace!(%name, ?schema, "applying schema");

        self.mst.upsert(name, &schema);
    }

    fn handle_op(&mut self, op: Op) {
        match op {
            Op::ContentHash(tx) => {
                let root_hash = self.mst.root_hash().clone();

                debug!(%root_hash, "generated content hash");

                // The caller may have stopped listening, so ignore any errors.
                let _ = tx.send(root_hash);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::namespace_cache::MemoryNamespaceCache;

    use super::*;

    #[tokio::test]
    async fn test_empty_content_hash_fixture() {
        let (actor, handle) = AntiEntropyActor::new(Arc::new(MemoryNamespaceCache::default()));
        tokio::spawn(actor.run());

        let got = handle.content_hash().await;
        assert_eq!(got.to_string(), "UEnXR4Cj4H1CAqtH1M7y9A==");
    }
}
