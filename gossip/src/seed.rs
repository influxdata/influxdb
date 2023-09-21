use std::{future, net::SocketAddr, sync::Arc};

use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{
    net::{self, UdpSocket},
    time::{timeout, MissedTickBehavior},
};
use tracing::{debug, warn};

use crate::{
    metric::{SentBytes, SentFrames},
    reactor::ping,
    RESOLVE_TIMEOUT, SEED_PING_INTERVAL,
};

/// The user-provided seed peer address.
///
/// NOTE: the IP/socket address this resolves to may change over the
/// lifetime of the peer, so the raw address is retained instead of
/// the [`SocketAddr`] to ensure it is constantly re-resolved when the peer
/// is unreachable.
#[derive(Debug)]
pub(crate) struct Seed(String);

impl Seed {
    pub(crate) fn new(addr: String) -> Self {
        Self(addr)
    }

    /// Resolve this peer address, returning an error if resolution is not
    /// complete within [`RESOLVE_TIMEOUT`].
    pub(crate) async fn resolve(&self) -> Option<SocketAddr> {
        match timeout(RESOLVE_TIMEOUT, resolve(&self.0)).await {
            Ok(v) => v,
            Err(_) => {
                warn!(addr = %self.0, "timeout resolving seed address");
                None
            }
        }
    }
}

/// Resolve `addr`, returning the first IP address, if any.
async fn resolve(addr: &str) -> Option<SocketAddr> {
    match net::lookup_host(addr).await.map(|mut v| v.next()) {
        Ok(Some(v)) => {
            debug!(%addr, peer=%v, "resolved peer address");
            Some(v)
        }
        Ok(None) => {
            warn!(%addr, "resolved peer address contains no IPs");
            None
        }
        Err(e) => {
            warn!(%addr, error=%e, "failed to resolve peer address");
            None
        }
    }
}

/// Block forever, sending `ping_frame` over `socket` to all the entries in
/// `seeds`.
///
/// This method immediately pings all the seeds, and then pings periodically at
/// [`SEED_PING_INTERVAL`].
///
/// Seeds must be periodically pinged to ensure they're discovered when they
/// come online - if seeds were not pinged continuously, this node could become
/// isolated from all peers, mark all known peers as dead, and would never
/// rejoin.
pub(super) async fn seed_ping_task(
    seeds: Arc<[Seed]>,
    socket: Arc<UdpSocket>,
    ping_frame: Arc<[u8]>,
    sent_frames: SentFrames,
    sent_bytes: SentBytes,
) {
    let mut interval = tokio::time::interval(SEED_PING_INTERVAL);

    // Do not burden seeds with faster PING frames to catch up this timer.
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Start the ping loop, with the first iteration starting immediately.
    loop {
        interval.tick().await;

        let bytes_sent = seeds
            .iter()
            .map(|seed| async {
                if let Some(addr) = seed.resolve().await {
                    ping(&ping_frame, &socket, addr, &sent_frames, &sent_bytes).await
                } else {
                    0
                }
            })
            .collect::<FuturesUnordered<_>>()
            .fold(0, |acc, x| future::ready(acc + x))
            .await;

        debug!(bytes_sent, "seed ping sweep complete");
    }
}
