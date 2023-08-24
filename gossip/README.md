A simple gossip & broadcast primitive for best-effort metadata distribution
between IOx nodes.

# Peers

Peers are uniquely identified by their self-reported "identity" UUID. A unique
UUID is generated for each gossip instance, ensuring the identity changes across
restarts of the underlying node.

An identity is associated with an immutable socket address used for peer
communication.

# Topics

The gossip system implements a topic / interest based, send-side filter to
prevent node A sending frames to node B that it doesn't care about - this helps
reduce traffic and CPU / processing on both nodes.

During peer exchange, a node advertises the set of peers it is (and other peers
are) interested in, and messages for a given topic are dispatched only to
interested nodes.

Topics are immutable for the lifetime of a gossip instance, and control frames
are exempt from topic filtering and are always exchanged between all peers.

# Transport

Prefer small payloads where possible, and expect loss of some messages - this
primitive provides *best effort* delivery.

This implementation sends unicast UDP frames between peers, with support for
both control frames & user payloads. The maximum UDP message size is 65,507
bytes ([`MAX_USER_PAYLOAD_BYTES`] for application-level payloads), but a packet
this large is fragmented into smaller (at most MTU-sized) packets and is at
greater risk of being dropped due to a lost fragment.

# Security

Messages exchanged between peers are unauthenticated and connectionless - it's
trivial to forge a message appearing to come from a different peer, or include
malicious payloads.

The security model of this implementation expects the peers to be running in a
trusted environment, secure from malicious users.

# Peer Exchange

When a gossip instance is initialised, it advertises itself to the set of
user-provided "seed" peers - other gossip instances with fixed, known addresses.
The peer then bootstraps the peer list from these seed peers.

Peers are discovered through PONG messages from peers, which contain the list of
peers the sender has successfully communicated with.

On receipt of a PONG frame, a node will send PING frames to all newly discovered
peers without adding the peer to its local peer list. Once the discovered peer
responds with a PONG, the peer is added to the peer list. This acts as a
liveness check, ensuring a node only adds peers it can communicate with to its
peer list.

```text
                            ┌──────────┐
                            │   Seed   │
                            └──────────┘
                                ▲   │
                                │   │
                           (1)  │   │   (2)
                          PING  │   │  PONG
                                │   │    (contains Peer A)
                                │   ▼
                            ┌──────────┐
                            │  Local   │
                            └──────────┘
                                ▲   │
                                │   │
                           (4)  │   │   (3)
                          PONG  │   │  PING
                                │   │
                                │   ▼
                            ┌──────────┐
                            │  Peer A  │
                            └──────────┘
```

The above illustrates this process when the "local" node joins:

  1. Send PING messages to all configured seeds
  2. Receive a PONG response containing the list of all known peers
  3. Send PING frames to all discovered peers - do not add to peer list
  4. Receive PONG frames from discovered peers - add to peer list

The peer addresses sent during PEX rounds contain the advertised peer identity
and the socket address the PONG sender discovered.

# Dead Peer Removal

All peers are periodically sent a PING frame, and a per-peer counter is
incremented. If a message of any sort is received (including the PONG response
to the soliciting PING), the peer's counter is reset to 0.

Once a peer's counter reaches [`MAX_PING_UNACKED`], indicating a number of PINGs
have been sent without receiving any response, the peer is removed from the
node's peer list.

Dead peers age out of the cluster once all nodes perform the above routine. If a
peer dies, it is still sent in PONG messages as part of PEX until it is removed
from the sender's peer list, but the receiver of the PONG will not add it to the
node's peer list unless it successfully commutates, ensuring dead peers are not
propagated.

Ageing out dead peers is strictly an optimisation (and not for correctness). A
dead peer consumes a tiny amount of RAM, but also will have frames dispatched to
it - over time, as the number of dead peers accumulates, this would cause the
number of UDP frames sent per broadcast to increase, needlessly increasing
gossip traffic.

This process is heavily biased towards reliability/deliverability and is too
slow for use as a general peer health check.