//! A serialiser and broadcaster of [`gossip`] messages for the
//! [`Topic::SchemaChanges`] topic.

use generated_types::{
    influxdata::iox::gossip::{
        v1::{schema_message::Event, SchemaMessage, TableCreated, TableUpdated},
        Topic,
    },
    prost::Message,
};
use gossip::MAX_USER_PAYLOAD_BYTES;
use observability_deps::tracing::{debug, error, warn};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    task::JoinHandle,
};

/// A gossip broadcast primitive specialised for schema [`Event`] notifications.
///
/// This type accepts [`Event`] from the application logic, serialises the
/// message (applying any necessary transformations due to the underlying
/// transport limitations) and broadcasts the result to all listening peers.
///
/// Serialisation and processing of the [`Event`] given to the
/// [`SchemaTx::broadcast()`] method happens in a background actor task,
/// decoupling the caller from the latency of processing each frame. Dropping
/// the [`SchemaTx`] stops this background actor task.
#[derive(Debug)]
pub struct SchemaTx {
    tx: mpsc::Sender<Event>,
    task: JoinHandle<()>,
}

impl Drop for SchemaTx {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SchemaTx {
    /// Construct a new [`SchemaTx`] that publishes gossip messages over
    /// `gossip`.
    pub fn new(gossip: gossip::GossipHandle<Topic>) -> Self {
        let (tx, rx) = mpsc::channel(100);

        let task = tokio::spawn(actor_loop(rx, gossip));

        Self { tx, task }
    }

    /// Asynchronously broadcast `event` to all interested peers.
    ///
    /// This method enqueues `event` into the serialisation queue, and processed
    /// & transmitted asynchronously.
    pub fn broadcast(&self, event: Event) {
        debug!(?event, "sending schema message");
        match self.tx.try_send(event) {
            Ok(_) => {}
            Err(TrySendError::Closed(_)) => panic!("schema serialisation actor not running"),
            Err(TrySendError::Full(_)) => {
                warn!("schema serialisation queue full, dropping message")
            }
        }
    }
}

/// A background task loop that pulls [`Event`] from `rx`, serialises / packs
/// them into a single gossip frame, and broadcasts the result over `gossip`.
async fn actor_loop(mut rx: mpsc::Receiver<Event>, gossip: gossip::GossipHandle<Topic>) {
    while let Some(event) = rx.recv().await {
        let frames = match event {
            v @ Event::NamespaceCreated(_) => vec![v],
            Event::TableCreated(v) => serialise_table_create_frames(v),
            Event::TableUpdated(v) => {
                // Split the frame up into N frames, sized as big as the gossip
                // transport will allow.
                let mut frames = Vec::with_capacity(1);
                if !serialise_table_update_frames(v, MAX_USER_PAYLOAD_BYTES, &mut frames) {
                    warn!("dropping oversized columns in table update");
                    // Continue sending the columns that were packed
                    // successfully
                }

                // It's possible all columns were oversized and therefore
                // dropped during the split.
                if frames.is_empty() {
                    warn!("dropping empty table update message");
                    continue;
                }

                frames.into_iter().map(Event::TableUpdated).collect()
            }
        };

        for frame in frames {
            let msg = SchemaMessage { event: Some(frame) };

            // Send the frame
            if let Err(e) = gossip
                .broadcast(msg.encode_to_vec(), Topic::SchemaChanges)
                .await
            {
                error!(error=%e, "failed to broadcast payload");
            }
        }
    }

    debug!("stopping schema serialisation actor");
}

/// Serialise `msg` into one or more frames and append them to `out`.
///
/// If when `msg` is serialised it is less than or equal to `max_frame_bytes` in
/// length, this method appends a single frame. If `msg` is too large, it is
/// split into `N` multiple smaller frames and each appended individually.
///
/// If any [`Column`] within the message is too large to fit into an update
/// containing only itself, then this method returns `false` indicating
/// oversized columns were dropped from the output.
///
/// [`Column`]: generated_types::influxdata::iox::gossip::v1::Column
fn serialise_table_update_frames(
    mut msg: TableUpdated,
    max_frame_bytes: usize,
    out: &mut Vec<TableUpdated>,
) -> bool {
    // Does this frame fit within the maximum allowed frame size?
    if msg.encoded_len() <= max_frame_bytes {
        // Never send empty update messages.
        if !msg.columns.is_empty() {
            out.push(msg);
        }
        return true;
    }

    // This message is too large to be sent as a single message.
    debug!(
        n_bytes = msg.encoded_len(),
        "splitting oversized table update message"
    );

    // Find the midpoint in the column list.
    //
    // If the column list is down to one candidate, then the TableUpdate
    // containing only this column is too large to be sent, so this column must
    // be dropped from the output.
    if msg.columns.len() <= 1 {
        // Return false to indicate some columns were dropped.
        return false;
    }
    let mid = msg.columns.len() / 2;

    // Split up the columns in the message into two.
    let right = msg.columns.drain(mid..).collect();

    // msg now contains the left-most half of the columns.
    //
    // Construct the frame for the right-most half of the columns.
    let other = TableUpdated {
        columns: right,
        table_name: msg.table_name.clone(),
        namespace_name: msg.namespace_name.clone(),
        table_id: msg.table_id,
    };

    // Recursively split the frames until they fit, at which point they'll
    // be sent within this call.
    serialise_table_update_frames(msg, max_frame_bytes, out)
        && serialise_table_update_frames(other, max_frame_bytes, out)
}

/// Split `msg` into a [`TableCreated`] and a series of [`TableUpdated`] frames,
/// each sized less than [`MAX_USER_PAYLOAD_BYTES`].
fn serialise_table_create_frames(mut msg: TableCreated) -> Vec<Event> {
    // If it fits, do nothing.
    if msg.encoded_len() <= MAX_USER_PAYLOAD_BYTES {
        return vec![Event::TableCreated(msg)];
    }

    // If not, split the message into a single create, followed by N table
    // updates.
    let columns = std::mem::take(&mut msg.table.as_mut().unwrap().columns);

    // Check that on its own, without columns, it'll be send-able.
    if msg.encoded_len() > MAX_USER_PAYLOAD_BYTES {
        error!("dropping oversized table create message");
        return vec![];
    }

    // Recreate the new TableUpdate containing all the columns
    let mut update = msg.table.as_ref().unwrap().clone();
    update.columns = columns;

    let mut updates = Vec::with_capacity(1);
    if !serialise_table_update_frames(update, MAX_USER_PAYLOAD_BYTES, &mut updates) {
        warn!("dropping oversized columns in table update");
        // Continue sending the columns that were packed
        // successfully
    }

    // Return the table creation, followed by the updates containing columns.
    std::iter::once(Event::TableCreated(msg))
        .chain(updates.into_iter().map(Event::TableUpdated))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::{
        gossip::v1::Column,
        partition_template::v1::{template_part::Part, PartitionTemplate, TemplatePart},
    };
    use proptest::prelude::*;

    const TABLE_NAME: &str = "bananas";
    const NAMESPACE_NAME: &str = "platanos";
    const TABLE_ID: i64 = 42;

    proptest! {
        /// Assert that overly-large [`TableUpdated`] frames are correctly split
        /// into multiple smaller frames that are under the provided maximum
        /// size.
        #[test]
        fn prop_table_update_frame_splitting(
            max_frame_bytes in 0_usize..1_000,
            n_columns in 0_i64..1_000,
        ) {
            let mut msg = TableUpdated {
                table_name: TABLE_NAME.to_string(),
                namespace_name: NAMESPACE_NAME.to_string(),
                table_id: TABLE_ID,
                columns: (0..n_columns).map(|v| {
                    Column {
                        name: "ignored".to_string(),
                        column_id: v,
                        column_type: 42,
                    }
                }).collect(),
            };

            let mut out = Vec::new();
            let success = serialise_table_update_frames(
                msg.clone(),
                max_frame_bytes,
                &mut out
            );

            // For all inputs, either:
            //
            //  - The return value indicates a frame could not be split
            //    entirely, in which case at least one column must exceed
            //    max_frame_bytes when packed in an update message, preventing
            //    it from being split down to N=1, and that column is absent
            //    from the output messages.
            //
            // or
            //
            //  - The message may have been split into more than one message of
            //    less-than-or-equal-to max_frame_bytes in size, in which case
            //    all columns must appear exactly once (validated by checking
            //    the column ID values reduce to the input set)
            //
            // All output messages must contain identical table metadata and be
            // non-empty.

            // Otherwise validate the successful case.
            for v in &out {
                // Invariant: the split frames must be less than or equal to the
                // desired maximum encoded frame size
                assert!(v.encoded_len() <= max_frame_bytes);

                // Invariant: all messages must contain the same metadata
                assert_eq!(v.table_name, msg.table_name);
                assert_eq!(v.namespace_name, msg.namespace_name);
                assert_eq!(v.table_id, msg.table_id);

                // Invariant: there should always be at least one column per
                // message (no empty update frames).
                assert!(!v.columns.is_empty());
            }

            let got_ids = into_sorted_vec(out.iter()
                .flat_map(|v| v.columns.iter().map(|v| v.column_id)));

            // Build the set of IDs that should appear.
            let want_ids = if success {
                // All column IDs must appear in the output as the splitting was
                // successful.
                (0..n_columns).collect::<Vec<_>>()
            } else {
                // Splitting failed.
                //
                // Build the set of column IDs expected to be in the output
                // (those that are under the maximum size on their own).
                let cols = std::mem::take(&mut msg.columns);
                let want_ids = into_sorted_vec(cols.into_iter().filter_map(|v| {
                        let column_id = v.column_id;
                        msg.columns = vec![v];
                        if msg.encoded_len() > max_frame_bytes {
                            return None;
                        }
                        Some(column_id)
                    }));

                // Assert at least one column must be too large to be packed
                // into an update containing only that column if one was
                // provided.
                if n_columns != 0 {
                    assert_ne!(want_ids.len(), n_columns as usize);
                }

                want_ids
            };

            // The ordered iterator of observed IDs must match the input
            // interval of [0, n_columns)
            assert!(want_ids.into_iter().eq(got_ids.into_iter()));
        }

        #[test]
        fn prop_table_create_frame_splitting(
            n_columns in 0..MAX_USER_PAYLOAD_BYTES as i64,
            partition_template_size in 0..MAX_USER_PAYLOAD_BYTES
        ) {
            let mut msg = TableCreated {
                table: Some(TableUpdated {
                    table_name: TABLE_NAME.to_string(),
                    namespace_name: NAMESPACE_NAME.to_string(),
                    table_id: TABLE_ID,
                    columns: (0..n_columns).map(|v| {
                        Column {
                            name: "ignored".to_string(),
                            column_id: v,
                            column_type: 42,
                        }
                    }).collect(),
                }),
                // Generate a potentially outrageously big partition template.
                // It's probably invalid, but that's fine for this test.
                partition_template: Some(PartitionTemplate{ parts: vec![
                    TemplatePart{ part: Some(Part::TagValue(
                        "b".repeat(partition_template_size).to_string()
                    ))
                }]}),
            };

            let frames = serialise_table_create_frames(msg.clone());

            // This TableCreated message is in one of three states:
            //
            //   1. Small enough to be sent in a one-er
            //   2. Big enough to need splitting into a create + updates
            //   3. Too big to send the create message at all
            //
            // For 1 and 2, all columns should be observed, and should follow
            // the create message. For 3, nothing should be sent, as those
            // updates are likely to go unused by peers who likely don't know
            // about this table.

            // Validate 3 first.
            if frames.is_empty() {
                // Then it MUST be too large to send even with no columns.
                //
                // Remove them and assert the size.
                msg.table.as_mut().unwrap().columns = vec![];
                assert!(msg.encoded_len() > MAX_USER_PAYLOAD_BYTES);
                return Ok(());
            }

            // Both 1 and 2 require all columns to eventually be observed, as
            // well as the create message.

            let mut iter = frames.into_iter();
            let mut columns = assert_matches!(iter.next(), Some(Event::TableCreated(v)) => {
                // Invariant: table metadata must be correct
                assert_eq!(v.partition_template, msg.partition_template);

                let update = v.table.unwrap();
                assert_eq!(update.table_name, TABLE_NAME);
                assert_eq!(update.namespace_name, NAMESPACE_NAME);
                assert_eq!(update.table_id, TABLE_ID);

                // Return the columns in this create message, if any.
                update.columns
            });

            // Combine the columns from above, with any subsequent update
            // messages
            columns.extend(iter.flat_map(|v| {
                assert_matches!(v, Event::TableUpdated(v) => {
                    // Invariant: metadata in follow-on updates must also match
                    assert_eq!(v.table_name, TABLE_NAME);
                    assert_eq!(v.namespace_name, NAMESPACE_NAME);
                    assert_eq!(v.table_id, TABLE_ID);

                    v.columns
                })
            }));

            // Columns now contains all the columns, across all the output
            // messages.
            let got_ids = into_sorted_vec(columns.into_iter().map(|v| v.column_id));

            // Which should match the full input set of column IDs
            assert!((0..n_columns).eq(got_ids.into_iter()));
        }
    }

    /// Generate a `Vec` of sorted `T`, preserving duplicates, if any.
    fn into_sorted_vec<T>(v: impl IntoIterator<Item = T>) -> Vec<T>
    where
        T: Ord,
    {
        let mut v = v.into_iter().collect::<Vec<_>>();
        v.sort_unstable();
        v
    }
}
