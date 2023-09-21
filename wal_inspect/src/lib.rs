//! # WAL Inspect
//!
//! This crate builds on top of the WAL implementation to provide tools for
//! inspecting individual segment files and translating them to human readable
//! formats.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use std::error::Error;
use std::io::Write;
use std::{borrow::Cow, future::Future};

use data_types::{NamespaceId, TableId};
use hashbrown::{hash_map::Entry, HashMap};
use mutable_batch::MutableBatch;
use parquet_to_line_protocol::convert_to_lines;
use thiserror::Error;

/// Errors emitted by a [`TableBatchWriter`] during operation.
#[derive(Debug, Error)]
pub enum WriteError {
    /// The mutable batch is in a state that prevents obtaining
    /// the data needed to write to the [`TableBatchWriter`]'s
    /// underlying implementation.
    #[error("failed to get required data from mutable batch: {0}")]
    BadMutableBatch(#[from] mutable_batch::Error),

    /// The record batch could not be mapped to line protocol
    #[error("failed to translate record batch: {0}")]
    RecordBatchTranslationFailure(String),

    /// A write failure caused by an IO error
    #[error("failed to write table batch: {0}")]
    IoError(#[from] std::io::Error),
}

/// The [`TableBatchWriter`] trait provides functionality to write table-ID
/// mutable batches to the implementation defined destination and format.
pub trait TableBatchWriter {
    /// The bounds which [`TableBatchWriter`] implementors must adhere to when
    /// returning errors for a failed write.
    type WriteError: Error + Into<WriteError>;

    /// Write out `table_batches` to the implementation defined destination and format.
    fn write_table_batches<B>(&mut self, table_batches: B) -> Result<(), Self::WriteError>
    where
        B: Iterator<Item = (TableId, MutableBatch)>;
}

/// NamespaceDemultiplexer is a delegator from [`NamespaceId`] to some namespaced
/// type, lazily initialising instances as required.
#[derive(Debug)]
pub struct NamespaceDemultiplexer<T, F> {
    // The map used to hold currently initialised `T` and lookup within.
    demux_map: HashMap<NamespaceId, T>,
    // Mechanism to initialise a new `T` when no entry is found in the
    // `demux_map`.
    init_new: F,
}

impl<T, F, I, E> NamespaceDemultiplexer<T, F>
where
    T: Send,
    F: (Fn(NamespaceId) -> I) + Send + Sync,
    I: Future<Output = Result<T, E>> + Send,
{
    /// Creates a [`NamespaceDemultiplexer`] that uses `F` to lazily initialise
    /// instances of `T` when there is no entry in the map for a given [`NamespaceId`].
    pub fn new(init_new: F) -> Self {
        Self {
            demux_map: Default::default(),
            init_new,
        }
    }

    /// Looks up the `T` corresponding to `namespace_id`, initialising a new
    /// instance through the provided mechanism if no entry exists yet.
    pub async fn get(&mut self, namespace_id: NamespaceId) -> Result<&mut T, E> {
        match self.demux_map.entry(namespace_id) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(empty_entry) => {
                let value = (self.init_new)(namespace_id).await?;
                Ok(empty_entry.insert(value))
            }
        }
    }
}

/// The [`LineProtoWriter`] enables rewriting table-keyed mutable batches as
/// line protocol to a [`Write`] implementation.
#[derive(Debug)]
pub struct LineProtoWriter<W>
where
    W: Write,
{
    sink: W,
    table_name_lookup: Option<HashMap<TableId, String>>,
}

impl<W> LineProtoWriter<W>
where
    W: Write,
{
    /// Constructs a new [`LineProtoWriter`] that writes batches of table writes
    /// to `sink` as line protocol.
    ///
    /// If provided, `table_name_lookup` MUST contain an exhaustive mapping from
    /// table ID to corresponding table name so that the correct measurement name
    /// can be placed in the line proto. WAL entries store only table ID, not
    /// name and thus cannot be inferred.
    ///
    /// If no lookup is given then measurement names are written as table IDs.
    pub fn new(sink: W, table_name_lookup: Option<HashMap<TableId, String>>) -> Self {
        Self {
            sink,
            table_name_lookup,
        }
    }
}

impl<W> Drop for LineProtoWriter<W>
where
    W: Write,
{
    fn drop(&mut self) {
        _ = self.sink.flush()
    }
}

impl<W> TableBatchWriter for LineProtoWriter<W>
where
    W: Write,
{
    type WriteError = WriteError;

    /// Writes the provided set of table batches as line protocol write entries
    /// to the destination for the provided namespace ID.
    fn write_table_batches<B>(&mut self, table_batches: B) -> Result<(), Self::WriteError>
    where
        B: Iterator<Item = (TableId, MutableBatch)>,
    {
        for (table_id, mb) in table_batches {
            let schema = mb.schema(schema::Projection::All)?;
            let record_batch = mb.to_arrow(schema::Projection::All)?;
            let measurement_name = match &self.table_name_lookup {
                Some(idx) => Cow::Borrowed(idx.get(&table_id).ok_or(
                    WriteError::RecordBatchTranslationFailure(format!(
                        "missing table name for id {}",
                        &table_id
                    )),
                )?),
                None => Cow::Owned(table_id.to_string()),
            };
            self.sink.write_all(
                convert_to_lines(&measurement_name, &schema, &record_batch)
                    .map_err(WriteError::RecordBatchTranslationFailure)?
                    .as_slice(),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use data_types::TableId;
    use dml::DmlWrite;
    use generated_types::influxdata::{
        iox::wal::v1::sequenced_wal_op::Op, pbdata::v1::DatabaseBatch,
    };
    use mutable_batch_lp::lines_to_batches;
    use wal::{SequencedWalOp, WriteOpEntry, WriteOpEntryDecoder};

    use super::*;

    #[tokio::test]
    async fn translate_valid_wal_segment() {
        let test_dir = test_helpers::tmp_dir().expect("failed to create test dir");
        let wal = wal::Wal::new(test_dir.path()).await.unwrap();

        // Assign table IDs to the measurements and place some writes in the WAL
        let (table_id_index, table_name_index) =
            build_indexes([("m1", TableId::new(1)), ("m2", TableId::new(2))]);
        let line1 = "m1,t=foo v=1i 1";
        let line2 = r#"m2,t=bar v="arán" 1"#;
        let line3 = "m1,t=foo v=2i 2";

        // Generate a single entry
        wal.write_op(SequencedWalOp {
            table_write_sequence_numbers: [(TableId::new(1), 0)].into_iter().collect(),
            op: Op::Write(encode_line(NamespaceId::new(1), &table_id_index, line1)),
        });
        wal.write_op(SequencedWalOp {
            table_write_sequence_numbers: [(TableId::new(2), 1)].into_iter().collect(),
            op: Op::Write(encode_line(NamespaceId::new(2), &table_id_index, line2)),
        });
        wal.write_op(SequencedWalOp {
            table_write_sequence_numbers: [(TableId::new(1), 2)].into_iter().collect(),
            op: Op::Write(encode_line(NamespaceId::new(1), &table_id_index, line3)),
        })
        .changed()
        .await
        .expect("WAL should have changed");

        // Rotate the WAL and create the translator.
        let (closed, _) = wal.rotate().expect("failed to rotate WAL");

        let decoder = WriteOpEntryDecoder::from(
            wal.reader_for_segment(closed.id())
                .expect("failed to open reader for closed segment"),
        );
        let mut namespace_demux = NamespaceDemultiplexer::new(|_namespace_id| async {
            let result: Result<LineProtoWriter<Vec<u8>>, WriteError> = Ok(LineProtoWriter::new(
                Vec::<u8>::new(),
                Some(table_name_index.clone()),
            ));
            result
        });

        let decoded_entries = decoder
            .into_iter()
            .map(|r| r.expect("unexpected bad entry"))
            .collect::<Vec<_>>();
        assert_eq!(decoded_entries.len(), 1);
        let decoded_ops = decoded_entries
            .into_iter()
            .flatten()
            .collect::<Vec<WriteOpEntry>>();
        assert_eq!(decoded_ops.len(), 3);

        for entry in decoded_ops {
            namespace_demux
                .get(entry.namespace)
                .await
                .expect("failed to get namespaced writer")
                .write_table_batches(entry.table_batches.into_iter())
                .expect("should not fail to write table batches");
        }
        // Assert that the namespaced writes contain ONLY the following:
        //
        // NamespaceId 1:
        //
        //     m1,t=foo v=1i 1
        //     m1,t=foo v=2i 2
        //
        // NamespaceId 2:
        //
        //     m2,t=bar v="arán" 1
        //
        assert_eq!(namespace_demux.demux_map.len(), 2);
        let ns_1_results = namespace_demux
            .get(NamespaceId::new(1))
            .await
            .expect("failed to get namespaced writer")
            .sink
            .clone();
        assert_eq!(
            String::from_utf8(ns_1_results).unwrap().as_str(),
            format!("{}\n{}\n", line1, line3)
        );
        let ns_2_results = namespace_demux
            .get(NamespaceId::new(2))
            .await
            .expect("failed to get namespaced writer")
            .sink
            .clone();
        assert_eq!(
            String::from_utf8(ns_2_results).unwrap().as_str(),
            format!("{}\n", line2)
        );
    }

    #[test]
    fn write_line_proto_without_index() {
        let batches = BTreeMap::from_iter(
            lines_to_batches(
                r#"m1,t=foo v=1i 1
m2,t=bar v="arán" 1"#,
                0,
            )
            .expect("failed to create batches from line proto")
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .enumerate()
            .map(|(i, (_table_name, batch))| (TableId::new(i as i64), batch)),
        );

        let mut lp_writer = LineProtoWriter::new(Vec::new(), None);

        lp_writer
            .write_table_batches(batches.into_iter())
            .expect("write back to line proto should succeed");

        assert_eq!(
            String::from_utf8(lp_writer.sink.clone()).expect("invalid output"),
            r#"0,t=foo v=1i 1
1,t=bar v="arán" 1
"#
        );
    }

    fn build_indexes<'a>(
        iter: impl IntoIterator<Item = (&'a str, TableId)>,
    ) -> (HashMap<String, TableId>, HashMap<TableId, String>) {
        let table_id_index: HashMap<String, TableId> =
            HashMap::from_iter(iter.into_iter().map(|(s, id)| (s.to_string(), id)));

        let table_name_index: HashMap<TableId, String> = table_id_index
            .clone()
            .into_iter()
            .map(|(name, id)| (id, name))
            .collect();

        (table_id_index, table_name_index)
    }

    fn encode_line(
        ns: NamespaceId,
        table_id_index: &HashMap<String, TableId>,
        lp: &str,
    ) -> DatabaseBatch {
        let batches = lines_to_batches(lp, 0).unwrap();
        let batches = batches
            .into_iter()
            .map(|(table_name, batch)| {
                (
                    table_id_index
                        .get(&table_name)
                        .expect("table name not present in table id index")
                        .to_owned(),
                    batch,
                )
            })
            .collect();

        let write = DmlWrite::new(ns, batches, "bananas".into(), Default::default());

        mutable_batch_pb::encode::encode_write(ns.get(), &write)
    }
}
