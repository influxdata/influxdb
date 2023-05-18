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
    missing_docs
)]
use std::borrow::Cow;
use std::io::Write;

use data_types::{NamespaceId, TableId};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;
use parquet_to_line_protocol::convert_to_lines;
use thiserror::Error;

/// Errors emitted by a [`LineProtoWriter`] during operation.
#[derive(Debug, Error)]
pub enum WriteError {
    /// The mutable batch is in a state that prevents obtaining
    /// the data needed to write line protocol
    #[error("failed to get required data from mutable batch: {0}")]
    BadMutableBatch(#[from] mutable_batch::Error),

    /// The record batch could not be mapped to line protocol
    #[error("failed to map record batch to line protocol: {0}")]
    ConvertToLineProtocolFailed(String),

    /// A write failure caused by an IO error
    #[error("failed to write translation: {0}")]
    IoError(#[from] std::io::Error),
}

/// A [`NamespacedBatchWriter`] takes a namespace and a set of associated table
/// batch writes and writes them elsewhere.
pub trait NamespacedBatchWriter {
    /// Writes out each table batch to a destination associated with the given
    /// namespace ID.
    fn write_namespaced_table_batches(
        &mut self,
        ns: NamespaceId,
        table_batches: HashMap<i64, MutableBatch>,
    ) -> Result<(), WriteError>;
}

/// Provides namespaced write functionality from table-based mutable batches
/// to namespaced line protocol output.
#[derive(Debug)]
pub struct LineProtoWriter<W, F>
where
    W: Write,
{
    namespaced_output: HashMap<NamespaceId, W>,
    new_write_sink: F,

    table_name_index: Option<HashMap<TableId, String>>,
}

impl<W, F> LineProtoWriter<W, F>
where
    W: Write,
{
    /// Performs a best effort flush of all write destinations opened by the [`LineProtoWriter`].
    pub fn flush(&mut self) -> Result<(), Vec<WriteError>> {
        let mut errs = Vec::<WriteError>::new();
        for w in self.namespaced_output.values_mut() {
            if let Err(e) = w.flush() {
                errs.push(WriteError::IoError(e));
            }
        }
        if !errs.is_empty() {
            return Err(errs);
        }
        Ok(())
    }
}

impl<W, F> Drop for LineProtoWriter<W, F>
where
    W: Write,
{
    fn drop(&mut self) {
        _ = self.flush()
    }
}

impl<W, F> LineProtoWriter<W, F>
where
    W: Write,
    F: Fn(NamespaceId) -> Result<W, WriteError>,
{
    /// Constructs a new [`LineProtoWriter`] that uses `new_write_sink` to
    /// get the destination for each line protocol write by its namespace ID.
    ///
    /// The optional `table_name_index` is used to provide a mapping for all table IDs
    /// to the corresponding table name to recover the measurement name, as WAL write
    /// entries do not contain this information. If supplied, the index MUST be exhaustive.
    ///
    /// If no index is given then measurement names are written as table IDs.
    pub fn new(new_write_sink: F, table_name_index: Option<HashMap<TableId, String>>) -> Self {
        Self {
            namespaced_output: HashMap::new(),
            new_write_sink,
            table_name_index,
        }
    }
}

impl<W, F> NamespacedBatchWriter for LineProtoWriter<W, F>
where
    W: Write,
    F: Fn(NamespaceId) -> Result<W, WriteError>,
{
    /// Writes the provided set of table batches as line protocol write entries
    /// to the destination for the provided namespace ID.
    fn write_namespaced_table_batches(
        &mut self,
        ns: NamespaceId,
        table_batches: HashMap<i64, MutableBatch>,
    ) -> Result<(), WriteError> {
        let sink = self
            .namespaced_output
            .entry(ns)
            .or_insert((self.new_write_sink)(ns)?);

        write_batches_as_line_proto(
            sink,
            self.table_name_index.as_ref(),
            table_batches.into_iter(),
        )
    }
}

fn write_batches_as_line_proto<W, B>(
    sink: &mut W,
    table_name_index: Option<&HashMap<TableId, String>>,
    table_batches: B,
) -> Result<(), WriteError>
where
    W: Write,
    B: Iterator<Item = (i64, MutableBatch)>,
{
    for (table_id, mb) in table_batches {
        let schema = mb.schema(schema::Projection::All)?;
        let record_batch = mb.to_arrow(schema::Projection::All)?;
        let table_id = TableId::new(table_id);
        let measurement_name = match table_name_index {
            Some(idx) => Cow::Borrowed(idx.get(&table_id).ok_or(
                WriteError::ConvertToLineProtocolFailed(format!(
                    "missing table name for id {}",
                    &table_id
                )),
            )?),
            None => Cow::Owned(table_id.to_string()),
        };
        sink.write_all(
            convert_to_lines(&measurement_name, &schema, &record_batch)
                .map_err(WriteError::ConvertToLineProtocolFailed)?
                .as_slice(),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs::{read_dir, OpenOptions};

    use assert_matches::assert_matches;
    use data_types::TableId;
    use dml::DmlWrite;
    use generated_types::influxdata::{
        iox::wal::v1::sequenced_wal_op::Op, pbdata::v1::DatabaseBatch,
    };
    use mutable_batch_lp::lines_to_batches;
    use wal::{SequencedWalOp, WriteOpEntry, WriteOpEntryDecoder};

    use super::*;

    #[tokio::test]
    async fn translate_good_wal_segment_file() {
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
            sequence_number: 0,
            op: Op::Write(encode_line(NamespaceId::new(1), &table_id_index, line1)),
        });
        wal.write_op(SequencedWalOp {
            sequence_number: 1,
            op: Op::Write(encode_line(NamespaceId::new(2), &table_id_index, line2)),
        });
        wal.write_op(SequencedWalOp {
            sequence_number: 2,
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
        let mut writer = LineProtoWriter::new(|_| Ok(Vec::<u8>::new()), Some(table_name_index));

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
            writer
                .write_namespaced_table_batches(entry.namespace, entry.table_batches)
                .expect("batch write should not fail");
        }
        // The WAL has been given a single entry containing three write ops

        let results = &writer.namespaced_output;

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
        assert_eq!(results.len(), 2);
        assert_matches!(results.get(&NamespaceId::new(1)), Some(e) => {
            assert_eq!(
                String::from_utf8(e.to_owned()).unwrap().as_str(), format!("{}\n{}\n", line1, line3));
        });
        assert_matches!(results.get(&NamespaceId::new(2)), Some(e) => {
            assert_eq!(
                String::from_utf8(e.to_owned()).unwrap().as_str(), format!("{}\n", line2));
        });
    }

    #[tokio::test]
    async fn partial_translate_bad_wal_segment_file() {
        let test_dir = test_helpers::tmp_dir().expect("failed to create test dir");
        let wal = wal::Wal::new(test_dir.path()).await.unwrap();

        let (table_id_index, table_name_index) =
            build_indexes([("m3", TableId::new(3)), ("m4", TableId::new(4))]);
        let line1 = "m3,s=baz v=3i 1";
        let line2 = "m3,s=baz v=2i 2";
        let line3 = "m4,s=qux v=2i 3";
        let line4 = "m4,s=qux v=5i 4";

        // Generate some WAL entries
        wal.write_op(SequencedWalOp {
            sequence_number: 0,
            op: Op::Write(encode_line(NamespaceId::new(3), &table_id_index, line1)),
        });
        wal.write_op(SequencedWalOp {
            sequence_number: 1,
            op: Op::Write(encode_line(NamespaceId::new(3), &table_id_index, line2)),
        })
        .changed()
        .await
        .expect("WAL should have changed");
        wal.write_op(SequencedWalOp {
            sequence_number: 2,
            op: Op::Write(encode_line(NamespaceId::new(4), &table_id_index, line3)),
        });
        wal.write_op(SequencedWalOp {
            sequence_number: 3,
            op: Op::Write(encode_line(NamespaceId::new(4), &table_id_index, line4)),
        })
        .changed()
        .await
        .expect("WAL should have changed");

        // Get the path of the only segment file, then rotate it and add some
        // garbage to the end.
        let mut reader = read_dir(test_dir.path()).unwrap();
        let closed_path = reader
            .next()
            .expect("no segment file found in WAL dir")
            .unwrap()
            .path();
        assert_matches!(reader.next(), None); // Only 1 file should be in the WAL dir prior to rotation

        let (closed, _) = wal.rotate().expect("failed to rotate WAL");

        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(closed_path)
                .expect("unable to open closed WAL segment for writing");
            file.write_all(b"bananananananananas").unwrap();
        }

        // Create the translator and read as much as possible out of the bad segment file
        let decoder = WriteOpEntryDecoder::from(
            wal.reader_for_segment(closed.id())
                .expect("failed to open reader for closed segment"),
        );
        let mut writer = LineProtoWriter::new(|_| Ok(Vec::<u8>::new()), Some(table_name_index));

        // The translator should be able to read all 2 good entries containing 4 write ops
        let decoded_entries = decoder
            .into_iter()
            .map_while(|r| r.ok())
            .collect::<Vec<_>>();
        assert_eq!(decoded_entries.len(), 2);
        let decoded_ops = decoded_entries
            .into_iter()
            .flatten()
            .collect::<Vec<WriteOpEntry>>();
        assert_eq!(decoded_ops.len(), 4);
        for entry in decoded_ops {
            writer
                .write_namespaced_table_batches(entry.namespace, entry.table_batches)
                .expect("batch write should not fail");
        }

        let results = &writer.namespaced_output;

        // Assert that the namespaced writes contain ONLY the following:
        //
        // NamespaceId 3:
        //
        //     m3,s=baz v=3i 1
        //     m3,s=baz v=2i 2
        //
        // NamespaceId 4:
        //
        //     m4,s=qux v=2i 3
        //     m4,s=qux v=5i 4
        //
        assert_eq!(results.len(), 2);
        assert_matches!(results.get(&NamespaceId::new(3)), Some(e) => {
            assert_eq!(
                String::from_utf8(e.to_owned()).unwrap().as_str(), format!("{}\n{}\n", line1, line2));
        });
        assert_matches!(results.get(&NamespaceId::new(4)), Some(e) => {
            assert_eq!(
                String::from_utf8(e.to_owned()).unwrap().as_str(), format!("{}\n{}\n", line3, line4));
        });
    }

    #[test]
    fn write_line_proto_without_index() {
        let mut sink = Vec::<u8>::new();
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
            .map(|(i, (_table_name, batch))| (i as i64, batch)),
        );

        write_batches_as_line_proto(&mut sink, None, batches.into_iter())
            .expect("write back to line proto should succeed");

        assert_eq!(
            String::from_utf8(sink).expect("invalid output"),
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
