#![no_main]

use hashbrown::HashMap;
use libfuzzer_sys::fuzz_target;
use mutable_batch::{column::ColumnData, MutableBatch, PartitionWrite, WritePayload};
use mutable_batch_lp::LinesConverter;

fuzz_target!(|data: &[u8]| {
    if let Ok(body) = std::str::from_utf8(data) {
        let table_partition_template = Default::default();
        let mut converter = LinesConverter::new(10);
        let errors = match converter.write_lp(body) {
            Ok(_) => vec![],
            Err(mutable_batch_lp::Error::PerLine { lines }) => lines,
            Err(other) => panic!("unexpected error: `{other}` input: `{body}`"),
        };

        if let Ok((batches, stats)) = converter.finish() {
            let mut total_rows = 0;

            let mut partitions: HashMap<_, HashMap<String, MutableBatch>> =
                HashMap::default();

            for (table_name, mutable_batch) in &batches {
                assert!(
                    mutable_batch.column("time").is_ok(),
                    "batch for table `{table_name}` does not have a time column: \
                    {mutable_batch:#?}\ninput: `{body}`\nerrors: `{errors:#?}`"
                );

                let data = mutable_batch.column("time").unwrap().data();
                assert!(
                    matches!(data, ColumnData::I64(_, _)),
                    "expected the time column to be I64, instead got `{data:?}`.\ninput: `{body}`"
                );

                for (partition_key, partition_payload) in
                    PartitionWrite::partition(&mutable_batch, &table_partition_template).unwrap()
                {
                    let partition = partitions.entry(partition_key).or_default();

                    let mut table_batch = partition
                        .raw_entry_mut()
                        .from_key(table_name.as_str())
                        .or_insert_with(|| (table_name.to_owned(), MutableBatch::default()));
                    partition_payload
                        .write_to_batch(&mut table_batch.1)
                        .unwrap();
                }

                total_rows += mutable_batch.rows();
            }

            for (_partition_key, table_batches) in partitions {
                for (_table_name, batch) in table_batches {
                    assert_ne!(batch.rows(), 0);
                }
            }

            assert_eq!(
                stats.num_lines, total_rows,
                "batches: {batches:#?}\ninput: `{body}`\nerrors: `{errors:#?}`"
            );
        }
    }
});
