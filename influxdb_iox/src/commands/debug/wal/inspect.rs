//! A module providing a CLI command to inspect the contents of a WAL file.
use std::{io::Write, ops::RangeInclusive, path::PathBuf};

use itertools::Itertools;
use wal::SequencedWalOp;

use super::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    /// The path to the input WAL file
    #[clap(value_parser)]
    input: PathBuf,

    /// An optional range of sequence numbers to restrict the inspection to, in
    /// the format "%d-%d". Only entries that have a sequence number falling
    /// within the range (inclusive) will be displayed
    #[clap(long, short, value_parser = parse_sequence_number_range)]
    sequence_number_range: Option<RangeInclusive<u64>>,
}

fn parse_sequence_number_range(s: &str) -> Result<RangeInclusive<u64>, String> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 2 {
        return Err("sequence number range provided does not use format <START>-<END>".to_string());
    }

    let min = parts[0]
        .parse()
        .map_err(|_| format!("{} isn't a valid sequence number", parts[0]))?;
    let max = parts[1]
        .parse()
        .map_err(|_| format!("{} isn't a valid sequence number", parts[1]))?;
    if max < min {
        Err(format!(
            "invalid sequence number range provided, {max} is less than {min}"
        ))
    } else {
        Ok(RangeInclusive::new(min, max))
    }
}

pub fn command(config: Config) -> Result<(), Error> {
    let reader = wal::ClosedSegmentFileReader::from_path(&config.input)
        .map_err(Error::UnableToReadWalFile)?;

    inspect(config.sequence_number_range, &mut std::io::stdout(), reader)
}

fn inspect<W, R>(
    sequence_number_range: Option<RangeInclusive<u64>>,
    output: &mut W,
    reader: R,
) -> Result<(), Error>
where
    W: Write,
    R: Iterator<Item = Result<Vec<SequencedWalOp>, wal::Error>>,
{
    let mut inspect_errors = Vec::<wal::Error>::new();

    let formatter = reader
        .flatten_ok()
        .filter_ok(|op| {
            sequence_number_range.as_ref().map_or(true, |range| {
                op.table_write_sequence_numbers
                    .values()
                    .any(|seq| range.contains(seq))
            })
        })
        .format_with(",\n", |op, f| match op {
            Ok(op) => f(&format_args!("{:#?}", op)),
            Err(e) => {
                let err_string = e.to_string();
                inspect_errors.push(e);
                f(&err_string)
            }
        });

    let result = writeln!(output, "{}", formatter);

    if inspect_errors.is_empty() {
        result.map_err(Error::IoFailure)
    } else {
        Err(Error::IncompleteInspection {
            sources: inspect_errors,
        })
    }
}

#[cfg(test)]
mod tests {
    use data_types::TableId;
    use generated_types::influxdata::iox::wal::v1::sequenced_wal_op::Op as WalOp;
    use proptest::{prelude::*, prop_assert};

    use super::*;

    fn arbitrary_sequence_wal_op(seq_number: u64) -> SequencedWalOp {
        SequencedWalOp {
            table_write_sequence_numbers: [(TableId::new(0), seq_number)].into(),
            op: WalOp::Write(Default::default()),
        }
    }

    #[test]
    fn test_range_filters_operations() {
        let mut sink = Vec::<u8>::new();

        inspect(
            Some(RangeInclusive::new(2, 3)),
            &mut sink,
            [
                Ok(vec![
                    arbitrary_sequence_wal_op(1),
                    arbitrary_sequence_wal_op(2),
                ]),
                Ok(vec![
                    arbitrary_sequence_wal_op(3),
                    arbitrary_sequence_wal_op(4),
                    arbitrary_sequence_wal_op(5),
                ]),
            ]
            .into_iter(),
        )
        .expect("should inspect entries given without error");

        let results = String::from_utf8(sink).expect("failed to recover string from write sink");

        // Expect two operations inspected, with the appropriate sequence numbers
        assert_eq!(results.matches("SequencedWalOp").count(), 2);

        // Strip the whitespace before checking the output
        let results: String = results.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(
            results
                .matches("table_write_sequence_numbers:{TableId(0,):2,},")
                .count(),
            1
        );
        assert_eq!(
            results
                .matches("table_write_sequence_numbers:{TableId(0,):3,},")
                .count(),
            1
        );
    }

    proptest! {
        #[test]
        fn test_sequence_number_range_parsing(a in any::<u64>(), b in any::<u64>()) {
            let input = format!("{}-{}", a, b);

            match parse_sequence_number_range(input.as_str()) {
                Ok(_) => prop_assert!(a <= b),
                Err(_) => prop_assert!(a > b),
            }
        }
    }
}
