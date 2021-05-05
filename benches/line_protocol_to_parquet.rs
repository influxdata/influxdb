use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use influxdb_line_protocol::parse_lines;
use ingest::{
    parquet::writer::{CompressionLevel, IOxParquetTableWriter},
    ConversionSettings, LineProtocolConverter,
};
use internal_types::schema::Schema;
use packers::{Error as TableError, IOxTableWriter, IOxTableWriterSource};
use std::time::Duration;

use parquet::file::writer::TryClone;
use std::io::{Seek, SeekFrom, Write};

static LINES: &str = include_str!("../tests/fixtures/lineproto/metrics.lp");

/// Stream that throws away all output
struct IgnoringWriteStream {}

impl Write for IgnoringWriteStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for IgnoringWriteStream {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        Ok(0)
    }
}

impl TryClone for IgnoringWriteStream {
    fn try_clone(&self) -> std::result::Result<Self, std::io::Error> {
        Ok(IgnoringWriteStream {})
    }
}

#[derive(Debug)]
/// Creates parquet writers that write to /dev/null
struct IgnoringParquetDirectoryWriterSource {}

impl IOxTableWriterSource for IgnoringParquetDirectoryWriterSource {
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn IOxTableWriter>, TableError> {
        let dev_null = IgnoringWriteStream {};
        let writer = IOxParquetTableWriter::new(schema, CompressionLevel::Compatibility, dev_null)
            .expect("Creating table writer");
        Ok(Box::new(writer))
    }
}

fn line_parser(c: &mut Criterion) {
    let mut group = c.benchmark_group("line_protocol_to_parquet");

    // Expand dataset by 10x to amortize writer setup and column writer overhead
    const NUM_COPIES: usize = 10;
    let mut input = String::with_capacity(NUM_COPIES * (LINES.len() + 1));
    for _ in 0..NUM_COPIES {
        input.push_str(LINES);
        input.push('\n');
    }

    group.throughput(Throughput::Bytes(input.len() as u64));
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(20);

    group.bench_function("all lines", |b| {
        b.iter(|| {
            let settings = ConversionSettings::default();
            let only_good_lines = parse_lines(&input).map(|r| r.expect("Unexpected parse error"));

            let writer_source = Box::new(IgnoringParquetDirectoryWriterSource {});

            let mut converter = LineProtocolConverter::new(settings, writer_source);
            converter
                .convert(only_good_lines)
                .expect("Converting all lines");
            converter.finalize().expect("Finalizing writer");
        })
    });

    group.finish();
}

criterion_group!(benches, line_parser);

criterion_main!(benches);
