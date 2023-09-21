# influxdb_line_protocol

<!-- cargo-rdme start -->

This crate contains pure Rust implementations of

1. A [parser](https://docs.rs/influxdb_line_protocol/latest/influxdb_line_protocol/fn.parse_lines.html) for [InfluxDB Line Protocol] developed as part of the
[InfluxDB IOx] project.  This implementation is intended to be
compatible with the [Go implementation], however, this
implementation uses a [nom] combinator-based parser rather than
attempting to port the imperative Go logic so there are likely
some small diferences.

2. A [builder](https://docs.rs/influxdb_line_protocol/latest/influxdb_line_protocol/builder/struct.LineProtocolBuilder.html) to contruct valid [InfluxDB Line Protocol]

## Example

Here is an example of how to parse the following line
protocol data into a `ParsedLine`:

```text
cpu,host=A,region=west usage_system=64.2 1590488773254420000
```

```rust
use influxdb_line_protocol::{ParsedLine, FieldValue};

let mut parsed_lines =
    influxdb_line_protocol::parse_lines(
        "cpu,host=A,region=west usage_system=64i 1590488773254420000"
    );
let parsed_line = parsed_lines
    .next()
    .expect("Should have at least one line")
    .expect("Should parse successfully");

let ParsedLine {
    series,
    field_set,
    timestamp,
} = parsed_line;

assert_eq!(series.measurement, "cpu");

let tags = series.tag_set.unwrap();
assert_eq!(tags[0].0, "host");
assert_eq!(tags[0].1, "A");
assert_eq!(tags[1].0, "region");
assert_eq!(tags[1].1, "west");

let field = &field_set[0];
assert_eq!(field.0, "usage_system");
assert_eq!(field.1, FieldValue::I64(64));

assert_eq!(timestamp, Some(1590488773254420000));
```

[InfluxDB Line Protocol]: https://v2.docs.influxdata.com/v2.0/reference/syntax/line-protocol
[Go implementation]: https://github.com/influxdata/influxdb/blob/217eddc87e14a79b01d0c22994fc139f530094a2/models/points_parser.go
[InfluxDB IOx]: https://github.com/influxdata/influxdb_iox
[nom]: https://crates.io/crates/nom

<!-- cargo-rdme end -->
