//! The setups available for any `TestCase` to use by specifying the test name in a comment at the
//! start of the `.sql` file in the form of:
//!
//! ```text
//! -- IOX_SETUP: [test name]
//! ```

use iox_time::{SystemProvider, Time, TimeProvider};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use test_helpers_end_to_end::Step;

/// The string value that will appear in `.sql` files.
pub type SetupName = &'static str;
/// The steps that should be run when this setup is chosen.
pub type SetupSteps = Vec<Step>;

/// timestamps for the retention test
static RETENTION_SETUP: Lazy<RetentionSetup> = Lazy::new(RetentionSetup::new);

/// All possible setups for the [`TestCase`][crate::TestCase]s to use, indexed by name
pub static SETUPS: Lazy<HashMap<SetupName, SetupSteps>> = Lazy::new(|| {
    HashMap::from([
        (
            "TwoMeasurements",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2 100",
                        "cpu,region=west user=21.0 150",
                        "disk,region=east bytes=99i 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "TwoChunksDedupWeirdnessParquet",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag=A foo=1,bar=1 0".into()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(["table,tag=A bar=2 0", "table,tag=B foo=1 0"].join("\n")),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoChunksDedupWeirdnessParquetIngester",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag=A foo=1,bar=1 0".into()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::WriteLineProtocol(["table,tag=A bar=2 0", "table,tag=B foo=1 0"].join("\n")),
            ],
        ),
        (
            "OneMeasurementFourChunksWithDuplicatesWithIngester",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1:
                //  . time range: 50-250
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston min_temp=70.4 50",
                        "h2o,state=MA,city=Bedford min_temp=71.59 150",
                        "h2o,state=MA,city=Boston max_temp=75.4 250",
                        "h2o,state=MA,city=Andover max_temp=69.2, 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 2: overlaps with chunk 1
                //  . time range: 150 - 300
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        // new field (area) and update available NULL (max_temp)
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150",
                        "h2o,state=MA,city=Boston min_temp=65.4 250", // update min_temp from NULL
                        "h2o,state=MA,city=Reading min_temp=53.4, 250",
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 3: no overlap
                //  . time range: 400 - 500
                //  . duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
                        "h2o,state=MA,city=Boston min_temp=68.4 400",
                        "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
                        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
                        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                // Chunk 4: no overlap
                //  . time range: 600 - 700
                //  . no duplicates
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
                        "h2o,state=MA,city=Boston min_temp=67.4 600",
                        "h2o,state=MA,city=Reading min_temp=60.4, 600",
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
                    ]
                    .join("\n"),
                ),
            ],
        ),
        (
            "OneMeasurementFourChunksWithDuplicatesParquetOnly",
            vec![
                Step::RecordNumParquetFiles,
                // Chunk 1:
                //  . time range: 50-250
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston min_temp=70.4 50",
                        "h2o,state=MA,city=Bedford min_temp=71.59 150",
                        "h2o,state=MA,city=Boston max_temp=75.4 250",
                        "h2o,state=MA,city=Andover max_temp=69.2, 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 2: overlaps with chunk 1
                //  . time range: 150 - 300
                //  . no duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        // new field (area) and update available NULL (max_temp)
                        "h2o,state=MA,city=Bedford max_temp=78.75,area=742u 150",
                        "h2o,state=MA,city=Boston min_temp=65.4 250", // update min_temp from NULL
                        "h2o,state=MA,city=Reading min_temp=53.4, 250",
                        "h2o,state=CA,city=SF min_temp=79.0,max_temp=87.2,area=500u 300",
                        "h2o,state=CA,city=SJ min_temp=78.5,max_temp=88.0 300",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 3: no overlap
                //  . time range: 400 - 500
                //  . duplicates in its own chunk
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=80.75,area=742u 400",
                        "h2o,state=MA,city=Boston min_temp=68.4 400",
                        "h2o,state=MA,city=Bedford min_temp=65.22,area=750u 400", // duplicate
                        "h2o,state=MA,city=Boston min_temp=65.40,max_temp=82.67 400", // duplicate
                        "h2o,state=CA,city=SJ min_temp=77.0,max_temp=90.7 450",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=88.2 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // Chunk 4: no overlap
                //  . time range: 600 - 700
                //  . no duplicates
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Bedford max_temp=88.75,area=742u 600",
                        "h2o,state=MA,city=Boston min_temp=67.4 600",
                        "h2o,state=MA,city=Reading min_temp=60.4, 600",
                        "h2o,state=CA,city=SF min_temp=68.4,max_temp=85.7,area=500u 650",
                        "h2o,state=CA,city=SJ min_temp=69.5,max_temp=89.2 650",
                        "h2o,state=CA,city=SJ min_temp=75.5,max_temp=84.08 700",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwentySortedParquetFiles",
            (0..20)
                .flat_map(|i| {
                    let write = if i % 2 == 0 {
                        Step::WriteLineProtocol(format!(
                            "m,tag=A f=1 {}\nm,tab=B f=2 {}",
                            1000 - i, // unique in this chunk
                            1000 - i, // unique in this chunk (not plus i!)
                        ))
                    } else {
                        Step::WriteLineProtocol(
                            "m,tag=A f=3 2001".into(), // duplicated across all chunks
                        )
                    };
                    [
                        Step::RecordNumParquetFiles,
                        write,
                        Step::Persist,
                        Step::WaitForPersisted2 {
                            expected_increase: 1,
                        },
                    ]
                    .into_iter()
                })
                .collect::<Vec<_>>(),
        ),
        (
            "OneMeasurementManyFields",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,tag1=foo,tag2=bar field1=70.6,field3=2 100",
                        "h2o,tag1=foo,tag2=bar field1=70.4,field2=\"ss\" 100",
                        "h2o,tag1=foo,tag2=bar field1=70.5,field2=\"ss\" 100",
                        "h2o,tag1=foo,tag2=bar field1=70.6,field4=true 1000",
                        "h2o,tag1=foo,tag2=bar field1=70.3,field5=false 3000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFields",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                        "h2o,state=CA,city=Boston other_temp=72.4 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    "h2o,state=MA,city=Boston temp=70.4,moisture=43.0 100000".into(),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFieldsTwoChunks",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=Boston other_temp=72.4 150",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                // The system tables test looks for queries, so the setup needs to run this query.
                Step::Query {
                    sql: "SELECT 1;".into(),
                    expected: vec![
                        "+----------+",
                        "| Int64(1) |",
                        "+----------+",
                        "| 1        |",
                        "+----------+",
                    ],
                },
            ],
        ),
        (
            "TwoMeasurementsManyNulls",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=LA,county=LA temp=70.4 100",
                        "h2o,state=MA,city=Boston,county=Suffolk temp=72.4 250",
                        "o2,state=MA,city=Boston temp=50.4 200",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "o2,state=NY temp=60.8 400",
                        "o2,state=NY,city=NYC temp=61.0 500",
                        "o2,state=NY,city=NYC,borough=Brooklyn temp=61.0 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementWithTags",
            vec![Step::WriteLineProtocol(
                [
                    "cpu,foo=me bar=1 10",
                    "cpu,foo=you bar=2 20",
                    "cpu,foo=me bar=1 30",
                    "cpu,foo=me bar=1 40",
                ]
                .join("\n"),
            )],
        ),
        (
            "PeriodsInNames",
            vec![Step::WriteLineProtocol(
                [
                    "measurement.one,tag.one=value,tag.two=other field.one=1.0,field.two=t \
                    1609459201000000001",
                    "measurement.one,tag.one=value2,tag.two=other2 field.one=1.0,field.two=f \
                    1609459201000000002",
                ]
                .join("\n"),
            )],
        ),
        (
            "TwoMeasurementsPredicatePushDown",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "restaurant,town=andover count=40000u,system=5.0 100",
                        "restaurant,town=reading count=632u,system=5.0 120",
                        "restaurant,town=bedford count=189u,system=7.0 110",
                        "restaurant,town=tewsbury count=471u,system=6.0 110",
                        "restaurant,town=lexington count=372u,system=5.0 100",
                        "restaurant,town=lawrence count=872u,system=6.0 110",
                        "restaurant,town=reading count=632u,system=6.0 130",
                    ]
                    .join("\n"),
                ),
                Step::WriteLineProtocol(
                    [
                        "school,town=reading count=17u,system=6.0 150",
                        "school,town=andover count=25u,system=6.0 160",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "AllTypes",
            vec![Step::WriteLineProtocol(
                [
                    "m,tag=row1 float_field=64.0 450",
                    "m,tag=row1 int_field=64 550",
                    "m,tag=row1 \
                        float_field=61.0,int_field=22,uint_field=25u,\
                        string_field=\"foo\",bool_field=t 500",
                    "m,tag=row1 \
                        float_field=62.0,int_field=21,uint_field=30u,\
                        string_field=\"ba\",bool_field=f 200",
                    "m,tag=row1 \
                        float_field=63.0,int_field=20,uint_field=35u,\
                        string_field=\"baz\",bool_field=f 300",
                    "m,tag=row1 \
                        float_field=64.0,int_field=19,uint_field=20u,\
                        string_field=\"bar\",bool_field=t 400",
                    "m,tag=row1 \
                        float_field=65.0,int_field=18,uint_field=40u,\
                        string_field=\"fruz\",bool_field=f 100",
                    "m,tag=row1 \
                        float_field=66.0,int_field=17,uint_field=10u,\
                        string_field=\"faa\",bool_field=t 600",
                ]
                .join("\n"),
            )],
        ),
        (
            "ManyFieldsSeveralChunks",
            vec![
                Step::RecordNumParquetFiles,
                // c1: parquet stage
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        // duplicate with a row in c4 and will be removed
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c2: parquet stage & overlaps with c1
                Step::WriteLineProtocol("h2o,state=CA,city=Andover other_temp=72.4 150".into()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c3: parquet stage & doesn't overlap with any
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=80.7 350",
                        "h2o,state=MA,city=Boston other_temp=68.2 450",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                // c4: parquet stage & overlap with c1
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=88.6 230",
                        // duplicate with a row in c1 but more
                        // recent => this row is kept
                        "h2o,state=MA,city=Boston other_temp=80 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                // c5: ingester stage & doesn't overlap with any
                Step::WriteLineProtocol("h2o,state=CA,city=Andover temp=67.3 500".into()),
            ],
        ),
        (
            "MeasurementWithMaxTime",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(format!(
                    "cpu,host=server01 value=100 {}",
                    // This is the maximum timestamp that can be represented in the InfluxDB data
                    // model:
                    // <https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34>
                    i64::MAX - 1, // 9223372036854775806
                )),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementRealisticTimes",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2 1626809330000000000",
                        "cpu,region=west user=21.0 1626809430000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementTwoSeries",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=a user=23.2 957529200000000000", // 2000-05-05T12:20:00Z
                        "cpu,region=a user=21.0 957530400000000000", // 2000-05-05T12:40:00Z
                        "cpu,region=b user=25.2 957529860000000000", // 2000-05-05T12:31:00Z
                        "cpu,region=b user=28.9 957530340000000000", // 2000-05-05T12:39:00Z
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoChunksMissingColumns",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag1=a,tag2=b field1=10,field2=11 100".into()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol("table,tag1=a,tag3=c field1=20,field3=22 200".into()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsMultiSeries",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // Data is deliberately not in series order.
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=CA,city=LA temp=90.0 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 250",
                        "o2,state=MA,city=Boston temp=50.4,reading=50 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
        (
            // This recreates the test case for <https://github.com/influxdata/idpe/issues/16238>.
            "StringFieldWithNumericValue",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["m,tag0=foo fld=\"200\" 1000", "m,tag0=foo fld=\"404\" 1050"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementStatusCode",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "status_code,url=http://www.example.com value=404 1527018806000000000",
                        "status_code,url=https://influxdb.com value=418 1527018816000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsMultiTagValue",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Lowell temp=75.4 100",
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "o2,state=MA,city=Boston temp=50.4,reading=50 100",
                        "o2,state=KS,city=Topeka temp=60.4,reading=60 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "MeasurementsSortableTags",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,zz_tag=A,state=MA,city=Kingston temp=70.1 800",
                        "h2o,state=MA,city=Kingston,zz_tag=B temp=70.2 100",
                        "h2o,state=CA,city=Boston temp=70.3 250",
                        "h2o,state=MA,city=Boston,zz_tag=A temp=70.4 1000",
                        "h2o,state=MA,city=Boston temp=70.5,other=5.0 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // See issue: https://github.com/influxdata/influxdb_iox/issues/2845
            "MeasurementsForDefect2845",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=host.local load1=1.83 1527018806000000000",
                        "system,host=host.local load1=1.63 1527018816000000000",
                        "system,host=host.local load3=1.72 1527018806000000000",
                        "system,host=host.local load4=1.77 1527018806000000000",
                        "system,host=host.local load4=1.78 1527018816000000000",
                        "system,host=host.local load4=1.77 1527018826000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "EndToEndTest",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu_load_short,host=server01,region=us-west value=0.64 0000",
                        "cpu_load_short,host=server01 value=27.99 1000",
                        "cpu_load_short,host=server02,region=us-west value=3.89 2000",
                        "cpu_load_short,host=server01,region=us-east value=1234567.891011 3000",
                        "cpu_load_short,host=server01,region=us-west value=0.000003 4000",
                        "system,host=server03 uptime=1303385 5000",
                        "swap,host=server01,name=disk0 in=3,out=4 6000",
                        "status active=t 7000",
                        "attributes color=\"blue\" 8000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 5,
                },
            ],
        ),
        (
            "OneMeasurementNoTags",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o temp=70.4 100",
                        "h2o temp=72.4 250",
                        "h2o temp=50.4 200",
                        "h2o level=200.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementNoTags2",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(["m0 foo=1.0 1", "m0 foo=2.0 2"].join("\n")),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "OneMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=CA,city=LA temp=90.0 200",
                        "h2o,state=CA,city=LA temp=90.0 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 100",
                        "h2o,state=MA,city=Boston temp=72.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "o2,state=CA,city=LA temp=90.0 200",
                        "o2,state=CA,city=LA temp=90.0 350",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "AnotherMeasurementForAggs",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=80 50",
                        "h2o,state=MA,city=Cambridge temp=81 100",
                        "h2o,state=MA,city=Cambridge temp=82 200",
                        "h2o,state=MA,city=Boston temp=70 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=71 400",
                        "h2o,state=CA,city=LA temp=90,humidity=10 500",
                        "h2o,state=CA,city=LA temp=91,humidity=11 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForSelectors",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"d\" 1000"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"c\" 2000",
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=false,s=\"b\" 3000",
                        "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"a\" 4000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForMin",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=false,s=\"c\" 1000",
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=true,s=\"a\" 2000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"z\" 3000",
                        "h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"c\" 4000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForMax",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge f=8.0,i=8i,b=true,s=\"c\" 1000",
                        "h2o,state=MA,city=Cambridge f=7.0,i=7i,b=false,s=\"d\" 2000",
                        "h2o,state=MA,city=Cambridge f=6.0,i=6i,b=true,s=\"a\" 3000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    ["h2o,state=MA,city=Cambridge f=5.0,i=5i,b=false,s=\"z\" 4000"].join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForGroupKeys",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=80 50",
                        "h2o,state=MA,city=Cambridge temp=81 100",
                        "h2o,state=MA,city=Cambridge temp=82 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70 300",
                        "h2o,state=MA,city=Boston temp=71 400",
                        "h2o,state=CA,city=LA temp=90,humidity=10 500",
                        "h2o,state=CA,city=LA temp=91,humidity=11 600",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForGroupByField",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=local,region=A load1=1.1,load2=2.1 100",
                        "system,host=local,region=A load1=1.2,load2=2.2 200",
                        "system,host=remote,region=B load1=10.1,load2=2.1 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=remote,region=B load1=10.2,load2=20.2 200",
                        "system,host=local,region=C load1=100.1,load2=200.1 100",
                        "aa_system,host=local,region=C load1=100.1,load2=200.1 100",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsManyFieldsOneChunk",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.4 50",
                        "h2o,state=MA,city=Boston other_temp=70.4 250",
                        "h2o,state=CA,city=Boston other_temp=72.4 350",
                        "o2,state=MA,city=Boston temp=53.4,reading=51 50",
                        "o2,state=CA temp=79.0 300",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
        (
            "MeasurementForDefect2691",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "system,host=host.local load1=1.83 1527018806000000000",
                        "system,host=host.local load1=1.63 1527018816000000000",
                        "system,host=host.local load3=1.72 1527018806000000000",
                        "system,host=host.local load4=1.77 1527018806000000000",
                        "system,host=host.local load4=1.78 1527018816000000000",
                        "system,host=host.local load4=1.77 1527018826000000000",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "MeasurementForWindowAggregate",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Boston temp=70.0 100",
                        "h2o,state=MA,city=Boston temp=71.0 200",
                        "h2o,state=MA,city=Boston temp=72.0 300",
                        "h2o,state=MA,city=Boston temp=73.0 400",
                        "h2o,state=MA,city=Boston temp=74.0 500",
                        "h2o,state=MA,city=Cambridge temp=80.0 100",
                        "h2o,state=MA,city=Cambridge temp=81.0 200",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "h2o,state=MA,city=Cambridge temp=82.0 300",
                        "h2o,state=MA,city=Cambridge temp=83.0 400",
                        "h2o,state=MA,city=Cambridge temp=84.0 500",
                        "h2o,state=CA,city=LA temp=90.0 100",
                        "h2o,state=CA,city=LA temp=91.0 200",
                        "h2o,state=CA,city=LA temp=92.0 300",
                        "h2o,state=CA,city=LA temp=93.0 400",
                        "h2o,state=CA,city=LA temp=94.0 500",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Test data to validate fix for
            // <https://github.com/influxdata/influxdb_iox/issues/2697>
            "MeasurementForDefect2697",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "mm,section=1a bar=5.0 1609459201000000011",
                        "mm,section=1a bar=0.28 1609459201000000031",
                        "mm,section=2b bar=4.0 1609459201000000009",
                        "mm,section=2b bar=6.0 1609459201000000015",
                        "mm,section=2b bar=1.2 1609459201000000022",
                        "mm,section=1a foo=1.0 1609459201000000001",
                        "mm,section=1a foo=3.0 1609459201000000005",
                        "mm,section=1a foo=11.24 1609459201000000024",
                        "mm,section=2b foo=2.0 1609459201000000002",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "ThreeChunksWithRetention",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_partially_inside.clone()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_fully_inside.clone()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(RETENTION_SETUP.lp_fully_outside.clone()),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::SetRetention(Some(RETENTION_SETUP.retention_period_ns)),
            ],
        ),
        (
            // Test data to validate fix for
            // <https://github.com/influxdata/influxdb_iox/issues/2890>
            "MeasurementForDefect2890",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "mm foo=2.0 1609459201000000001",
                        "mm foo=2.0 1609459201000000002",
                        "mm foo=3.0 1609459201000000005",
                        "mm foo=11.24 1609459201000000024",
                        "mm bar=4.0 1609459201000000009",
                        "mm bar=5.0 1609459201000000011",
                        "mm bar=6.0 1609459201000000015",
                        "mm bar=1.2 1609459201000000022",
                        "mm bar=2.8 1609459201000000031",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // Single measurement that has several different chunks with different (but
            // compatible) schemas
            "MultiChunkSchemaMerge",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=west user=23.2,system=5.0 100",
                        "cpu,region=west user=21.0,system=6.0 150",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "cpu,region=east,host=foo user=23.2 100",
                        "cpu,region=west,host=bar user=21.0 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            "TwoMeasurementsUnsignedType",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        "restaurant,town=andover count=40000u 100",
                        "restaurant,town=reading count=632u 120",
                        "school,town=reading count=17u 150",
                        "school,town=andover count=25u 160",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // This has two chunks with different tag/key sets for queries whose columns do not
            // include keys
            "OneMeasurementTwoChunksDifferentTagSet",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // tag: state
                        "h2o,state=MA temp=70.4 50",
                        "h2o,state=MA other_temp=70.4 250",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    [
                        // tag: city
                        "h2o,city=Boston other_temp=72.4 350",
                        "h2o,city=Boston temp=53.4,reading=51 50",
                    ]
                    .join("\n"),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 1,
                },
            ],
        ),
        (
            // This is the dataset defined by https://github.com/influxdata/influxdb_iox/issues/6112
            "InfluxQLSelectSupport",
            vec![
                Step::RecordNumParquetFiles,
                Step::WriteLineProtocol(
                    r#"
                    m0,tag0=val00 f64=10.1,i64=101i,str="hi" 1667181600000000000
                    m0,tag0=val00 f64=21.2,i64=211i,str="hi" 1667181610000000000
                    m0,tag0=val00 f64=11.2,i64=191i,str="lo" 1667181620000000000
                    m0,tag0=val00 f64=19.2,i64=392i,str="lo" 1667181630000000000
                    m0,tag0=val01 f64=11.3,i64=211i,str="lo" 1667181600000000000
                    m0,tag0=val02 f64=10.4,i64=101i,str="lo" 1667181600000000000
                    m0,tag0=val00,tag1=val10 f64=18.9,i64=211i,str="lo" 1667181610000000000
                    m1,tag0=val00 f64=100.5,i64=1001i,str="hi" 1667181600000000000
                    m1,tag0=val00 f64=200.6,i64=2001i,str="lo" 1667181610000000000
                    m1,tag0=val01 f64=101.7,i64=1011i,str="lo" 1667181600000000000
                    cpu,host=host1,cpu=cpu0 usage_idle=0.98,usage_system=0.2 1667181600000000000
                    cpu,host=host1,cpu=cpu0 usage_idle=0.99,usage_system=0.1 1667181610000000000
                    disk,host=host1,device=disk1s1 bytes_free=1234,bytes_used=219838 1667181600000000000
                    disk,host=host1,device=disk1s1 bytes_free=1239,bytes_used=219833 1667181610000000000
                    "#
                    .to_string(),
                ),
                Step::Persist,
                Step::WaitForPersisted2 {
                    expected_increase: 2,
                },
            ],
        ),
    ])
});

/// Holds parameters for retention period. Tests based on this need to
/// be run within the hour of being created.
///
/// ```text                  (cut off)                    (now)
/// time: -----------------------|--------------------------|>
///
/// partially_inside:   |-----------------|
///
/// fully_inside:                 +1h |-----------|
///
/// fully_outside: |---------|
/// ```
///
/// Note this setup is only good for 1 hour after it was created.
struct RetentionSetup {
    /// the retention period, relative to now() that the three data
    /// chunks fall inside/outside
    retention_period_ns: i64,

    /// lineprotocol data partially inside retention
    lp_partially_inside: String,

    /// lineprotocol data fully inside (included in query)
    lp_fully_inside: String,

    /// lineprotocol data fully outside (excluded from query)
    lp_fully_outside: String,
}

impl RetentionSetup {
    fn new() -> Self {
        let retention_period_1_hour_ns = 3600 * 1_000_000_000;

        // Data is relative to this particular time stamp
        let cutoff = Time::from_rfc3339("2022-01-01T00:00:00+00:00")
            .unwrap()
            .timestamp_nanos();
        // Timestamp 1 hour later than the cutoff, so the data will be retained for 1 hour
        let inside_retention = cutoff + retention_period_1_hour_ns;
        let outside_retention = cutoff - 10; // before retention

        let lp_partially_inside = format!(
            "cpu,host=a load=1 {inside_retention}\n\
             cpu,host=aa load=11 {outside_retention}"
        );

        let lp_fully_inside = format!(
            "cpu,host=b load=2 {inside_retention}\n\
             cpu,host=bb load=21 {inside_retention}"
        );

        let lp_fully_outside = format!(
            "cpu,host=z load=3 {outside_retention}\n\
             cpu,host=zz load=31 {outside_retention}"
        );

        // Set retention period to be at the cutoff date. Note that
        // since real world time advances, after 1 hour of real world
        // time the data that is inside the retention interval will
        // move outside (and thus not appear in queries).
        //
        // Thus this setup is only valid for 1 hour.
        let retention_period_ns = SystemProvider::new().now().timestamp_nanos() - cutoff;

        Self {
            // translate the retention period to be relative to now
            retention_period_ns,
            lp_partially_inside,
            lp_fully_inside,
            lp_fully_outside,
        }
    }
}
