# Overview
**May 22, 2020**

Delorean will use the same logical data schema ([Line Protocol](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#data-types)) used by influxdb and will provide compatible query capabilities. This document sketches out how we could use [Apache Parquet](https://parquet.apache.org/documentation/latest/) as an on disk format and [Apache Arrow](https://arrow.apache.org/docs/format/Columnar.html#logical-types) as the in memory format.

# Goal
The primary goal of both the on disk and in memory formats is interoperability. Thus, if there is a choice between squeezing every last bit of compression or performance out of this encoding schema or making it easier to use with other tools, we have opted for interoperability convenience.

The idea is that someone could load the parquet files or arrow buffers and immediately be able to access the raw values easily via, for example, `pandas` without having to do additional data transformations.

# Example

## Original Data
Like all readable docs in this millennium, this one starts with a motivating example. In this case, some temperature data in line protocol format, shamelessly copied from Paul [here](https://github.com/influxdata/flux/blob/master/docs/new_flux_ideas_and_guide.md)

```
h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160
h2o_temperature,location=santa_monica,state=CA surface_degrees=63.6,bottom_degrees=49.2 1600756160
h2o_temperature,location=coyote_creek,state=CA surface_degrees=55.1,bottom_degrees=51.3 1568756160
h2o_temperature,location=coyote_creek,state=CA surface_degrees=50.2,bottom_degrees=50.9 1600756160
h2o_temperature,location=puget_sound,state=WA surface_degrees=55.8,bottom_degrees=40.2 1568756160
h2o_temperature,location=puget_sound,state=WA surface_degrees=54.7,bottom_degrees=40.1 1600756160
```

## Apache Arrow (memory) format and encodings

The example would be represented as a `h20_temperature` 'table' in the following columns:

| location (`Utf8`) | state (`Utf8`) | surface_degrees (`FloatingPoint(precision=64)`)  | bottom_degrees (`FloatingPoint(precision=64)`) | timestamp `(Timestamp(unit=NANOSECOND, timezone="")` |
| --- | --- | --- | --- | --- |
| coyote_creek | CA | 55.1 | 51.3 | 1568756160 |
| coyote_creek | CA | 50.2 | 50.9 | 1600756160 |
| puget_sound | WA | 55.8 | 40.2  | 1568756160 |
| puget_sound | WA | 54.7 | 40.1  | 1600756160 |
| santa_monica | CA | 65.2 | 50.4 | 1568756160 |
| santa_monica | CA | 63.6 | 49.2 | 1600756160 |

Note the 'location' and 'state' columns would be dictionary encoded in memory.


## On Disk (parquet) format and encodings

The example could be stored in  a single .parquet file:
* With 2 "Row Groups", each corresponding to different unique 'location' values (it could be any number of row groups)
* The 'location' and 'state' columns are encoded using `RLE_DICTIONARY=8`,
* The 'surface\_degrees' and 'bottom\_degrees' columns are encoded using `BYTE_STREAM_SPLIT = 9`
* The 'timestamp' column is encoded using `DELTA_BINARY_PACKED=5`

Row Group 1 (5 column chunks):

| location (`STRING`) | state (`STRING`) | surface_degrees (`DOUBLE`)  | bottom_degrees (`DOUBLE`) | timestamp `(Timestamp(unit=NANOSECOND, timezone="")` |
| --- | --- | --- | --- | --- |
| Coyote_creek | CA | 55.1 | 51.3 | 1568756160 |
| coyote_creek | CA | 50.2 | 50.9 | 1600756160 |

Row Group 2 (5 column chunks):

| location (`STRING`) | state (`STRING`) | surface_degrees (`DOUBLE`)  | bottom_degrees (`DOUBLE`) | timestamp `(Timestamp(unit=NANOSECOND, timezone="")` |
| --- | --- | --- | --- | --- |
| puget_sound | WA | 55.8 | 40.2  | 1568756160 |
| puget_sound | WA | 54.7 | 40.1  | 1600756160 |
| santa_monica | CA | 65.2 | 50.4 | 1568756160 |
| santa_monica | CA | 63.6 | 49.2 | 1600756160 |

*Note*: In the above example the split of rows into Row Groups is arbitrary (e.g. we could just as easily have 3 or more groups). In this document, we don't pre-suppose any particular way of dividing rows into row groups within the files.


# Logical Data Types

The follow table contains a proposed mapping from the types described in [Line Protocol Data Types](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#data-types) to the types supported by the [Parquet Disk Format](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md), and the [Arrow In Memory Format](https://arrow.apache.org/docs/format/Columnar.html#logical-types).


| Line Protocol Component | Line Protocol Type | Disk/Parquet Logical Type  | Memory/Arrow Logical Type |
| ----------------------- | ------------------ | -------------------------- | -----------------------|
| Measurements            | `String`           | N/A (Table Name)           | N/A (Tables)           |
| Tag Key                 | `String`           | N/A (Column Name)          | N/A (Column Name)      |
| Tag Value               | `String`           |`STRING`                    | `Utf8`                 |
| Field Name              | `String`           | N/A (Column Name)          | N/A (Column Name)      |
| Field Value (float)     | `float`            | `DOUBLE`                   | `FloatingPoint(precision=64)`        |
| Field Value (integer)   | `integer`          | `INT(bitWidth=64, isSigned=true)` | `Int(bitWidth=64)`       |
| Field Value (strings)   | UTF-8 `String`     | `STRING`                   | `Utf8`                 |
| Field Value (Boolean)   | `Boolean`          | `BOOLEAN`                  | `Bool`        |
| Timestamp               | 64-bit Unix Timestamp | `TIMESTAMP(isAdjustedToUTC=true, precision=NANOS)`  | `Timestamp(unit=NANOSECOND, timezone="")` |

## Alternate Tag Logical Data Type

The table above shows each distinct Tag name represented as a separate column. An alternate representation would be to encode *ALL* tags for a row in line protocol in a single 'tags' column with the [Map Logical Type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps) (aka key/value pairs), which would result in the following modification of the table:

| Line Protocol Component | Line Protocol Type | Disk/Parquet Logical Type  | Memory/Arrow Logical Type |
| ----------------------- | ------------------ | -------------------------- | -----------------------|
| Tag Key/Value           | `String`/`String`  | `Map<String,String>`       | `Map<Utf8,Utf8>`       |


The full parquet type, in the formal avro style (see [the spec](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#maps)) would be:
```
// Map<String, String>
required group tags (MAP) {
  repeated group key_value {
    required binary key (UTF8);
    optional binary value (UTF8);
  }
}
```

## Parquet Encodings
The Apache Parquet file format has several [available encodings](https://github.com/apache/parquet-format/blob/master/Encodings.md) for each physical data type. Here are proposed default encoding schemes (TODO double check this), though as noted above we would likely want to allow different encodings for the same Line Protocol Component depending on the actual data.

| Line Protocol Component | Line Protocol Type | Disk/Parquet Logical Type  | Encoding
| ----------------------- | ------------------ | -------------------------- | -----------------------|
| Tag Value               | `String`           |`STRING`                    | Dictionary (`RLE_DICTIONARY=8`)         |
| Field Value (float)     | `float`            | `DOUBLE`                   | Plain (`PLAIN = 0`) --> Byte stream split: (`BYTE_STREAM_SPLIT = 9`) if supported     |
| Field Value (integer)   | `integer`          | `INT(bitWidth=64, isSigned=true)` | Delta binary packed (`DELTA_BINARY_PACKED = 5`)|
| Field Value (strings)   | UTF-8 `String`     | `STRING`                   | Delta-length byte array: (`DELTA_LENGTH_BYTE_ARRAY = 6`)      |
| Field Value (Boolean)   | `Boolean`          | `BOOLEAN`                  | Run Length / Bit-packing hybrid (`RLE=3`) |
| Timestamp               | 64-bit Unix Timestamp | `TIMESTAMP(isAdjustedToUTC=true, precision=NANOS)`  | Delta Encoding (`DELTA_BINARY_PACKED=5`) |


## Arrow Encodings
Tag value columns would be [Dictionary](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) encoded in general. We may special case high cardinality tag columns (such as request ids) and choose not to dictionary encode them.

# Sorting
In general, many of these encodings are more efficient if repeated values occur next / near each other, so the delorean server may decide to sort data, for example, so that all repeated tags or fields appear sequentially. However, this may significantly reduce the effectiveness of `DELTA_BINARY_PACKED` encoding for the timestamp.

# Query Considerations

(TO FLESH OUT)

TODO: describe column chunks / row groups

TODO: it is possible to read parquet without having to scan all the row groups. Thus by careful placement / arrangement of rows into row groups (e.g. by placing all rows for a particular tag name or set of tag names in a particular row group, we determine which row group(s) could possibly contain the desired values and avoid having to scan/read all of them)

The information about what tag values are in what row groups can be stored in the parquet file metadata information. This metadata could also be used to store information about the ranges of field values as well

We would have to be clever about ensuring that tags with high cardinality didn't explode the metadata size

Another use case you might map out is spans — super high cardinality from a series perspective with minimal data per series. We see this in tracing use cases (for monitoring) and for potentially nested sessions (in general).
