# Response Cursor Protocol Specification

The cursor protocol is a streaming protocol that may be used by clients
who are reading from the database using a cursor.

## Encoding

The response structure here is defined independent of any specific
encoding. The structure here may be encoded using any method of
encoding whether it be JSON, MessagePack, Protocol Buffers, Flatbuffers,
etc. The default encoding is MessagePack. While any encoding may be
used, this document will describe both the general format and describe
how that format is encoded with MessagePack.

Alternative encodings, if they exist, should use similar data
structures. Alternative encodings may encode things differently
taking into account a balance of efficiency and human readability. When
there is an unclear choice between these, efficiency should take
priority over human-readability.

All responses will encode each object separately as part of a stream.
That means every structure in this document is encoded separately in
a specific order.

## Definitions

Unless otherwise stated, every struct is meant to be encoded as the
equivalent of a hash map.

## MIME Type

The MIME type for the InfluxDB Cursor Protocol is
`application/vnd.influxdb-cursor`. Encoding information is appended with
`+<encoding>`. To request the InfluxDB Cursor Protocol using MessagePack
encoding, use `application/vnd.influxdb-cursor+msgpack`. An optional
`version=x.y` tag can be added as a way of specifying an exact version
of the protocol. The current version is `1.0`.

The full content type would then be `application/vnd.influxdb-cursor+msgpack; version=1.0`.

## Usage

This protocol is not returned by default from the HTTPD service and must
be specifically requested by setting the MIME type in the `Accept` header.

## Protocol Format

The protocol is made up of a stream of objects. The first object returned
back from the server is the `Response`. The `Response` contains any
metadata about the response itself.

    type Response struct {
        // Results number of results that will be sent to this cursor.
        Results int
        // Error contains any error while processing the query,
        // but before any results are processed.
        Error   string
    }

If there is an error returned with the `Response`, the stream is finished.
The results field is the number of expected results from this query. There
is one expected result from each statement.

Each result starts with a `Result`.

    // Result is the metadata of the current result.
    type Result struct {
        ID int
        Messages []Message
        Error string
    }

    // Message is a nested data structure for an informational message
    // sent back by the server for a query.
    type Message struct {
        Level string
        Text string
    }

It contains the result id which will match with the result number
(zero-indexed). Embedded are any informational messages that can be
displayed to the user such as deprecation messages. If an error is
encountered, read the next result.

If there is no error, then the next object will be `NextSeries`.

    // NextSeries contains information about if there is another series
    // to be read from the result.
    // When encoded as MessagePack, this is a boolean.
    type NextSeries struct {
        // Present is true if there is another series to read.
        Present bool
    }

`NextSeries` communicates whether there is a next series or if we have
finished reading the result. If true, then a series should be read.

    type Series struct {
        // Name contains the measurement name of this series.
        Name string

        // Tags contains a mapping of tag keys to tag values for this
        // series.
        Tags map[string]string

        // Columns contains the column names for this series.
        Columns []Column

        // Error is set if an error was encountered while processing
        // this series.
        Error string
    }

    type Column struct {
        // Name contains the name of the column.
        Name string

        // Type contains a string representation of the returned type.
        Type string
    }

This contains the series metadata. If the error is set, then skip the
following and check for `NextSeries` again. If there is no error,
then read the `RowBatchHeader`.

    // RowBatchHeader contains information about the current batch.
    type RowBatchHeader struct {
        // Length contains the number of rows that are in this batch.
        Length int

        // Continue is set to true if there is another batch that
        // should be read.
        Continue bool

        // Error is present if an error was encountered while
        // emitting the rows for the query.
        Error string
    }

The `RowBatchHeader` contains metadata for the rows that are about to be
batched and sent to the user. It contains the number of rows to be read
and whether another batch will be sent after the current batch.

    // Row is an array of various types. Each column always has the
    // same type, but these may contain any type so the array itself
    // is untyped.
    type Row []interface{}

The row is simply an array of values. The length of each row will match
the length of the series columns array.

## Sample

    > SELECT value FROM cpu GROUP BY host; SHOW DATABASES;
    {"results":2}
    {"id":0}
    true
    {"name":"cpu","tags":{"host":"server01"},"columns":["time","value"]}
    {"length":1,"continue":true}
    ["2010-01-01T00:00:00Z",2]
    {"length":1}
    ["2010-01-01T00:00:10Z",3]
    true
    {"name":"cpu","tags":{"host":"server02"},"columns":["time","value"]}
    {"length":2}
    ["2010-01-01T00:00:00Z",2]
    ["2010-01-01T00:00:00Z",3]
    false
    {"id":1}
    true
    {"name":"databases","columns":["name"]}
    {"length":2}
    {"values":["db0"]}
    {"values":["db1"]}
    false
