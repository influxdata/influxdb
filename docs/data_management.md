# How InfluxDB IOx manages the lifecycle of time series data


## Definitions
While IOx is a columnar database built on top of object storage, that’s only part of its role. In addition to query functionality, one of the big goals of IOx is to help manage the data lifecycle for time series data. This post will cover some of the constructs within IOx to manage that data lifecycle.

The data lifecycle of time series data usually involves some sort of real-time ingest, buffering of data for asynchronous replication and subscriptions, in-memory data for query, and writing out large blocks of immutable data in a compressed format. Add the movement of that data to/from memory and object storage, removal of high precision data to free up space and that’s most of what IOx is considering with the data lifecycle.

The real-time aspect of time series data for monitoring, ETL, and visualization for recent data is what IOx is optimizing for. Because IOx uses object storage and Parquet as its persistence format, we can defer larger scale and more ad hoc processing to systems that are well suited for the task.

IOx defines APIs and configuration to manage the movement of data as it arrives and periodically in the background. These can be used to send data to other IOx servers for processing, query or sending it to object storage in the form of write buffer segments or Parquet files and their summaries.

## Vocabulary Definitions
This section describes the key terms and vocabulary that is used in this document. Brief descriptions are given, but their meanings should become more clear as you read through the rest of this document. It’s impossible to talk about the data lifecycle without touching on each of these terms.


*Database*: the top level organizational object for data in IOx. Logically a database is a collection of tables which each have a collection of columns. The mapping from the InfluxDB data model to this model is that each measurement is a table and each tag and field is a column. This last detail means that tag names and field names must be unique within a measurement.

*Table*: a table is a collection of records, or rows. A table has many columns where each column has a unique name and a logical data type (u64, i64, f64, bool, string). The underlying type in memory or in persisted form may be different to optimize for space. For InfluxDB style data, a measurement maps to a table.

*Column*: a vector of values of a single type. Tags are columns where the column name is the tag key and the values in the vector are the tag values. Fields are also columns where the field name is the column name and the values in the vector are the field values.

*Partition*: a grouping of data within a database defined by the administrator of IOx. Each row is assigned to a partition based on a string identifier called its partition key. Partitions within a database are generally non-overlapping, but because the rules for how to generate partition keys can change over time, this isn’t a strict requirement. When querying data, a partition key is irrelevant and only the actual data contained in the partition is considered when determining the best plan for querying.

*Write Buffer*: a buffer for entries into a write buffer. This buffer can exist in-memory only or as a file appended to on the local filesystem.

*Write Buffer Segment*: a historical part of the Write Buffer that has a monotonically increasing identifier. Segments are an ordered collection of individual Write Buffer entries. Segments can exist in memory or as a read-only file on local disk or object storage. Its filename should be its identifier as a base 10 number with 10 digits with leading zero padding to make it sort correctly by filename.

*Mutable Buffer*: an in-memory collection of data that can be actively written to and queried from. It is optimized for incoming, real-time writes. Its purpose is to buffer data for a given partition while that partition is being actively written to so that it can later be converted to a read-optimized format for persistence and querying.

*Read Buffer*: an in-memory read-optimized collection of data. Data within the read buffer is organized into large immutable chunks. Each chunk within the read buffer must be created all at once (rather than as data is written into the DB). This can be done from data buffered in the mutable buffer, or from Parquet files, or from Write Buffer segments.

*Object Store*: a store that can get, put, or delete individual objects with a path like /foo/bar/asdf.txt. It also supports listing objects in the store with an optional prefix filter. Underlying implementations exist for AWS S3, Google Cloud Storage, Azure Blob Storage, and for single node deployments, the local file system, or in memory.

*Chunk*: a chunk is a collection of data within a partition. A database can have many partitions, and each partition can have many chunks. Each chunk only contains data from a single partition. A chunk is a collection of tables and their data, the schema, and the summary statistics that describe the data within a chunk. Chunks, like Write Buffer segments, have a monotonically increasing ID, which is reflected in their path in the durable store. Chunks can exist in the Mutable Buffer, Read Buffer, and Object Store.

*Chunk Summary*: the summary information for what tables exist, what their column and data types are, and additional metadata such as what the min, max and count are for each column in each table. Chunk summaries can be rolled up to create a partition summary and partition summaries can be rolled up to create a database summary.

*Row Group*: row groups are separations of data within a Chunk. Row groups have non-overlapping data within a chunk and map to the same equivalent in Parquet files. The *Read Buffer* has a row group equivalent. What boundaries to use to separate data into row groups can be set by the operator. This provides another method for allowing the query planner and executor to rule out sections of data from consideration to process a query.

## Data Lifecycle
An IOx server defines the following parts of the data lifecycle:
1. Ingest (from InfluxDB Line Protocol, JSON, or Flatbuffers)
2. Schema validation & partition key creation
3. Write buffering and shipping Write Buffer segments to object storage
4. Writes to In-memory buffer that is queryable (named MutableBuffer)
5. Synchronous Replication (inside the write request/response)
6. Subscriptions (sent outside the write request/response from the Write Buffer)
7. Creation of immutable in-memory Chunks (in the ReadBuffer)
8. Creation of immutable durable Chunks (in the ObjectStore)
9. Tails (subscriptions, but from Write Buffer Segments in object storage potentially from many writers)
10. Loading of Parquet files into the Read Buffer

Each part of this lifecycle is optional. This can be run entirely on a single server or it can be split up across many servers to isolate workloads. Here’s a high level overview of the lifecycle flow:


<!---
From drawings/data_lifecycle.monopic
-->

```



                ┌────────────────────────────────────────────────────────────────────────────┐
                │   Ingest (write requests, replication, object store reads)                 │
                │          ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
                │          │     Line     │    │     JSON     │    │ FlatBuffers  │          │
                │          │   Protocol   │    │              │    │              │          │
                │          └──────────────┘    └──────────────┘    └──────────────┘          │
                │                                                                            │
                └────────────────────────────────────────────────────────────────────────────┘
                                                       │
                                                       │
                                                       ▼
                                       ┌───────────────────────────────┐
                                       │                               │
                                       │      Schema Validation &      │
                                       │   Partition Key Generation    │
                                       │                               │
                                       └───────────────────────────────┘
                                                       │
                            ┌──────────────────────────┼───────────────────────────┐
                            ▼                          ▼                           ▼
                 ┌─────────────────────┐   ┌───────────────────────┐    ┌────────────────────┐
                 │                     │   │      Synchronous      │    │                    │
                 │   Mutable Buffer    │   │      Replication      │    │    Write Buffer    │
                 │                     │   │                       │    │                    │
                 └─────────────────────┘   └───────────────────────┘    └────────────────────┘
                            │                                                      │
                            │                                                      │
         ┌──────────────────┼                                          ┌───────────┴────────────┐
         │                  │                                          │                        │
         ▼                  ▼                                          ▼                        ▼
 ┌──────────────┐   ┌────────────────┐                       ┌────────────────────┐   ┌────────────────────┐
 │   Queries    │◀─ │    Chunk to    │                       │    WB Segment to   │   │   Subscriptions    │
 │              │   │  Read Buffer   │                       │    Object Store    │   │                    │
 └──────────────┘   └────────────────┘                       └────────────────────┘   └────────────────────┘
         ▲                   │
         │                   ▼
         │          ┌────────────────┐ 
         │          │    Chunk to    │ 
          ────      │  Object Store  │ 
                    └────────────────┘ 
 ```


As mentioned previously, any of these boxes is optional. Because data can come from Write Buffer Segments or Chunks in object storage, even the ingest path is optional.

## How data is logically organized

Whether data is in the Mutable Buffer, the Read Buffer, or Object Store, it has a consistent logical organization describing it. Queries should work across all sources and be combined at query time. Real-time writes can only be written into the Mutable Buffer, which can then be converted over to immutable chunks in the form of Parquet files and their summaries in object storage or as read only blocks of memory with summary statistics in the Read Buffer.

Here’s an example of how data is logically organized for a setup where partitions have been defined by day:

<!---
From drawings/data_organization.monopic
-->

```


                         ┌──────────────────────────┐
                         │          mydata          │                                   Database
                         └──────────────────────────┘
                                      |
                                      ▼ 
                         ┌──────────────────────────┐
                         │          mytable         │                                   Table
                         └──────────────────────────┘
                                       │
             ┌─────────────────────────┼───────────────────────┐
             │                         │                       │
             ▼                         ▼                       ▼
   ┌──────────────────┐      ┌──────────────────┐    ┌──────────────────┐
   │    2020-12-07    │      │    2020-12-08    │    │    2020-12-09    │               Partitions
   └──────────────────┘      └──────────────────┘    └──────────────────┘
             │                         │                       │
      ┌──────┴───┐          ┌──────────┤            ┌──────────┼──────────┐
      │          │          │          │            │          │          │
      ▼          ▼          ▼          ▼            ▼          ▼          ▼
  ┌───────┐  ┌───────┐  ┌───────┐  ┌───────┐    ┌───────┐  ┌───────┐  ┌───────┐         Chunks
  │   1   │  │   2   │  │   1   │  │   2   │    │   1   │  │   2   │  │   3   │
  └───────┘  └───────┘  └───────┘  └───────┘    └───────┘  └───────┘  └───────┘


```
