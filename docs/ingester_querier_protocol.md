# Ingester ⇔ Querier Query Protocol
This document describes the query protocol that the querier uses to request data from the ingesters.

The protocol is based on [Apache Flight]. We however only support a single request type: `DoGet`.


## Request (Querier ⇒ Ingester)
The `DoGet` ticket contains a [Protocol Buffer] message
`influxdata.iox.ingester.v1.IngesterQueryRequest` (see our `generated_types` crate). This message
contains:

- **namespace ID:** The catalog namespace ID of the query.
- **table ID:** The catalog table ID that we request.
- **columns:** List of columns that the querier wants. If the ingester does NOT know about a
  specified column, it may just ignore that column (i.e. the resulting data is the intersection of
  the request and the ingester data).
- **predicate:** Predicate for row-filtering on the ingester side.

The request does NOT contain a selection of partitions or shards. The ingester must respond with
all partitions and shards it knows for that specified namespace-table combination.

## Response (Ingester ⇒ Querier)
The goal of the response is to stream the following ingester data hierarchy:

- For each shard:
  - For each partition **(A)**:
    - Persistence Information:
      - Sequence number of max. persisted parquet file
      - Sequence number of max. persisted tombstone
    - For each snapshot (contains _persisting_ data) **(B)**:
      - Record batches with following operations applied **(C)**:
        - selection (i.e. row filter via predicates)
        - projection (i.e. column filter)
        - tombstone materialization

This is mapped to the following stream of Flight messages:

- **A:** `None` message type with app metadata. The app metadata is a [Protocol Buffer] of
  `influxdata.iox.ingester.v1.IngesterQueryResponseMetadata`. This message contains:
  - partition id
  - Sequence number of max. persisted parquet file
  - Sequence number of max. persisted tombstone
- **B:** `Schema` message that announces the snapshot schema. No app metadata is attached. The snapshot belongs to the
  partition that was just announced. Transmitting a schema resets the dictionary information.
- **Between B and C:** `DictionaryBatch` messages that set the dictionary information for the next record batch.
- **C:** `RecordBatch` message that uses the last schema and the current dictionary state. The batch belongs to the
  snapshot that was just announced.

The protocol is stateful and therefore the order of the messages is important. A specific partition and snapshot may only
be announced once.

All other messages types (at the time of writing these are `Tensor` and `SparseTensor`) are unsupported.


## Example
Imagine the following ingester state:

- shard S1:
  - partition P1:
    - max. persisted parquet file at `sequence_number=10`
    - max. persisted tombstone at `sequence_number=11`
    - snapshots C1 and C2
  - partition P2:
    - max. persisted parquet file at `sequence_number=1`
    - no max. persisted tombstone
    - snapshot C3
- shard S2:
  - partition P3:
    - no persisted parquet file
    - no max. persisted tombstone
    - no snapshots (all deleted)
  - partition P4:
    - no persisted parquet file
    - no max. persisted tombstone
    - snapshot C4

This results in the following response stream:

1. `None` for P1:
   - `partition_id=1`
   - `parquet_max_sequence_number=10`
   - `tombstone_max_sequence_number=11`
2. `Schema` for C1
3. zero, one, or multiple `RecordBatch`es for C1
4. `Schema` for C2
5. zero, one, or multiple `RecordBatch`es for C2
6. `None` for P2:
   - `partition_id=2`
   - `parquet_max_sequence_number=1`
   - `tombstone_max_sequence_number=None`
7. `Schema` for C3
8. zero, one, or multiple `RecordBatch`es for C3
9. `None` for P4:
   - `partition_id=4`
   - `parquet_max_sequence_number=None`
   - `tombstone_max_sequence_number=None`
7. `Schema` for C4
8. zero, one, or multiple `RecordBatch`es for C4

Note that P3 was skipped because there was no unpersisted data.


[Apache Flight]: https://arrow.apache.org/docs/Format/Flight.html
[Protocol Buffer]: https://developers.google.com/protocol-buffers
