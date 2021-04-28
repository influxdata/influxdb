# IOx — Catalog Persistence

This document illustrates the design of the catalog preservation to object store for IOx.


## 1. Requirements

Designs MUST fulfill the following requirements.


### 1.1. Information Content
The catalog as a whole should provide the the following data:

- **List of Parquet Files:** Every file that is part of the preserved dataset must be listed in the catalog. Files that are present in the object store but are not listed in the catalog must not be considered for any read or write operation. A pruning / garbage collection operation might be offered to remove orphaned files. Note that parquet files written once will never be altered (they might be deleted though).
- **Statistics:** For every RowChunk in every Parquet file listed in the catalog the full Parquet statistics (per-column min/max but also bloom filters where applicable) must be preserved in easily accessible form. This provides a way to speed up query processing.
- **Schemas:** For every parquet, have the schema available (i.e. for every column the column type). Note that even for a single table, the schemas may vary due to federated writes using multiple unconnected writers.
- **Tombstone Records:** Since existing parquet files cannot be altered and (soft) deletes should be somewhat cheap, tombstone records or pointers to them must be preserved as well.
- **Historical Transactions:** Old states of the catalog must be preserved as well to allow for time travelling / transaction selection. The catalog might use full state preservation or delta encoding. Transactions might only be preserved at the granularity in which they are written (i.e. no row-level time travel, maybe not even a per-file-write time travel).

Note that this only preserves that part of the catalog that was written to the object store. It does NOT preserve any data about MutableBuffers and ReadBuffers since this could be recovered from the log.


### 1.2 Operations
The following operations must be supported:

- **Add:** Append new parquet files to the persisted catalog.
- **Remove:** Remove a parquet file from the persisted catalog.
- **Soft Delete:** Soft-delete a record (via tombstone).
- **Compact:** Create new parquet files with tombstone records committed and duplicates removed. This can likely be a simple Add+Remove.
- **Transaction/Checkpoint Pruning:** Remove old transactions/checkpoints to safe memory. Transactions/Checkpoints can only be deleted from the beginning of the recorded timeline (i.e. transactions/checkpoints in the middle or the end of the timeline cannot be removed).
- **Garbage Collection:** Remove parquet files that are not part of the catalog.
- **Read (at Point in Time): **Read data at the last or a dedicated point in time (= transaction).


### 1.3 Properties
The catalog must have the following properties (hard requirements):

- **Eventual Preservation:** After some time the parquet files a writer has written must appear in the catalog.
- **Defined State:** At each point in time (especially but not necessarily after a writer crashed) the catalog must be in a usable, defined state.
- **Minimal Data Loss:** Corrupting or losing objects belonging to the preserved catalog only leads to partial data loss.
- **Rebuildability:** It must be possible to rebuild the catalog from the in-store parquet files.
- **Forward Moving:** A transaction that gets defined and preserved is the basis for the next transaction. Defined transactions must never be lost (with transaction pruning being a notable exception!).
- **Linearity:** There exists one and only one linear timeline of transactions.
- **Upgradability:** If new features or information should be added in a future iteration, the catalog must provide a way to upgrade to a new protocol without violating the other properties.
- **Integration:** Simple integration with the remaining Rust IOx stack as well as the supported storage backends. No additional service (like an external database or HDFS) must be required. Configuration (options as well as the way to configure it) must be in line with the remaining stack. Writer-to-writer communication is not allowed/possible.
- **Writer Federation:** Writers write into their own namespace (and in the future maybe even their own object store buckets). No inter-writer communication must be required to handle the catalog. This also means that the catalog is writer-specific (via serverID).

The catalog should fulfill the following properties as good as possible (soft requirements):

- **Simplicity:** [KISS](https://en.wikipedia.org/wiki/KISS_principle).
- **Efficiency:** The catalog should be quick to process and should only consume a reasonable amount of object store space (both in terms of number of blobs as well as the size of the blobs).


### 1.4 Out of Scope
The following features are out of scope for the first iteration / version:

- Schema Changes
- Re-partitioning

However these features can be implemented in the future (see “Upgradability” in _Section 1.3_). The initial design must also ensure that there are at least no obvious blockers for these features.


## 2. Assumptions

For design and implementation, the following assumptions are made.


### 2.1 Object Store Consistence
The used object store offers strong consistency, especially strong read-after-write consistency. The object store is NOT required to offer a “PUT if not exists” feature. To workaround this limitation, writers (but not readers) use different namespaces:

```text
   ┌─────────────┐    ┌─────────────┐       ┌─────────────┐
   │             │    │             │       │             │
   │  Writer 1   │    │  Writer 2   │  ...  │  Writer N   │
   │             │    │             │       │             │
   └─────────────┘    └─────────────┘       └─────────────┘
          ▲                  ▲                     ▲
          │                  │                     │
..........│..................│.....................│..........
.         │                  │                     │         .
.         ▼                  ▼                     ▼         .
.  ┌─────────────┐    ┌─────────────┐       ┌─────────────┐  .
.  │             │    │             │       │             │  .
.  │ Namespace 1 │    │ Namespace 2 │       │ Namespace N │  .
.  │             │    │             │       │             │  .
.  └─────────────┘    └─────────────┘       └─────────────┘  .
.                                                            .
.                        Blob Store                          .
.                                                            .
..............................................................
          │                  │                     │
          ▼                  ▼                     ▼
   ┌─────────────┐    ┌─────────────┐       ┌─────────────┐
   │             │    │             │       │             │
   │  Reader 1   │    │  Reader 2   │  ...  │  Reader N   │
   │             │    │             │       │             │
   └─────────────┘    └─────────────┘       └─────────────┘
```

So parquet files are named using the following schema:


```text
<writer_id>/<database>/data/<partition_key>/<table_name>/<chunk_id>.parquet
```

Note that currently there is only a single partition key implemented. Hierarchical partitioning might follow in the future but should not change anything fundamental about the object store handling.

So an example file hierarchy could look like this:

```text
2815898179/
           my_db/
                    data/
                         2020-01/
                                 sensors/
                                         0.parquet
                                         1.parquet
                                 stocks/
                                         0.parquet
                         2020-02/
                                 sensors/
                                         0.parquet
           other_db/
                    data/
                         2020-01/
                                 health/
                                         0.parquet
3837527170/
           my_db/
                    data/
                         2021-01/
                                 stocks/
                                         0.parquet
```


### 2.2 Conductor-managed
IOx nodes are managed by Conductor. serverIDs are unique amongst all running IOx nodes.


### 2.3 Reader-based Schema Enforcement / Migration
Schemas during write are only enforced on “best effort” basis by the MutableBuffer and/or any dispatch logic beforehand. As a consequence parquet files belonging to the same table may have different schemas when being written by different writers. It is the responsibility of the query system (= reader) to prune invalid data or do necessary casts/migrations/fills.


### 2.4 UTF-8 Passthrough
The solution presented here will pass UTF-8 strings (for table and column names) as is. No [unicode normalization](http://www.unicode.org/reports/tr15/) or case-handling will be implemented.


### 2.5 Simple Transactions
Writers only need to perform single-writer single-database single-table transactions. Especially they do NOT need to perform any cross-writer or cross-database or cross-table transactions.


### 2.6 Parquet File Metadata
It is assumed that a parquet file encodes the following information either in its in-file JSON metadata or its name:

- **Statistics:** Statistics like min/max for every RowGroup. This is an out-of-box feature of Parquet.
- **Schema:** Type information for every column. This is an out-of-box feature of Parquet.
- **Logical Clock:** When the data was ingested. This is required for de-duplication. Store both the first AND last value over all rows.
- **Partition:** To which partition the file belongs to.
- **serverID:** Which writer wrote the data.
- **Database:** To which database the file belongs to.
- **Table:** To which table the file belongs to.
- **TransactionID:** During which transaction the file was added (or is about to be added) to the catalog. This is later required to perform pruning of files and distinguish between “already written and not yet added to the catalog” and “orphaned”. See _section 3_ for the design of that ID.

Therefore it shall be possible to rebuild a catalog just by scanning the files, assuming that orphaned files are pruned regularly.


## 3. Design Options – Transaction Encoding

This section discusses different design options that could be used to encode the transaction. The actual storage layout for the chosen encoding is clarified in _section 4_.

A transaction is addressed using the following schema:

```text
<linear_id>.<uuid4>
```

This ensures the following properties:

- **Linear Enumeration:** Transactions can be traversed in a linear fashion
- **Uniqueness:** Transactions both complete and incomplete (e.g. due to a crash) are always unique. This allows the detection of uncomplete transactions later on. 


### 3.1 Catalog Checkpoint _· rejected_
Store each transaction with its full information mentioned in _section 1.1_.

Pros:

-   Simple
-   Direct `O(1)` (writer-specific) transaction access

Cons:

- Whole catalog is preserved for every single transaction leading to an `O(n^2)` memory usage over the number of transactions


### 3.2 Catalog Transactions _· rejected_
Only preserve catalog changes between transactions. Changes could be encoded using the following messages:

- `Add`: Adds a new parquet file. Contains parquet file name, statistics and schema.
- `Remove`: Removes a parquet file. Contains parquet file name.
- `Tombstones`: Adds tombstones to catalog. Contains tombstones by key.
- `Commit`: Close a transaction. Contains the ID of the current transaction and the ID of the previous transaction. For the first transaction, the previous ID is `None`.
- `Upgrade`: Upgrades to a new format of the catalog.

So a transaction stream might look like this:

```text
Add(
    "2815898179/my_db/year=2020/month=01/0/sensors.parquet",
)
Add(
    "2815898179/my_db/year=2020/month=01/0/stocks.parquet",
)
Commit(
    "1.bdb70c0a-883b-4d85-81f2-e4c3390428ab",
    None,
)
Add(
    "2815898179/my_db/year=2020/month=01/1/sensors.parquet",
)
Commit(
    "2.dfaafae6-9904-49e7-986e-8a7a883abfd4",
    "1.bdb70c0a-883b-4d85-81f2-e4c3390428ab",
)
Delete(
    "2815898179/my_db/year=2020/month=01/0/stocks.parquet",
)
Commit(
    "3.b054881d-5421-41ba-82f2-e060ed87ca1e",
    "2.dfaafae6-9904-49e7-986e-8a7a883abfd4",
)
```


Note that time travel is only allowed to `Commit` messages. States between two commits (= full transactions) are considered incomplete / invalid, since they do not allow for atomic changes of multiple files or combinations of operations (like `Delete` + `Add`).

Pros:

- Somewhat simple
- Memory-efficient storage format

Cons:

- Increasing computation and access time for transactions over catalog, since transaction (= `Commit` message) access is `O(n)`


### 3.3 Transaction & Checkpoint Hybrid_ · accepted_
Based on the observations in _section 3.2_ that transactions accumulate over time and that reading them gets more and more expensive, this proposal also preserves checkpoints at some intervals. This allows easy state access for the preserved transaction and quick reconstruction of the transaction states that are not directly preserved, while avoiding full state preservations for every single transaction.

For the encoding, the transaction messages from the previous proposal can be used with an additional message `Checkpoint` that contains the transaction ID and the full transaction state. Note that the `Checkpoint` does NOT replace the `Commit` message. Since checkpointing is a mere optimization, it is always optional and the writer might skip it due to load constraints. Also, checkpoints might be added later. A transaction counts as “preserved” when the `Commit` message is written.

Pros:

- Relatively fast access time (`O(i)` with `i` being the checkpoint interval)
- Relatively memory efficient
- Possible tuning parameter for every database

Cons:

- Higher implementation complexity
- New parameter


## 4. Design Options – Storage Layout

This section illustrates different options for storing the encoded transactions. The exact layout obviously depends on chosen storage encoding, but generic options and their tradeoffs can be presented.


### 4.1 No Catalog _· rejected_
**IMPORTANT: This option is mostly here for sanity checking if we need catalog preservation at all. It is not meant to be a "real" design alternative.**

Just store parquet files as it is and list + scan them on reader/writer startup. The pure existence of parquet files means that they are "in the catalog".

Pros:

- Easy option to get everything up and running
- Cheapest option for writers

Cons:

- High scanning costs for readers: At least one listing to get all parquet files, plus (if desired to speed up the queries) one GET operation per parquet file to gather statistics
- Tombstones must still be preserved somewhere
- Assignments from parquet files ⇒ transaction need to be encoded somewhere (e.g. via metadata within parquet file or file name)
- Hard to ensure consistency (e.g. what is the desired state if some parquet files got written but some are still pending when a writer crashes)


### 4.2 Single-file Catalog _· rejected_
Preserve one file per writer and serialize all transactions and/or checkpoints in it (e.g. using Protobuf, Flatbuffer, …). The last file can simply be overwritten due to the blob store consistency guarantees. Use the following naming schema:

```text
<writer_id>/<database>/history
```

So an example file hierarchy could look like this:

```text
2815898179/
           my_db/
                    history
           other_db/
                    history
3837527170/
           my_db/
                    history
```

Pros:

- Simple
- Single read operation to get the whole (writer-specific) catalog history

Cons:

- Large transfer sizes over time (since catalog history grows)
- Loss of the full catalog when file is corrupted / lost (even though the catalog could be rebuilt)


### 4.3 Transaction/Checkpoint-based Catalog _· accepted_
Instead of lumping all transactions and checkpoints into a single object preserve transactions for every transaction into its own file:

```text
<writer_id>/<database>/transactions/<zero_padded(transaction_id)>.transaction
```

Depending on the design option for the transaction encoding, this file will have one of the following contents:

- **Catalog Checkpoints:** The checkpoints are written into the corresponding file.
- **Transaction (w/ or w/o additional Checkpointing):** The messages starting right after the previous `Commit` message (or all messages for the first transaction) ending at the corresponding `Commit` message (inclusive end). So the last message within a file is always a `Commit` message.

If transactions with checkpoints are used additional object that contain only the `Checkpoint` message are created. The `Checkpoint` message will NOT be part of the `.transaction` file though:

```text
<writer_id>/<database>/transactions/<zero_padded_and_trim(transaction_id)>.checkpoint
```

Note the zero padding for both schemas which will turn 1 into 00001 (padding size still to be decided) and `bdb70c0a-883b-4d85-81f2-e4c3390428ab` into `bdb70c0a`. This allows quick sorted listing of transactions. Furthermore the inclusion of the UUID-part into the file name allows readers and writers to detect the (hopefully unusual) case that identical serverIDs were used in parallel.

So an example file hierarchy for the variant with checkpoints could look like this:

```text
2815898179/
           my_db/
                    transactions/
                                 00001.bdb70c0a.transaction
                                 00002.dfaafae6.transaction
                                 00003.b054881d.transaction
                                 00004.612a514c.transaction
                                 00005.a50f2c92.transaction
                                 00005.3611950b.checkpoint
                                 00006.88a6fd49.transaction
                                 00007.91391de5.transaction
                                 00008.ca0ebb88.transaction
           other_db/
                    transactions/
                                 00001.35e47ad8.transaction
3837527170/
           my_db/
                    transactions/
                                 00001.82c816fd.transaction
                                 00002.08dbd199.transaction
```

Transactions/Checkpoints are stored using one of the established serialization formats (e.g. Protobuf, Flatbuffer).

Pros:

- Reduces I/O for the writer
- Simple transaction and checkpoint pruning possible without reading the whole history
- Reader only needs to read relevant data

Cons:

- Somewhat more complex


## 5. Design Options – Serialization Format

Independent of the chosen transaction encoding and storage layout (except when the option of _section 4.1_ was chosen), there are the following format options to serialize the catalog into an object.


### 5.1 JSON _· rejected_
Use JSON with optional compression.

Pros:

- Human-readable
- Can be viewed and edited with “ordinary” tools

Cons:

- Verbose
- Slow to (de)serialize
- Not used elsewhere in the IOx stack


### 5.2 Flatbuffers _· rejected_
Use Flatbuffers.

Pros:

- Already used for inter-node communication

Cons:

- Usually only used for data in motion, not for data at rest
- Needs extra tooling for inspection and editing


### 5.3 Protobuf _· accepted_
Use Protobuf.

Pros:

- Already used to store IOx config into object store

Cons:

- Needs extra tooling for inspection and editing


## 6. Design Options – Catalog Discovery / Update by Readers

This section enumerates options how readers (= nodes that execute queries) can discover writer-specific catalogs and update their view.

Note that inter-writer catalog (i.e. some kind of meta-catalog) is out of scope for this design.


### 6.1 Frequent Catalog Listing _· accepted_
Readers use a `LIST` operation (with adequate prefixes) to discover catalogs. They also repeat this in regular intervals to get informed about updates.

Pros:

- No additional signalling path required
- This works for Delta Lake

Cons:

- Potentially costly


### 6.2 Exclude UUID From File Names _· rejected_
Instead of file names like

```text
00001.bdb70c0a.transaction
```

Have

```text
00001.transaction
```

After an initial `LIST` to discover the latest transaction a reader can use simple `GET` operations to walk the transaction history.

Pros:

- A single HTTP request instead of two to get the latest transaction

Cons:

- Less robust if writers ever use the same ServerID


### 6.3 Marker File _· rejected_
Every writer produces a marker file:

```text
<writer_id>/<database>/transactions/
```

This file contains the ID of the last committed transaction. This allows readers to update their writer-specific view with a single `GET` operation.

Pros:

- Fast single `GET` operation per catalog, avoid potentially expensive LIST per catalog

Cons:

- Marker file can be out-of-sync with actual catalog ⇒ additional defined behavior required
- Marker file can be missing / corrupt


### 6.4 Conductor-based _· rejected_
Use Conductor to push or pull the latest commit ID from writers to readers.

Pros:

- More reliance on conductor

Cons:

- More conductor traffic
- Conductor does not keep state ⇒ needs some bootstrap mechanism


## 7. Scenarios

Some example scenarios showing how certain properties are guaranteed by the catalog and its environment.


### 7.1 Writer Crash and “Forward Moving” + “Linear”
Imagine the following scenario:

1. Writer writes parquet file.
2. Writer adds parquet file to catalog, issues write request to object storage.
3. Writer dies and gets restarted by Conductor.

For the new writer instance (and all readers), there are two options:

- Catalog change not preserved ⇒ read is lost
- Catalog change is preserved ⇒ move forward


## 8. Open Questions

These are questions that must be answered before finishing the design.


### 8.1 Network Split
What shall we do about network splits? Especially when K8s / Conductor spawn multiple writers with the same ID?


### 8.2 Catalog Separation
When it comes to "how local/global is a catalog", we have already established that the serialized catalogs should be unique per writer (due to our object store limitations). On top of that, we have the following options:

- **Per writer catalog:** Have a single catalog per writer and do NOT make any per-database split. This has the drawback that different databases actually have different lifecycles (e.g. transaction creation and pruning, compaction etc.) which is hard to realize with a single shared (but still writer-specific) catalog.
- **Per writer-database catalog: **This option is chosen in the designs above.
- **Per writer-database-table catalog:** Also split the catalog per table. The advantage here is that we would have a finer lifecycle control and that readers only need to access the relevant catalogs for each query. As a drawback however the number of catalogs gets unbounded.


### 8.3 Cross-writer Compaction
The number of unique serverIDs over a lifecycle of a database is potentially unbounded (writers might get lost for example). This increases the scan time for readers over time. Should we do any cross-writer catalog and/or payload (= parquet) compaction to mitigate this?


## 9. Glossary

- **Checkpoint:** The full data/state that belongs to a transaction (at the commit-point).
- **Commit:** A recorded state of the catalog to which “time travel” is allowed. It marks the end of a transaction. There is one and only one commit per transaction.
- **Message:** Information about a change of the catalog state, belongs to a transaction.
- **Object Store:** External system to store objects as a key-value store (mapping a string to a byte array). Supports at least `PUT`, `GET`, `LIST` (w/ and w/o prefix), `DELETE`. See _section 2.1_ for some more assumptions.
- **Time Travel:** Using an existing transaction to view the data (i.e. all upserts and deletes) like they were recorded at the given point in time. Operations that where performed after the selected transaction will not be visible.
- **Transaction:** Set of messages that form a “single action”. Always ends with a commit.
- **serverID:** Unique ID assigned to writers (and readers) by Conductor


## 10. References / Related Work


### 10.1 Delta Lake
- [Diving Into Delta Lake: Unpacking The Transaction Log](https://databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html)
- [Diving Into Delta Lake: Schema Enforcement & Evolution](https://databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html)
- [Delta Lake: Storage configuration](https://docs.delta.io/latest/delta-storage.html)
- [Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores](https://cs.stanford.edu/people/matei/papers/2020/vldb_delta_lake.pdf)


### 10.2 Iceberg
- [Iceberg Table Spec](https://iceberg.apache.org/spec/)


### 10.3 Other Transaction/Checkpoint Formats
- [Litestream](https://litestream.io/)


### 10.4 Object Store Consistency Models
- [Amazon S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- [If-None-Match](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match) (this is used for Azure in case of `overwrite=False`, this is NOT available for S3! DeltaLake for S3 only offers atomic updates for a single “driver”)
- [Google Cloud Storage Consistency](https://cloud.google.com/storage/docs/consistency)


### 10.5 Used Technologies
- [Parquet Format](https://github.com/apache/parquet-format)


### 10.6 Terminology
- [EDUCBA: Definition of DBMS Checkpoint](https://www.educba.com/checkpoint-in-dbms/)
- [Microsoft SQL Server: Database Checkpoints](https://docs.microsoft.com/en-us/sql/relational-databases/logs/database-checkpoints-sql-server?view=sql-server-ver15)

