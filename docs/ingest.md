# Ingest

A user expects to be able to write/delete data from an IOx database with an API call to an IOx server. Critically the user expects:

* An API response in a matter of seconds or less
* A successful API call to eventually be visible to query

This document aims to provide a high-level overview of how a number of related, but independent, subsystems come together to achieve this functionality within IOx:

* _In-Memory Catalog_ - metadata about the data contained within this server 
* Optional _Preserved Catalog_ - preserved metadata used to rebuild the in-memory catalog and start ingestion on restart
* Optional _Write Buffer_ - a write-ahead log for modifications that haven't reached the Preserved Catalog
* _Lifecycle Policy_ - determines when to persist data for a partition
* _Persistence Windows_ - keeps track of writes to unpersisted chunks in the in-memory catalog
* _Delete Mailbox_ - keeps track of unpersisted deletes applied to persisted chunks in the in-memory catalog

## 1. Produce to Write Buffer

When an API call arrives it is synchronously written to the _Write Buffer_, if any. If successful, success is returned to the client.

The _Write Buffer_ must therefore:

* Respond in a matter of seconds or less
* Provide sufficient durability for the given application
* Establish an ordering of any potentially conflicting operations, to ensure consistent recovery (and replication)

_If there is no _Write Buffer_ configured, writes are written directly to the in-memory data structures as described in the section below._

## 2. Consume from Write Buffer

Ingest servers consume from the _Write Buffer_. 

As durability is provided by the _Write Buffer_, modifications are initially only applied to in-memory data structures:

* Writes are written to the _In-Memory Catalog_ and the _Persistence Windows_
* Deletes are written to the _In-Memory Catalog_ and the _Delete Mailbox_

It is assumed that the _Write Buffer_ is configured in such a way as to provide sufficient ordering to provide eventual consistency.

This ordering is not only preserved by this ingest process, but by all lifecycle actions. This means:

* The order of chunks in a partition preserves any ordering established by the _Write Buffer_
* Query, persisted compaction, etc... only care about chunk ordering, and are unaware of the _Write Buffer_
* **The _Write Buffer_ is an implementation detail of ingest**

## 3. In-Memory Compaction

As more data is ingested, the [lifecycle](./data_organization_lifecycle.md) may re-arrange how the unpersisted data is stored within the _In-Memory Catalog_. 

As the _Persistence Windows_ tracks the unpersisted data separately, compaction can occur independently of it

## 4. Persist

If IOx is configured with an object store, and therefore has a _Preserved Catalog_, eventually the [lifecycle](./data_organization_lifecycle.md) will decide to make durable some data for a partition.

**Identify data to persist**

First the process obtains a `FlushHandle` for the partition from the _Persistence Windows_. This handle:

* Is a transaction on the _Persistence Windows_ that:
  * On commit marks the data as persisted
  * Prevents concurrent conflicting operations
* Identifies a range of data from the _In-Memory Catalog_ to persist
* Contains information about the unpersisted data outside this range

**Split data to persist**

The chunks containing data to persist within the _In-Memory Catalog_ are identified, and marked to prevent modification by another lifecycle action - e.g. compaction.

Snapshots of these chunks are taken, and two new chunks are created containing the data to persist, and what is left over.

These two new chunks are then atomically swapped into the _In-Memory Catalog_ replacing the chunks from which they were sourced. 

Any delete predicates that landed on the source chunks after the snapshots were taken, will be carried across to the two new chunks.

_Much like compaction, this must process a contiguous sequence of chunks within the partition to ensure ordering is preserved_ 

**Write data to parquet**

The data to persist is now located in a single chunk in the _In-Memory Catalog_, and is written to a single parquet file in object storage.

**Preserved Catalog**

The parquet file is now written, but it needs to be recorded in the _Preserved Catalog_, along with the position reached in the _Write Buffer_. On restart this will enable IOx to skip over data that has already been persisted.

It would be perfectly correct to simply record the min unpersisted positions in the _Write Buffer_ and on restart resume playback from there. However, as we persist data for different partitions separately, and out-of-order within a partition, this would likely result in re-ingesting lots of already persisted data. 

As such a more [sophisticated approach](../persistence_windows/src/checkpoint.rs) is used that more accurately describes what remains unpersisted.

As deletes are relatively small we opt to simply flush the entire _Delete Mailbox_ for all partitions and tables on persist. This means we only need to be concerned with recording writes that remain unpersisted.

The final _Preserved Catalog_ transaction therefore contains:

* The newly created parquet file
* All unpersisted deletes
* Checkpoint information about the _Write Buffer_ position

**Commit**

If the _Preserved Catalog_ transaction is successful we can then:

* Commit the `FlushHandle`
* Mark the chunk as persisted in the _In-Memory Catalog_

_There is a background worker that cleans up parquet files unreferenced by the Preserved Catalog. As such this process holds a shared "cleanup lock" to prevent this task from running and deleting the parquet file before it has been inserted into the Preserved Catalog._

## 5. Compact Persisted

As queries, deletes and writes land the [lifecycle](./data_organization_lifecycle.md) may decide to compact chunks that are persisted.

As this data is already durable, this occurs largely independently of the ingest flow - no new data is being persisted.

However, there are some subtleties worth mentioning:

* Like all other lifecycle actions, this must compact a contiguous sequence of chunks to preserve ordering within a partition
* The new chunk should preserve the maximum checkpoint information from the source chunks <sup>1.</sup>
* Deletes that land in the _Delete Mailbox_ whilst this operation is in-flight need to be also enqueued for the future compacted chunk

_<sup>1.</sup> Checkpoint information is only associated with chunks as a mechanism to recover from loss of the Preserved Catalog, conceptually the checkpoint is really a property of the partition_ 

## 6. Unload/Drop Data

Eventually memory pressure will trigger the [lifecycle](./data_organization_lifecycle.md) to free up memory.

If persistence is enabled, it will do this by unloading persisted chunks from the _In-Memory Catalog_. Following this the chunks will still exist, and their data can be retrieved from the persisted parquet files

If persistence is disabled, it will drop the chunks from the _In-Memory Catalog_. Following this the chunks will no longer exist, and will not be queryable
