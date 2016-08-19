# TSI Proposal

## Overview

This is a proposal for a TSI (Time-Series Index) file format to provide fast, memory-mapped access to an inverted index of measurement, series and tag data. It operates at a per-shard level and can be appended to as new series are added.


## Block types

The file contains multiple variable-length block types:

- Series Block - This block stores a series block ID and a list of series ids that map to series keys. The list is sorted by series key. The series id is only used internally within the index file and no guarantees are provided about preserving keys after a compaction.

- Measurement Block - This block stores the measurement name and a list of tag keys. Each tag key points to a tag value block.

- Tag Value Block - This block stores a list of values for a tag key that points to a sorted list of series ids. The series ids are used instead of series keys because the IDs are much smaller than the keys and sorted integer sets can be operated on more quickly.

## Usage

Initially, series are appended to a WAL file and the series information is held in-memory. Once this reaches a threshold, the WAL block is compacted to a set of series, measurement, & tag value blocks. The WAL file is discarded and these new blocks will serve as the index and will be operated upon through an mmap.

Series keys can be queried by measurement and tag value by first jumping to the appropriate measurement block. References to measurement blocks will be held in-memory once the files are read in. From the measurement block, a binary search on the tag keys will allow you to find a particular key. Once the key is found, you can jump to the key's tag value block and perform a binary search to find a particular value and a list of associated series IDs.

_One future optimization may be to maintain an in-memory histogram to limit the number of pages searched during tag key and tag value traversal._

### Tiered storage

Once the series, measurement, and tag value blocks are written for a WAL block, a new WAL block can be started at the end of the file. Once this WAL block reaches a threshold size, it can be rewritten as a new set of series, measurement, and tag value blocks.

These measurement blocks may duplicate measurement blocks created earlier by other WAL blocks so looking up measurements will involve first searching the new measurement block and then falling back to older measurement blocks and merging the data at query time.

### Compaction

As TSI files grow and measurements are duplicated, query time merging becomes more costly so a larger compaction will be necessary to consolidate all series, measurement, and tag value blocks into one.

When an index file or WAL file is compacted it is locked and the new index and WAL will begin on a new file. This is to allow snapshotting to take hard links on the files.

#### Two Compaction Types

There are two separate types of compaction that can occur:

1. Fast compaction -- compacts a set of WAL entries into series, measurement, and tag value blocks. These blocks are layered on top of previously compacted block sets.

2. Full compaction -- compacts sets of block sets into a single block set. This takes the multiple layers of series, measurement, and tag value blocks and merges them into one. It also removes any entries which have been tombstoned.


## Detailed Block Information

### Series Blocks

Series blocks contain a `uint32` block ID followed by a list of `uint32` local series IDs. Combining the block id and local series ID provides a globally addressable `uint64` series ID: `(blockID,seriesID)`. Block IDs are assigned in sequential order and are only unique within an index file.

For example, if the block ID was 1 and the local series ID was 5 then the global ID is (1,5) and is represented as `1 << 32 | 5`.

The layout on disk for the block would be:

```
BLOCKID<uint32>
KEYN<uint32>
(KEYPOS<uint64>)*
(FLAG<uint8>,KEYSZ<uint16>,KEY<string>)*
```

With a global ID, the key can be looked up by moving to the appropriate block and then moving to the `KEYPOS` based on the local series ID and then finally jumping to the key itself using the key position.

The `FLAG` contains boolean flags for the series. The first bit is a tombstone flag to mark the series as deleted. Other bits are reserved for future use.

Because the local ID is a `uint32` we are limited to approximately 4 billion series in a block. Once the ID overflows then additional series will need to be written to a new block.

#### Histogram

There will also be a smaller bucketed index at the end of the series block which will allow us to narrow down the set of pages to search. This index can be searched first and then a binary search can be performed within a limited range of the series block.

The layout of this histogram would be:

```
KEYN<uint32>
(KEYPOS<uint64>)*
(KEYSZ<uint16>,KEY<string>)*
```

### Measurement Blocks

Measurement blocks hold the measurement name and a list of tags with pointers to their tag value blocks.

The layout on disk for the block would be:

```
FLAG<uint8>,NAMESZ<uint16>,NAME<string>
KEYN<uint64>
(KEYPOS<uint64>,VALBLKPOS<uint64>)*
(KEYFLAG<uint8>,KEYSZ<uint16>,KEY<string>)*
```

The `FLAG` and `KEYFLAG` contains boolean flags for the measurement and key, respectively. The first bit is a tombstone flag to mark the object as deleted. Other bits are reserved for future use.

### Tag Value Blocks

Tag value blocks contain a sorted list of tag values.

The layout on disk for the block would be:

```
VALN<uint32>
(VALPOS<uint64>)*
(FLAG<uint8>,VALSZ<uint16>,VAL<string>)*
```

The `FLAG` contains boolean flags for the tag value. The first bit is a tombstone flag to mark the tag value as deleted. Other bits are reserved for future use.

### WAL File

The Write Ahead Log (WAL) exists to quickly append new data. Once it reaches a size threshold it can be compacted into a set of blocks on the index.

The format of each entry in the WAL is:

```
FLAG<uint8>,KEYSZ<uint16>,KEY<string>,CHECKSUM<uint8>
```

The `FLAG` holds boolean flags for the series key while the `KEYSZ` and `KEY` store a fixed-size series key. The `CHECKSUM` at the end ensures all bytes were written correctly to disk.
