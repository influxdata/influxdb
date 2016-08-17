# TSI Proposal

## Overview

This is a proposal for a TSI (Time-Series Index) file format to provide fast, memory-mapped access to an inverted index of measurement, series and tag data. It operates at a per-shard level and can be appended to as new series are added.


## Block types

The file contains multiple block types:

- Series Block - This block stores a series block ID and a list of series ids that map to series keys. The list is sorted by series key. The series id is only used internally within the index file and no guarantees are provided about preserving keys after a compaction.

- Measurement Block - This block stores the measurement name and a list of tag keys. Each tag key points to a tag value block.

- Tag Value Block - This block stores a list of values for a tag key that points to a sorted list of series ids. The series ids are used instead of series keys because the IDs are much smaller than the keys and sorted integer sets can be operated on more quickly.

- WAL Block - This block stores a list of series keys that have not been compacted yet.

## Usage

Initially, series are appended to a WAL block and the series information is held in-memory. Once this reaches a threshold, the WAL block is compacted to a set of series, measurement, & tag value blocks. The WAL is discarded and these new blocks will serve as the index and will be operated upon through an mmap.

Series keys can be queried by measurement and tag value by first jumping to the appropriate measurement block. References to measurement blocks will be held in-memory once the files are read in. From the measurement block, a binary search on the tag keys will allow you to find a particular key. Once the key is found, you can jump to the key's tag value block and perform a binary search to find a particular value and a list of associated series IDs.

_One future optimization may be to maintain an in-memory histogram to limit the number of pages searched during tag key and tag value traversal._

### Tiered storage

Once the series, measurement, and tag value blocks are written for a WAL block, a new WAL block can be started at the end of the file. Once this WAL block reaches a threshold size, it can be rewritten as a new set of series, measurement, and tag value blocks.

These measurement blocks may duplicate measurement blocks created earlier by other WAL blocks so looking up measurements will involve first searching the new measurement block and then falling back to older measurement blocks and merging the data at query time.

### Compaction

As TSI files grow and measurements are duplicated, query time merging becomes more costly so a larger compaction will be necessary to consolidate all series, measurement, and tag value blocks into one.


## Detailed Block Information

### Series Blocks

Series blocks contain a `uint32` block ID followed by a list of `uint32` local series IDs. Combining the block id and local series ID provides a globally addressable `uint64` series ID: `(blockID,seriesID)`

The layout on disk for the block would be:

```
BLOCKID<uint32>
KEYN<uint32>
(KEYPOS<uint64>)*
(KEYSZ<uint16>,KEY<string>)*
```

### Measurement Blocks

Measurement blocks hold the measurement name and a list of tags with pointers to their tag value blocks.

The layout on disk for the block would be:

```
NAMESZ<uint16>,NAME<string>
KEYN<uint64>
(KEYPOS<uint64>,VALBLKPOS<uint64>)*
(KEYSZ<uint16>,KEY<string>)*
```


### Tag Value Blocks

Tag value blocks contain a sorted list of tag values.

The layout on disk for the block would be:

```
VALN<uint32>
(VALPOS<uint64>)*
(VALSZ<uint16>,VAL<string>)*
```


## Other considerations

Some other data we may want to include are:

- Checksums on blocks
- Block sizes to avoid out-of-range reference bugs
- Trailers on measurement blocks to allow files to be parsed backwards. This may conflict with having WAL blocks because partial writes to a WAL will make it difficult to read backwards.
