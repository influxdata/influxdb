# Overview

This document describes the "high cardinality" problem as it relates to the in-memory index.  The current problems and the requirements needed for a solution to the problem are explained.

## Problem Statement

The database maintains an in-memory, inverted index that maps measurements with tags and values to series.  This was initially implemented as part of previous storage engines to facilitate faster query planning and meta queries.

The in-memory aspect of the index present problems when series cardinality is large.  This can happen from writing to many distinct measurements or tags as well as using tag keys and values that are large in size.  For example, writing a Docker container ID create high cardinality as well as large tag values consuming large amounts of memory.  Storing counter as a tag can create many distinct series causing memory problems over time.

Another issue that arises with high cardinality is slower startup times.  Startup times increase because the index needs to be reloaded from TSM and WAL files.  Each series key must be re-parsed from the TSM file to reconstruct the measurement and tag to value index.

## Requirements

### Functional

1. The index must support finding all series by a measurement
2. The index must support finding all series by a tag and value
3. The index must support retrieving all tag keys
4. The index must support retrieve all tag keys by measurement
5. The index must support retrieving all tag values by tag key
6. The index must support retrieving all tag values by tag key and measurement
7. The index must support the removal of measurements
8. The index must support removal of series
9. The index must support removal of tag keys and values
10. The index must support finding all series by regex
11. Updating the index for new series must not cause writes to block
12. Queries and writes must not block each other
13. The index must support point-in-time snapshots using the current shard backup and snapshotting mechanism (hard links)

### Performance

1. The index must be able to support 1B+ series without exhausting RAM
2. Startup times should must not exceed 1 min
3. Query planning must be <10ms
4. The index should not significantly increase the total storage size of a shard.

### Reliability

1. The index should be able to be re-created from existing TSM and WAL files.

### Backward Compatibility

1. The server must be able to operate with shards that do not have an index.

## Use Cases

The following use-cases should be applied to any proposed designs to evaluate their feasibility.  For each use case, the requirements above should be evaluated to better understand the design.

1. `SHOW MEASUREMENTS`
2. `SHOW TAG KEYS`
3. `SHOW TAG VALUES WITH KEY = foo`
4. `SELECT * FROM cpu`
5. `SELECT * FROM cpu WHERE host = 'server-01'`
6. `SELECT count(value) FROM cpu where host ='server-01' AND location = 'us-east1'`
6. `SELECT count(value) FROM cpu where host ='server-01' AND location = 'us-east1' GROUP BY host`
7. `DROP MEASUREMENT cpu`
8. `DROP SERIES cpu WHERE time > now() - 1h`
9. `DROP SEREIES cpu WHERE host = 'server-01'`

For each use case, the proposed design should be evaluated against it to understand how the index will be queried to return results for a single shard and across shards.  What are the performance characteristics? Is it natively supported by the index or does it require post-processing?
