# Time-Series Index

## Introduction

## Architecture

### index structures and access patterns
### series ID sets
### partitioning and file types
### compactions

## File Format

## Access Times

### Insertion

TODO

### Retrieval

This section provides some general idea of the typical timings one can expect to experience when accessing the index.

#### Measurement Retrieval

Approximate times for retrieving _all_ measurements, equivalent to executing `SHOW MEASUREMENTS`, follow. These types of query only involve materialising data held in the index.

 - Retrieve 1 measurement from TSI index: `~100µs`
 - Retrieve 100 measurements from TSI index: `~200µs`
 - Retrieve 10,000 measurements from TSI index: `~8ms`
 

Note: as the number of measurements gets larger, much of the time will be spent allocating and materialising the measurements into a `[][]byte` to be returned to the caller.


#### Tag Keys Retrieval

Approximate times for retrieving _all_ tag keys, equivalent to executing `SHOW TAG KEYS`, follow. These types of query only involve materialising data held in the index.

 - Retrieve 1 tag key from TSI index: `~65µs`
 - Retrieve 100 tag keys from TSI index: `~90µs`
 - Retrieve 1,000 tag keys from TSI index: `~1.3ms`

Note: the times here show only the TSI index access for retrieving the tag keys. In practice, the measurement retrieval times need to be added on top, since you need a measurement name to access the tag keys.


#### Tag Value Retrieval

Approximate times for retrieving _all_ tag values for a _specific_ tag key, equivalent to `SHOW TAG VALUES WITH KEY = "region"`, follow. These types of query only involve materialising data held in the index.

 - Retrieve 1 tag value from TSI index: `~20µs`
 - Retrieve 100 tag values from TSI index: `~240µs`
 - Retrieve 10,000 tag values from TSI index: `~13ms` 
 
 
#### Series ID Retrieval

Approximate times for retrieving a set of matching series ids for different total cardinalities, follow.

 - Retrieve 1 series id for db with cardinality 1: `~50µs` (`10µs`)
 - Retrieve 10 series ids for db with cardinality 100: `~50µs` (`10µs`)
 - Retrieve 100 series ids for db with cardinality 10,000: `~80µs` (`10µs`) 
 - Retrieve 10,000 series ids for db with cardinality 1,000,000: `~600µs` (`10µs`)
 - Retrieve 100,000 series ids for db with cardinality 10,000,000: `~22ms` (`10µs`)


Note: the initial time is for the first observation. The second—parenthesised—time is for subsequent observations. Subsequent observations make use of the TSI bitset cache introduced in [#10234](https://github.com/influxdata/influxdb/pull/10234).


## Complex Series ID Retrieval

Approximate times for retrieving a set of matching series ids for different total cardinalities. In these cases, each retrieval is based on two tag key/value predicates, e.g., `SHOW SERIES WHERE "region" = 'west' AND "zone" = 'a'`
 
 - Retrieve 1,000 series ids for db with cardinality 1,000,000: `~8ms` (`15µs`)
 - Retrieve 10,000 series ids for db with cardinality 10,000,000: `~7ms` (`25µs`)


Note: the initial time is for the first observation. The second—parenthesised—time is for subsequent observations. Subsequent observations make use of the TSI bitset cache introduced in [#10234](https://github.com/influxdata/influxdb/pull/10234).
In these more complex cases, a series ID set is retrieved for each of the predicates. The sets are then intersected to identify the final set. Cache times, then, are typically doubled since each series id set for each predicate is stored separately. 
There will be some additional overhead for the intersection operation.



