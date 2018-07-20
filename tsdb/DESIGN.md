# High Level Concepts

These descriptions are meant to be very simple, to help understanding of the overall tsdb package.

## Series File

- bi-directional map of series ID : series key
- disk-backed (WAL, compaction, partitions)
- O(1) lookup time
- fast as possible entry insertion

## Field Set
- map of measurement : fields
- disk-backed (serialize map as protobuf and overwrite fields.idx)

## Index
- add/remove series by: series key, measurement, tag key+value
- iterate exact matches on: measurements, tag keys, tag values, series key
  - iterators return series keys, which Engine(TSM) can append field keys to

## Engine
- fast point lookup by exact match with most subsets of (measurement, tag set, field) plus timestamp
  - read operation returns cursor
- fast point write (fastest for batch writes)
- leverages Index to perform fast queries and writes
- disk-backed (WAL, compaction, partitions)
