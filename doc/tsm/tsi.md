# Metadata Index Proposal
This document lays out another proposal for a method for implementing a disk based index for the measurement, tags and fields metadata in InfluxDB. This proposal uses BoltDB as the underlying storage mechanism rather than defining a new format. Also, it uses a single database to store information for all shards, eliminating the need to merge multiple index results at query time.

## Disadvantages
These are the disadvantages I think this approach has to the other two proposals that create a new index file format for our use.

### Inability to query metadata based on time
The first is that queries that want to limit what series, measurements, tag keys, or tag values are returned based on some recent window of time wouldn't be easy to implement because of the single database shared across shards. There may be a way to make this efficient, but I'd have to explore that later.

### Global write lock
More importantly, the creation of new series could cause other writes to pause and possibly time out while Bolt requires a global write lock. This could be particularly bad while the underlying mmap file must be expanded for extra storage. I'll outline the approach later, which does a fair amount to limit the number of write locks that occur, but it is still a concern.

This might be something that isn't very problematic over the lifetime of a database. Once the database has been running for a while, series aren't created in mass. Rather, they're created gradually over time.

For example, if you have an average of 10,000 values per series, then you're only creating a new series for each 10k writes that go into TSM. If a user is sampling at 10 second intervals, I would expect them to hvae more than 10k points per series on average since that's only about 27 hours worth of data.

The write lock would be problematic on the initial creation of a database if a large number of series are created immediately. We'd have to do some empircal testing to find what those limits are, which would almost certainly also depend on the IOPS of the given hardware. We'd also have to do empircal testing for how the database performs as series are added over time.

This problem might make this approach a non-starter, but there are some things I like about it.

## Advantages
I think this approach has a few advantages to imlpementing our own index format.

1. Less prone to bugs/errors. BoltDB is well tested and mature so we're unlikely to have storage based bugs.
1. Would be much faster to implement. This would require updating mostly the `meta.go` file without the need for a new WAL, index format, tombstoning, and compacting mechanism. Also woulnd't need logic to merge multiple indexes at query time.
1. Might result in less storage overall because of the single global index (although the other proposals could be updated to do this too, in which case they would probably require less storage)
1. Not having to merge multiple indexes at query time might be a performance win. Although we'd have to test to see the impact. Particularly on queries that hit a single shard, or a few, or a few dozen.

## Design
The design mostly centers on how data is organized within BoltDB's structure. This creates buckets for various parts of the mapping that will enable the different types of metadata queries.

In the following layout, names are buckets, while key/value pairs are separated by `->`.

```
# given a measurement name, what fields exist, does a specific field exist, and what are the types.
# can do a prefix range scan to get all fields for measurement
Fields:
  	<measurement name><field name> -> <type byte>

# given a measurement name, what series IDs does it have
MeasurementSeries:
  <measurement name> -> bitmap

# given a series key, what is its uint64 ID
SeriesKeyToID:
  <series key> -> <id>

# given a series ID, what is its key
SeriesIDToKey:
  <series id> -> <series key>

# what series IDs are under a given measurement and tag key/value pair. e.g. cpu,region=west
MeasurementTags
  <measurement name><tag key/value> -> bitmap

# Given a measurement, what tag keys does it have.
# can do a prefix range scan to get all keys for a mesurement
MeasurementTagKey
  	<measurement><key> -> <empty byte>

# what is the unique ID for a measurement and a tag key. e.g. cpu,region
# this is used in the next bucket to quickly scan for tag values
MeasurementTagKeyIDs
  <measurement><key> -> <id>

# tag values for a given measurement and key.
# can do a prefix range scan to get all values for a measurement and key
# e.g. cpu,host
MeasurementTagKeyValues
  <mid>value -> <empty byte>

# which shards exist
ShardMappings
  # each shard ID has its own bucket
  ShardIDBucket
    # every series ID in a shard will be in this bucket
  	seriesID -> <empty byte>
```

That set of mappings will give all the current functionality and be able to return measurements, series, tag keys, and tag values without sorting them in the query planning stage since they're already sorted in BoltDB.

## Minimizing Write Locks
When writing new points into the database, a view transaction would be obtained on BoltDB. It would loop through the points and validate that the series keys and field values already exist and are of the right type. For any new series or fields, they would be marked as new to be created. We also see if any of these series need to be marked as new to the shard.

If no series, fields, or shard mappings are new, the regular write path continues without ever obtaining. My key assumption for this scheme to work is that this will be the case a significant portion of the time writes are coming in.

If new mappings must be created, obtain a write transaction on BoltDB, create the new series, fields and shard mappings. Be sure to check that the fields havnen't been created in the interim between the read and write lock being obtained.

Finally, the process for dropping shards. Given a shard ID, we'd obtain a view transaction on Bolt:

* For all series IDs in shard bucket to be dropped (fast range scan in Bolt)
  * Put ID into a map[uint64]struct{}, M
* While len(M) > 0
  * For each of the other shard buckets, B
    * For each ID in B
      * delete(M, ID)

If `len(M) == 0` we exit out, otherwise get a write transaction on Bolt
* delete the bucket for the shard being dropped
* validate that none of the IDs in M have been written to any of the other buckets since the read lock was released
* drop any series that are still in M
