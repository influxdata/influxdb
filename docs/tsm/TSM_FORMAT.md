# TSM Inverted Index Proposal

This is a proposal for adding a disk-based, inverted index for measurements, tags and values.  This is a extension and/or alternative of the ideas proposed in [TSI Proposal #7174](https://github.com/influxdata/influxdb/pull/7174).

The inverted index supports the following:

* Hash lookups of measurement, tags, and values
* Sorted iteration over measurement, tags and values
* Efficient series boolean operations over compressed bitmaps
* Compressed series keys

The index is a separate set of files specific to a shard that are compacted and merged periodically.  Searches run across multiple files and are merged.

## Format

```
┌────────────────────────────────────────────────────────────────┐
│                         Inverted Index                         │
├────────────────────┬──────────────────┬────┬───────────────────┤
│    Series Dict     │     Tag Sets     │... │   Measurements    │
└────────────────────┴──────────────────┴────┴───────────────────┘
```

The `Inverted Index` contains three sections: Series Dictionary, Tag Sets and Measurements.  The `Series Dictionary` contains a dictionary encoded version of all series in the index.  The `Tag Sets` are the unique tag/value pairs for a given measurement.  The `Measurements` are the unique set of measurements in the index.

```
┌───────────────────────────────────────────────────────┐
│                   Series Dictionary                   │
├───────────────────────────┬───────────────────────────┤
│        Dictionary         │          Series           │
├─────────┬─────────────────┼─────────┬─────────────────┤
│   Len   │  Sorted Terms   │   Len   │  Sorted Series  │
│ 4 bytes │                 │ 4 bytes │                 │
└─────────┴─────────────────┴─────────┴─────────────────┘
```

The `Series Dictionary` contains two sections.  The first is an array of unique terms for all series keys.  For example, `cpu,host=server0`, would create an array `3cpu4host7server0` where each term is prefixed with it's variable byte encoded length.

The series are a dictionary encoded version of the series keys.  Using the current example, it would be encoded as (0, 4, 9) where 0 is the offset where `cpu` begins, 4 is where `host` begins, and 9 is where `server0` begins.  These 3 small integers would be variable byte encoded as a byte string using the format:

```
┌───────┬───────┬───────┬───────┐
│ Meas  │  Tag  │  Tag  │ Value │
│       │ Count │       │       │
└───────┴───────┴───────┴───────┘
```

`cpu,host=server0` would encode to 4 bytes.

```
┌───────────────────────────────────────────────────────┐
│                        Tag Set                        │
├─────────┬─────────────────┬─────────┬─────────────────┤
│Vals Len │     Values      │Tags Len │      Tags       │
│ 4 bytes │                 │ 4 bytes │                 │
└─────────┴─────────────────┴─────────┴─────────────────┘
```

A `Tag Set` block stores the unique set of tags and values for a measurement.  The first 4 bytes are the byte length of the `Values` block followed by the the `Values` block.  The next 4 bytes are the byte length of the `Tags` blocks.

```
┌───────────────────────────────────────────────────────────┐
│                          Values                           │
├────────────────────┬─────────────────────────────────┬────┤
│     Hash Index     │     Sorted Value/Block List     │    │
├───────┬────────────┼───────┬────────┬────────┬───────┤... │
│  Len  │ Hash Array │  Len  │  Val   │  Len   │Bitmap │    │
│4 bytes│ N*4 bytes  │2 bytes│N bytes │4 bytes │N bytes│    │
└───────┴────────────┴───────┴────────┴────────┴───────┴────┘
```

A `Values` block contains a sorted set of values for a single tag.  It has a `Hash Index` into the `Sorted Value` section.

The `Sorted Value/Block List` section contains a 2 byte length followed by the text value bytes.  This is followed by a 4 byte length and a compressed bitmap containing Series offsets.  They always point to the offset in the file where the encoded Series exists.

An alternative option is to store uncompressed 4 byte pointers instead of a compressed bitmap.

```
┌───────────────────────────────────────────────────┐
│                       Tags                        │
├────────────────────┬─────────────────────────┬────┤
│     Hash Index     │       Sorted Tags       │    │
├───────┬────────────┼───────┬────────┬────────┤... │
│  Len  │ Hash Array │  Len  │  Tag   │Val Ofs │    │
│4 bytes│ N*4 bytes  │2 bytes│N bytes │4 bytes │    │
└───────┴────────────┴───────┴────────┴────────┴────┘
```

The `Tags` section is similar to the `Values` section in that it first has a
`Hash Index` containing offsets into the tags block.  The `Sorted Tags` section contains a sorted list of tag length and tag bytes followed by a 4 byte offset pointing to the start of the `Values` block for that tag.

```
┌────────────────────────────────────────────────────────────────────┐
│                            Measurement                             │
├────────────────────┬──────────────────────────────────────────┬────┤
│     Hash Index     │           Sorted Measurements            │    │
├───────┬────────────┼───────┬────────┬────────┬────────┬───────┤... │
│  Len  │ Hash Array │  Len  │  Meas  │Tag Ofs │  Len   │Bitmap │    │
│4 bytes│ N*4 bytes  │2 bytes│N bytes │4 bytes │4 bytes │4 bytes│    │
└───────┴────────────┴───────┴────────┴────────┴────────┴───────┴────┘
```

The `Measurement` block continas a `Hash Index` into the `Sorted Measurements` section.  The `Sorted Measurements` sections is an ordered list of measurement names followed by a 4 byte offset pointing to the start of the `Tags` section for that measurement.  It also contains a compressed bitmap of offsets into the `Series` that maps all series in the measurment for the cases where a measurement has no tags.

## Tombstones

TODO: Fill this out more (Use FLAG idea already proposed)

## Index Construction

TODO: Fill this out more (Use compactions/WAL idea already proposed)

## Hash Indexes

The `Measurement`, `Tags` and `Values` sections all contain hash indexes hashing the key to an offset in the sorted data section.  The hashing algorithm used would be _Robin Hood Hashing_ [1] which minimizes worst case search time and is also more CPU/Disk cache friendly.  This allows for O(1) lookups when using exact filtering (very common) and more closely matches the current in-memory structure.

## Bitmap Indexes

The Bitmap indexes in the `Values` and `Measurement` sections are _Roaring Bitmaps_ [2] which are a compressed bitmap format that allow for set operations on the compressed bitmaps.

## Searches

The `Hash Index` on measurements, tags and values allows for O(1) access by exact filtering.  The sorted measurement, tags and values allows for retrieval in sorted order.  The latter is useful for avoiding the current `sort` in place for sorting measurements, tags and values.

When searching across multiple indexex, the sorted results are merged at runtime to generate the unique set.

The following are some high-level flows of how the index would be searched.

### `SHOW MEASUREMENTS`

This is a liner scan over the measurements block.

### `SHOW TAG KEYS`

This is a nested loop over the measurements and then tags for each measurement

### `SHOW TAG VALUES WITH KEY = foo`

This is a nested loop over the measurements and then a hash lookup into the sorted tags to find `foo` followed by a walk of the values for `foo`

### `SELECT * FROM cpu`

This is a hash lookup over measurements to find 'cpu' and then returning the series keys for each offset contained in the bitmap.

### `SELECT * FROM cpu WHERE host = 'server-01'`

This is a hash lookup over measuremetns to find 'cpu', then a hash lookup over tags to find 'host', then a hash lookup over values to find 'server-01'.  Each offset contained in the resulting bitmap is access to return the series keys.

### `SELECT count(value) FROM cpu where host ='server-01' AND location = 'us-east1'`

This is a hash lookup over measuremetns to find 'cpu', then a hash looup over tags to find 'host', then a hash lookup over values to find 'server-01'.  This bitmap would then be intersected  with the series ID found by doing a hash lookup on tags again for 'location', followed by hash lookup of `location` values for `us-east1`.  From the intersected bitmaps, the resulting series keys would be read and returned.

### `DROP MEASUREMENT cpu`

This is a hash lookup over measurements to find 'cpu' and then returning the series keys for each offset contained in the bitmap.

### `DROP SERIES cpu WHERE time > now() - 1h`

This is a hash lookup over measurements to find 'cpu' and then returning the series keys for each offset contained in the bitmap.

### `DROP SEREIES cpu WHERE host = 'server-01'`

This is a hash lookup over measurements to find 'cpu', followed by a hash lookup over tags to find `host` key, followed by a hash lookup over the resulting values to find `server-0`.  The resulting bitmap is then used to retrieve the series keys.


# Alternative Options

## TSM Index Extension

Another option would be to append an inverted index for each TSM file to the end of the TSM file.  Each TSM file would have it's own inverted index for the data stored within the file.  Searching the index would run in parallel across each TSM file with the results merged.  As compactions run, the number of indexes decrease and the series key space is gradually sorted across all TSM files.

# References

* [1] [Robin Hood Hashing Paper](https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf)
* [2] [Roaring Bitmaps](http://roaringbitmap.org)
* [3] [Sparkey](https://github.com/spotify/sparkey) - Robin Hood Hashing based Key/Value store
* [4] [Key Value Store - Emmanuel Goossaert](http://codecapsule.com/2014/05/07/implementing-a-key-value-store-part-6-open-addressing-hash-tables/) - Robin Hood Hashing based Key/Value store.
