# TSI Proposal

## Overview

This is a proposal for a TSI (Time-Series Index) file format to provide fast,
memory-mapped access to an inverted index of measurement, series and tag data.
It operates at a per-shard level and can be appended to as new series are added.


## Sections

The file format for the inverted index consists of two top-level sections:

1. `Series Dictionary` - This contains a sorted list of terms as well as a
   sorted, dictionary-encoded list of series keys. There is only one dictionary
   per file.

2. `Tag Sets` - This contains one or more tag set blocks, one for each
   measurement in the index. Inside, the Tag Keys block lists all the keys for
   a single measurement and they point to a Tag Values block which contains all
   the values for a single key.

3. `Measurements Block` - This contains a list of all the measurements in the
   index. Each measurement points to its associated tag set.


The following is a diagram of the high level components:

	╔═══════Inverted Index═══════╗
	║                            ║
	║ ┌────────────────────────┐ ║
	║ │                        │ ║
	║ │   Series Dictionary    │ ║
	║ │                        │ ║
	║ └────────────────────────┘ ║
	║ ┏━━━━━━━━Tag Set━━━━━━━━━┓ ║
	║ ┃┌──────────────────────┐┃ ║
	║ ┃│   Tag Values Block   │┃ ║
	║ ┃├──────────────────────┤┃ ║
	║ ┃│   Tag Values Block   │┃ ║
	║ ┃├──────────────────────┤┃ ║
	║ ┃│    Tag Keys Block    │┃ ║
	║ ┃└──────────────────────┘┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━Tag Set━━━━━━━━━┓ ║
	║ ┃┌──────────────────────┐┃ ║
	║ ┃│   Tag Values Block   │┃ ║
	║ ┃├──────────────────────┤┃ ║
	║ ┃│   Tag Values Block   │┃ ║
	║ ┃├──────────────────────┤┃ ║
	║ ┃│   Tag Values Block   │┃ ║
	║ ┃├──────────────────────┤┃ ║
	║ ┃│    Tag Keys Block    │┃ ║
	║ ┃└──────────────────────┘┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║  ┌──────────────────────┐  ║
	║  │  Measurements Block  │  ║
	║  └──────────────────────┘  ║
	╚════════════════════════════╝



### Series Dictionary

The series dictionary contains two sections:

1. `Dictionary` - A list of terms sorted by frequency

2. `Series List` - A sorted list of dictionary-encoded series keys.

The following is a diagram of the Series Dictionary:

	╔══Series Dictionary═══╗
	║ ┌───────────────────┐║
	║ │    Dictionary     │║
	║ ├───────────────────┤║
	║ │    Series List    │║
	║ └───────────────────┘║
	╚══════════════════════╝


#### Dictionary

The dictionary is a sorted list of terms with the most frequent terms coming
first. Each term consists of a variable encoded length followed by the term
itself. The starting position of the length is used in dictionary encoding in
the next block.

	╔══════════Dictionary══════════╗
	║ ┌──────────────────────────┐ ║
	║ │   Term Count <uint32>    │ ║
	║ └──────────────────────────┘ ║
	║ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │  len(Term) <varint>  │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │    Term <byte...>    │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │  len(Term) <varint>  │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │    Term <byte...>    │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	╚══════════════════════════════╝



#### Series List

This section contains a sorted list of dictionary encoded series keys:

	╔═════════Series List══════════╗
	║ ┌──────────────────────────┐ ║
	║ │  Series Count <uint32>   │ ║
	║ └──────────────────────────┘ ║
	║ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │     Flag <uint8>     │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │ len(Series) <varint> │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │   Series <byte...>   │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║             ...              ║
	╚══════════════════════════════╝

Each series is dictionary encoded using the terms in the Dictionary. The series
also contains a `flag`. The LSB of the flag is used to indicate a tombstone and
the remaining bits are reserved for future use.

The format for the series is:

	╔════════════Series════════════╗
	║ ┌──────────────────────────┐ ║
	║ │   Measurement <varint>   │ ║
	║ ├──────────────────────────┤ ║
	║ │    Tag Count <varint>    │ ║
	║ └──────────────────────────┘ ║
	║ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
	║ ┃┌────────────────────────┐┃ ║
	║ ┃│    Tag Key <varint>    │┃ ║
	║ ┃├────────────────────────┤┃ ║
	║ ┃│   Tag Value <varint>   │┃ ║
	║ ┃└────────────────────────┘┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━━━━━━━━━━━━━━━━━━━┓ ║
	║ ┃┌────────────────────────┐┃ ║
	║ ┃│    Tag Key <varint>    │┃ ║
	║ ┃├────────────────────────┤┃ ║
	║ ┃│   Tag Value <varint>   │┃ ║
	║ ┃└────────────────────────┘┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	╚══════════════════════════════╝

Each `varint` in the series is a pointer to a position in the Dictionary for
a given term.



### Tag Sets

This group contains a Tag Key block and multiple Tag Value blocks. There is one
tag set for each measurement.

	╔════════Tag Set═════════╗
	║┌──────────────────────┐║
	║│   Tag Values Block   │║
	║├──────────────────────┤║
	║│   Tag Values Block   │║
	║├──────────────────────┤║
	║│    Tag Keys Block    │║
	║└──────────────────────┘║
	╚════════════════════════╝


#### Tag Values Block

This block contains all the values for a single tag key. It starts with a `Hash
Index` which points into the `Value List`. The value list contains a sorted list
of values which contain a list of series they belong to.

	╔═══════Tag Values Block═══════╗
	║                              ║
	║ ┏━━━━━━━━Hash Index━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │ len(Values) <uint32> │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │Value Offset <uint32> │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │Value Offset <uint32> │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━Value List━━━━━━━━┓ ║
	║ ┃                          ┃ ║
	║ ┃┏━━━━━━━━━Value━━━━━━━━━━┓┃ ║
	║ ┃┃┌──────────────────────┐┃┃ ║
	║ ┃┃│     Flag <uint8>     │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│ len(Value) <uint16>  │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│   Value <byte...>    │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│ len(Series) <uint32> │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│SeriesIDs <uint32...> │┃┃ ║
	║ ┃┃└──────────────────────┘┃┃ ║
	║ ┃┗━━━━━━━━━━━━━━━━━━━━━━━━┛┃ ║
	║ ┃           ...            ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	╚══════════════════════════════╝

The LSB of the `Flag` byte is used to indicate a tombstone on the value.


#### Tag Key Block

This block is structured similarly to the `Tag Values` block. The `Hash Index`
points into the `Key List`. The key list contains a sorted list of keys which
point to tag value blocks.

	╔════════Tag Key Block═════════╗
	║                              ║
	║ ┏━━━━━━━━Hash Index━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │  len(Keys) <uint32>  │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │ Key Offset <uint32>  │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │ Key Offset <uint32>  │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━━Key List━━━━━━━━━┓ ║
	║ ┃                          ┃ ║
	║ ┃┏━━━━━━━━━━Key━━━━━━━━━━━┓┃ ║
	║ ┃┃┌──────────────────────┐┃┃ ║
	║ ┃┃│     Flag <uint8>     │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│  len(Key) <uint16>   │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│    Key <byte...>     │┃┃ ║
	║ ┃┃├──────────────────────┤┃┃ ║
	║ ┃┃│Value Offset <uint32> │┃┃ ║
	║ ┃┃└──────────────────────┘┃┃ ║
	║ ┃┗━━━━━━━━━━━━━━━━━━━━━━━━┛┃ ║
	║ ┃           ...            ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	╚══════════════════════════════╝

The LSB of the `Flag` byte is used to indicate a tombstone on the key.


### Measurements Block

The trailer of the index file is the measurements block which contains a lookup
of all measurements. This block contains a hash index which points into a list
of measurements. Each measurement points to its associated Tag Key block and 
has a list of associated series.


	╔══════════Measurements Block═══════════╗
	║                                       ║
	║ ┏━━━━━━━━━━━━Hash Index━━━━━━━━━━━━━┓ ║
	║ ┃ ┌───────────────────────────────┐ ┃ ║
	║ ┃ │  len(Measurements) <uint32>   │ ┃ ║
	║ ┃ ├───────────────────────────────┤ ┃ ║
	║ ┃ │  Measurement Offset <uint32>  │ ┃ ║
	║ ┃ ├───────────────────────────────┤ ┃ ║
	║ ┃ │  Measurement Offset <uint32>  │ ┃ ║
	║ ┃ └───────────────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║ ┏━━━━━━━━━Measurement List━━━━━━━━━━┓ ║
	║ ┃                                   ┃ ║
	║ ┃┏━━━━━━━━━━Measurement━━━━━━━━━━━┓ ┃ ║
	║ ┃┃┌─────────────────────────────┐ ┃ ┃ ║
	║ ┃┃│        Flag <uint8>         │ ┃ ┃ ║
	║ ┃┃├─────────────────────────────┤ ┃ ┃ ║
	║ ┃┃│     len(Name) <uint16>      │ ┃ ┃ ║
	║ ┃┃├─────────────────────────────┤ ┃ ┃ ║
	║ ┃┃│       Name <byte...>        │ ┃ ┃ ║
	║ ┃┃├─────────────────────────────┤ ┃ ┃ ║
	║ ┃┃│  Tag Block Offset <uint32>  │ ┃ ┃ ║
	║ ┃┃├─────────────────────────────┤ ┃ ┃ ║
	║ ┃┃│    len(Series) <uint32>     │ ┃ ┃ ║
	║ ┃┃├─────────────────────────────┤ ┃ ┃ ║
	║ ┃┃│    SeriesIDs <uint32...>    │ ┃ ┃ ║
	║ ┃┃└─────────────────────────────┘ ┃ ┃ ║
	║ ┃┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ┃ ║
	║ ┃                ...                ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	╚═══════════════════════════════════════╝

The LSB of the `Flag` byte is used to indicate a tombstone on the measurement.

_Note: The list of series positions may be changed to a compressed bitmap if
performance of the uncompressed `uint32` array is found to be poor._


## Write-Ahead Log (WAL)

A separate file will be used as a write-ahead log to append new series. This
file will simply contain the series key followed by a checksum:

	╔═════════════WAL══════════════╗
	║                              ║
	║ ┏━━━━━━━━━━Entry━━━━━━━━━━━┓ ║
	║ ┃ ┌──────────────────────┐ ┃ ║
	║ ┃ │     Flag <uint8>     │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │ len(Series) <uint16> │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │   Series <byte...>   │ ┃ ║
	║ ┃ ├──────────────────────┤ ┃ ║
	║ ┃ │  Checksum <uint32>   │ ┃ ║
	║ ┃ └──────────────────────┘ ┃ ║
	║ ┗━━━━━━━━━━━━━━━━━━━━━━━━━━┛ ║
	║             ...              ║
	╚══════════════════════════════╝

The LSB of the `Flag` byte is used to indicate a tombstone on the series.

New series keys are appended to the WAL until the WAL reaches a size or time
based threshold and can be compacted into an inverted index.


#### Compaction Types

There are two separate types of compaction that can occur:

1. Fast compaction -- compacts a set of WAL entries into an inverted index. Each
   new index will layer on top of the previous one and searches will need to
   combine each layered index.

2. Full compaction -- compacts multiple inverted indexes into a single index.


## Hash Indexing

All hash indexes within the Measurement, Tag Key, and Tag Value blocks will use
[Robin Hood Hashing](robin-hood) which minimizes minimizes worst case search
time.

[robin-hood]: https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.pdf

