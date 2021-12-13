# IOx Data Organization and Lifecycle



## Data Organization
Figure 1 illustrates an `IOx Server` which is a columnar database management system (DBMS). An IOx Serves includes many `databases`, each represents an isolated dataset from an organization or user. The IOx Server in Figure 1 consists of `p` databases. Each database has as many `tables` as needed. Data of each table is partitioned on a specified partition key which is an expression of the table column(s). In the example of Figure 1, `Table 1` is partitioned by date which is an expression on a time column of `Table 1`. `Partition` data is physically split into many chunks depending on the table's flow of ingested data which will be described in the next section, Data Life Cycle. Each chunk contains a subset of rows of its table partition on a subset of columns of the table. For example, `Chunk 1` has 2 rows of data on columns `col1`, `col2`, and `col3` while `Chunk 2` includes 3 rows on `col1` and `col4`. Since every chunk can consist of data of the same or different columns, a chunk has it own `schema` defined with it. `Chunk 1`'s schema is {`col1`, `col2`, `col3`} (and their corresponding data types) and `Chunk 2`'s schema is {`col1`, `col4`}. Same name column, `col1`, represents the same column of the table and must have the same data type. 

```text  
                                                                 ┌───────────┐                                    
                                                                 │IOx Server │                        IOx Server  
                                                                 └───────────┘                                    
                                                                       │                                          
                                                        ┌──────────────┼────────────────┐                         
                                                        ▼              ▼                ▼                         
                                              ┌───────────┐                     ┌────────────┐                    
                                              │Database 1 │           ...       │ Database p │        Databases   
                                              └───────────┘                     └────────────┘                    
                                                    │                                                             
                                     ┌──────────────┼─────────────┐                                               
                                     ▼              ▼             ▼                                               
                               ┌──────────┐                 ┌──────────┐                                          
                               │ Table 1  │       ...       │ Table n  │                                Tables    
                               └──────────┘                 └──────────┘                                          
                                     │                            │                                               
                      ┌──────────────┼──────────────┐             │                                               
                      ▼              ▼              ▼             ▼                                               
               ┌────────────┐                ┌────────────┐                                                       
               │Partition 1 │       ...      │Partition m │      ...                                 Partitions   
               │(2021-12-10)│                │(2021-12-20)│                                                       
               └────────────┘                └──────┬─────┘                                                       
                      │                             │                                                             
        ┌─────────────┼─────────────┐               │                                                             
        ▼             ▼             ▼               ▼                                                             
┌──────────────┐              ┌───────────┐                                                                       
│    Chunk 1   │     ...      │  Chunk 2  │        ...                                                 Chunks     
│              │              │           │                                                                       
│col1 col2 col3│              │ col1 col4 │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
│---- ---- ----│              │ ---- ---- │                                                                       
└──────────────┘              │ ---- ---- │                                                                       
                              └───────────┘                                                                       
 
Figure 1: Data organization in an IOx Server
```

Chunk is considered the smallest unit of block of data in IOx and the central discussion of the rest of this document. IOx does not (yet) support direct data modification but does allow deletion[^del] which means a modification can be done through a deletion and an ingestion. Another way to modify values of non-primary-key columns in IOx is to reload data of that row using same key values but different non-key ones. These duplicated rows will be deduplicated during compaction (see next section) and/or eliminated at Query time.[^dup]
[^del]: `Deletion` is large topic that deserves its own document.
[^dup]: The detail of `duplication` and `deduplication` during compaction and query are parts of a large topic that deserve another document.

### Chunk Types
A `Chunk` in IOx is an abstraction object defined in the code as [DBChunk](https://github.com/influxdata/influxdb_iox/blob/12c40b0f0f93e94e483015f9104639a1f766d594/server/src/db/chunk.rs#L78). To optimize the Data LifeCycle and query performance, IOx supports these types of chunks: O-MUB, F-MUB, RUB, OS, L-OS.

1. O-MUB: Open MUtable Buffer chunk is optimized for writes and the only chunk type that accepts ingesting data. O-MUB is kept in memory but its data is neither sorted nor encoded.[^type]
1. F-MUB: Frozen MUtable BUffer chunk has the same format as O-MUB (in memory, not sorted, not encoded) but it no longer accepts any writes. This type of chunk is used as a transition while chunk is being moved from optimized-for-writes to optimized-for-reads.
1. RUB: Read Buffer chunk is optimized for reads and does not accept writes. RUB is kept in memory and its data is sorted an encoded on the chunk's primary key. Note that since a chunk includes a subset of its table columns, Chunk's primary key can also be a subset of its table's primary key.
1. OS: Object Store chunk is a parquet-format file of a chunk stored in a durable cloud storage such as Amazon S3 (IOx also supports Azure and Google Clouds). OS chunk inherits all sorting and encoding properties of Parquet and RUB so its size is smallest in all chunk type.
1. L-OS: Local-cached Object Store chunk is a kind of OS but stored on local disk of IOx Server. 

[^type]: The detailed format of each chunk type is out of scope of this document

Depending which stage in the lifecycle a chunk is, it will represented by one or many types above.

### Stages of a Chunk

Before digging into the chunk lifecycle, let us go through the stages of a chunk named [ChunkStage](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/server/src/db/catalog/chunk.rs#L130) in the code.

```text

                                                       ┌───────────────────┐
                                                       │     Persisted     │
                                                       │ ┌───────────────┐ │
┌───────────┐       ┌──────────────────────────┐       │ │RUB (optional) │ │
│   Open    │       │          Frozen          │       │ └───────────────┘ │
│           │       │                          │       │ ┌───────────────┐ │
│ ┌───────┐ │──────▶│┌───────┐        ┌───────┐│──────▶│ │      OS       │ │
│ │ O-MUB │ │       ││ F-MUB ├───────▶│  RUB  ││       │ └───────────────┘ │
│ └───────┘ │       │└───────┘        └───────┘│       │ ┌───────────────┐ │
└───────────┘       └──────────────────────────┘       │ │L-OS (optional)│ │
                                                       │ └───────────────┘ │
                                                       └───────────────────┘
Figure 2: Stages of a chunk
```

## Data Life Cycle