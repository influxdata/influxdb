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
A `Chunk` in IOx is an abstract object defined in the code as a [DbChunk](https://github.com/influxdata/influxdb_iox/blob/12c40b0f0f93e94e483015f9104639a1f766d594/server/src/db/chunk.rs#L78). To optimize the Data LifeCycle and Query Performance, IOx implements these types of physical chunks for a DbChunk: O-MUB, F-MUB, RUB, OS, L-OS.

1. O-MUB: **O**pen **MU**table **B**uffer chunk is optimized for writes and the only chunk type that accepts ingesting data. O-MUB is an in-memory chunk but its data is neither sorted nor encoded.[^type]
1. F-MUB: **F**rozen **MU**table **B**uffer chunk has the same format as O-MUB (in memory, not sorted, not encoded) but it no longer accepts writes. It is used as a transition chunk while its data is being moved from optimized-for-writes to optimized-for-reads.
1. RUB: **R**ead **B**uffer chunk is optimized for reads and does not accept writes. RUB is kept in memory and its data is sorted and encoded on the chunk's primary key. Note that since a chunk stores data of a subset of its table columns, Chunk's primary key can also be a subset of its table's primary key.
1. OS: **O**bject **S**tore chunk is a parquet file of a chunk stored in a durable cloud storage such as Amazon S3 (IOx also supports Azure and Google Clouds). Because an OS is always created from a RUB, it inherits all sorting and encoding properties of the corresponding RUB and Parquet.
1. L-OS: **L**ocal-cached **O**bject **S**tore chunk is an OS cached on local non volatile memory of IOx Server. 

[^type]: The detailed format of each chunk type is out of scope of this document

Depending on which stage of the lifecycle a chunk is in, it will be represented by one or a few physical chunk types above.

### Stages of a Chunk

Before digging into Data Lifecycle, let us look into the stages of a chunk implemented as [ChunkStage](https://github.com/influxdata/influxdb_iox/blob/76befe94ad14cd121d6fc5c58aa112997d9e211a/server/src/db/catalog/chunk.rs#L130). A chunk goes through three stages demonstrated in Figure 2: `Open`, `Frozen`, and `Persisted`. 
* When data is ingested into IOx, it will be written into an open chunk which is an `O-MUB`.
* When triggered by some manual or automatic event of the lifecycle (described in next section), the open chunk will be frozen, first to `F-MUB` then transitioned to `RUB`.
* When the `RUB` is persisted to an `OS` chunk, it stage will be moved to persisted. Unlike the `Open` and `Frozen` stages that are represented by only one type of chunk at a moment in time, the `Persisted` stage can be represented by three chunk types at a time: `RUB`, `OS` and `L-OS` that store the same data for the purpose of query performance. When a query needs to read data of a persisted chunk stage, it will first look for `RUB`, but, if not available, will look for `L-OS`, and then `OS`. `RUB` will be unloaded from the persisted stage if IOx memory runs low, and reloaded if data of that chunk is queried a lot and IOx memory is underused. `L-OS` will be created when data of that chunk is also read lot but no memory available to load its `RUB` back.

```text
                                                       ┌───────────────────┐
                                                       │     Persisted     │
                                                       │                   │
┌───────────┐       ┌──────────────────────────┐       │ ┌───────────────┐ │
│   Open    │       │          Frozen          │       │ │RUB (optional) │ │
│           │       │                          │       │ └───────────────┘ │
│ ┌───────┐ │──────▶│┌───────┐        ┌───────┐│──────▶│ ┌───────────────┐ │
│ │ O-MUB │ │       ││ F-MUB ├───────▶│  RUB  ││       │ │      OS       │ │
│ └───────┘ │       │└───────┘        └───────┘│       │ └───────────────┘ │
└───────────┘       └──────────────────────────┘       │ ┌───────────────┐ │
                                                       │ │L-OS (optional)│ │
                                                       │ └───────────────┘ │
                                                       └───────────────────┘
Figure 2: Stages of a C hunk
```

Now let us see how data of chunks are transformed in IOx's Data LifeCycle.

## Data Life Cycle