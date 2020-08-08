# bcache Input Plugin

The `bcache` input gathers bcache stat from stats_total directory and dirty_data file.

- Telegraf minimum version: Telegraf 0.2.0
- Plugin minimum tested version: 0.2.0

### Configuration

This section contains the default TOML to configure the plugin.  You can
generate it using `telegraf --usage bcache`.

```toml
# Read metrics of bcache from stats_total and dirty_data
[[inputs.bcache]]
  ## Bcache sets path
  ## If not specified, then default is:
  bcachePath = "/sys/fs/bcache"

  ## By default, telegraf gather stats for all bcache devices
  ## Setting devices will restrict the stats to the specified
  ## bcache devices.
  bcacheDevs = ["bcache0"]
```

### Metrics


- dirty_data
  - Amount of dirty data for this backing device in the cache. Continuously updated unlike the cache set's version, but may be slightly off.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, bytes)
- bypassed
  - Amount of IO (both reads and writes) that has bypassed the cache
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, bytes)
- cache_bypass_hits
  - Hits for IO that is intended to skip the cache are still counted, but broken out here.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
- cache_bypass_misses
  - Misses for IO that is intended to skip the cache are still counted, but broken out here.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
- cache_hit_ratio
  - The ratio of cache hits to misses.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, percent)
- cache_hits
  - Hits are counted per individual IO as bcache sees them; a partial hit is counted as a miss.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
- cache_misses
  - Misses are counted per individual IO as bcache sees them; a partial hit is counted as a miss.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
- cache_miss_collisions
  - Counts instances where data was going to be inserted into the cache from a cache miss, but raced with a write and data was already present (usually 0 since the synchronization for cache misses was rewritten)
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
- cache_readaheads
  - Count of times readahead occurred.
  - tags:
    - backing_dev
    - bcache_dev
  - fields:
    - value (int, count)
    

### Example Output

This section shows example output in Line Protocol format.  You can often use
`telegraf --input-filter bcache --test` or use the `file` output to get
this information.

```
bcache_dirty_data,backing_dev=md10,bcache_dev=bcache0 value=11639194i
bcache_bypassed,backing_dev=md10,bcache_dev=bcache0 value=5167704440832i
bcache_cache_bypass_hits,backing_dev=md10,bcache_dev=bcache0 value=146270986i
bcache_cache_bypass_misses,backing_dev=md10,bcache_dev=bcache0 value=0i
bcache_cache_hit_ratio,backing_dev=md10,bcache_dev=bcache0 value=90i
bcache_cache_hits,backing_dev=md10,bcache_dev=bcache0 value=511941651i
bcache_cache_miss_collisions,backing_dev=md10,bcache_dev=bcache0 value=157678i
bcache_cache_misses,backing_dev=md10,bcache_dev=bcache0 value=50647396i
bcache_cache_readaheads,backing_dev=md10,bcache_dev=bcache0 value=0i
```
