# ZFS plugin

This ZFS plugin provides metrics from your ZFS filesystems. It supports ZFS on
Linux and FreeBSD. It gets ZFS stat from `/proc/spl/kstat/zfs` on Linux and
from `sysctl` and `zpool` on FreeBSD.

### Configuration:

```toml
[[inputs.zfs]]
  ## ZFS kstat path. Ignored on FreeBSD
  ## If not specified, then default is:
  # kstatPath = "/proc/spl/kstat/zfs"

  ## By default, telegraf gather all zfs stats
  ## Override the stats list using the kstatMetrics array:
  ## For FreeBSD, the default is:
  # kstatMetrics = ["arcstats", "zfetchstats", "vdev_cache_stats"]
  ## For Linux, the default is:
  # kstatMetrics = ["abdstats", "arcstats", "dnodestats", "dbufcachestats",
  #     "dmu_tx", "fm", "vdev_mirror_stats", "zfetchstats", "zil"]

  ## By default, don't gather zpool stats
  # poolMetrics = false
```

### Measurements & Fields:

By default this plugin collects metrics about ZFS internals and pool.
These metrics are either counters or measure sizes
in bytes. These metrics will be in the `zfs` measurement with the field
names listed bellow.

If `poolMetrics` is enabled then additional metrics will be gathered for
each pool.

- zfs
    With fields listed bellow.

#### ARC Stats (FreeBSD and Linux)

- arcstats_allocated (FreeBSD only)
- arcstats_anon_evict_data (Linux only)
- arcstats_anon_evict_metadata (Linux only)
- arcstats_anon_evictable_data (FreeBSD only)
- arcstats_anon_evictable_metadata (FreeBSD only)
- arcstats_anon_size
- arcstats_arc_loaned_bytes (Linux only)
- arcstats_arc_meta_limit
- arcstats_arc_meta_max
- arcstats_arc_meta_min (FreeBSD only)
- arcstats_arc_meta_used
- arcstats_arc_no_grow (Linux only)
- arcstats_arc_prune (Linux only)
- arcstats_arc_tempreserve (Linux only)
- arcstats_c
- arcstats_c_max
- arcstats_c_min
- arcstats_data_size
- arcstats_deleted
- arcstats_demand_data_hits
- arcstats_demand_data_misses
- arcstats_demand_hit_predictive_prefetch (FreeBSD only)
- arcstats_demand_metadata_hits
- arcstats_demand_metadata_misses
- arcstats_duplicate_buffers
- arcstats_duplicate_buffers_size
- arcstats_duplicate_reads
- arcstats_evict_l2_cached
- arcstats_evict_l2_eligible
- arcstats_evict_l2_ineligible
- arcstats_evict_l2_skip (FreeBSD only)
- arcstats_evict_not_enough (FreeBSD only)
- arcstats_evict_skip
- arcstats_hash_chain_max
- arcstats_hash_chains
- arcstats_hash_collisions
- arcstats_hash_elements
- arcstats_hash_elements_max
- arcstats_hdr_size
- arcstats_hits
- arcstats_l2_abort_lowmem
- arcstats_l2_asize
- arcstats_l2_cdata_free_on_write
- arcstats_l2_cksum_bad
- arcstats_l2_compress_failures
- arcstats_l2_compress_successes
- arcstats_l2_compress_zeros
- arcstats_l2_evict_l1cached (FreeBSD only)
- arcstats_l2_evict_lock_retry
- arcstats_l2_evict_reading
- arcstats_l2_feeds
- arcstats_l2_free_on_write
- arcstats_l2_hdr_size
- arcstats_l2_hits
- arcstats_l2_io_error
- arcstats_l2_misses
- arcstats_l2_read_bytes
- arcstats_l2_rw_clash
- arcstats_l2_size
- arcstats_l2_write_buffer_bytes_scanned (FreeBSD only)
- arcstats_l2_write_buffer_iter (FreeBSD only)
- arcstats_l2_write_buffer_list_iter (FreeBSD only)
- arcstats_l2_write_buffer_list_null_iter (FreeBSD only)
- arcstats_l2_write_bytes
- arcstats_l2_write_full (FreeBSD only)
- arcstats_l2_write_in_l2 (FreeBSD only)
- arcstats_l2_write_io_in_progress (FreeBSD only)
- arcstats_l2_write_not_cacheable (FreeBSD only)
- arcstats_l2_write_passed_headroom (FreeBSD only)
- arcstats_l2_write_pios (FreeBSD only)
- arcstats_l2_write_spa_mismatch (FreeBSD only)
- arcstats_l2_write_trylock_fail (FreeBSD only)
- arcstats_l2_writes_done
- arcstats_l2_writes_error
- arcstats_l2_writes_hdr_miss (Linux only)
- arcstats_l2_writes_lock_retry (FreeBSD only)
- arcstats_l2_writes_sent
- arcstats_memory_direct_count (Linux only)
- arcstats_memory_indirect_count (Linux only)
- arcstats_memory_throttle_count
- arcstats_meta_size (Linux only)
- arcstats_mfu_evict_data (Linux only)
- arcstats_mfu_evict_metadata (Linux only)
- arcstats_mfu_ghost_evict_data (Linux only)
- arcstats_mfu_ghost_evict_metadata (Linux only)
- arcstats_metadata_size (FreeBSD only)
- arcstats_mfu_evictable_data (FreeBSD only)
- arcstats_mfu_evictable_metadata (FreeBSD only)
- arcstats_mfu_ghost_evictable_data (FreeBSD only)
- arcstats_mfu_ghost_evictable_metadata (FreeBSD only)
- arcstats_mfu_ghost_hits
- arcstats_mfu_ghost_size
- arcstats_mfu_hits
- arcstats_mfu_size
- arcstats_misses
- arcstats_mru_evict_data (Linux only)
- arcstats_mru_evict_metadata (Linux only)
- arcstats_mru_ghost_evict_data (Linux only)
- arcstats_mru_ghost_evict_metadata (Linux only)
- arcstats_mru_evictable_data (FreeBSD only)
- arcstats_mru_evictable_metadata (FreeBSD only)
- arcstats_mru_ghost_evictable_data (FreeBSD only)
- arcstats_mru_ghost_evictable_metadata (FreeBSD only)
- arcstats_mru_ghost_hits
- arcstats_mru_ghost_size
- arcstats_mru_hits
- arcstats_mru_size
- arcstats_mutex_miss
- arcstats_other_size
- arcstats_p
- arcstats_prefetch_data_hits
- arcstats_prefetch_data_misses
- arcstats_prefetch_metadata_hits
- arcstats_prefetch_metadata_misses
- arcstats_recycle_miss (Linux only)
- arcstats_size
- arcstats_sync_wait_for_async (FreeBSD only)

#### Zfetch Stats (FreeBSD and Linux)

- zfetchstats_bogus_streams (Linux only)
- zfetchstats_colinear_hits (Linux only)
- zfetchstats_colinear_misses (Linux only)
- zfetchstats_hits
- zfetchstats_max_streams (FreeBSD only)
- zfetchstats_misses
- zfetchstats_reclaim_failures (Linux only)
- zfetchstats_reclaim_successes (Linux only)
- zfetchstats_streams_noresets (Linux only)
- zfetchstats_streams_resets (Linux only)
- zfetchstats_stride_hits (Linux only)
- zfetchstats_stride_misses (Linux only)

#### Vdev Cache Stats (FreeBSD)

- vdev_cache_stats_delegations
- vdev_cache_stats_hits
- vdev_cache_stats_misses

#### Pool Metrics (optional)

On Linux (reference: kstat accumulated time and queue length statistics):

- zfs_pool
    - nread (integer, bytes)
    - nwritten (integer, bytes)
    - reads (integer, count)
    - writes (integer, count)
    - wtime (integer, nanoseconds) 
    - wlentime (integer, queuelength * nanoseconds)
    - wupdate (integer, timestamp)
    - rtime (integer, nanoseconds)
    - rlentime (integer, queuelength * nanoseconds)
    - rupdate (integer, timestamp)
    - wcnt (integer, count)
    - rcnt (integer, count)

On FreeBSD:

- zfs_pool
    - allocated (integer, bytes)
    - capacity (integer, bytes)
    - dedupratio (float, ratio)
    - free (integer, bytes)
    - size (integer, bytes)
    - fragmentation (integer, percent)

### Tags:

- ZFS stats (`zfs`) will have the following tag:
    - pools - A `::` concatenated list of all ZFS pools on the machine.

- Pool metrics (`zfs_pool`) will have the following tag:
    - pool - with the name of the pool which the metrics are for.
    - health - the health status of the pool. (FreeBSD only)

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter zfs --test
* Plugin: zfs, Collection 1
> zfs_pool,health=ONLINE,pool=zroot allocated=1578590208i,capacity=2i,dedupratio=1,fragmentation=1i,free=64456531968i,size=66035122176i 1464473103625653908
> zfs,pools=zroot arcstats_allocated=4167764i,arcstats_anon_evictable_data=0i,arcstats_anon_evictable_metadata=0i,arcstats_anon_size=16896i,arcstats_arc_meta_limit=10485760i,arcstats_arc_meta_max=115269568i,arcstats_arc_meta_min=8388608i,arcstats_arc_meta_used=51977456i,arcstats_c=16777216i,arcstats_c_max=41943040i,arcstats_c_min=16777216i,arcstats_data_size=0i,arcstats_deleted=1699340i,arcstats_demand_data_hits=14836131i,arcstats_demand_data_misses=2842945i,arcstats_demand_hit_predictive_prefetch=0i,arcstats_demand_metadata_hits=1655006i,arcstats_demand_metadata_misses=830074i,arcstats_duplicate_buffers=0i,arcstats_duplicate_buffers_size=0i,arcstats_duplicate_reads=123i,arcstats_evict_l2_cached=0i,arcstats_evict_l2_eligible=332172623872i,arcstats_evict_l2_ineligible=6168576i,arcstats_evict_l2_skip=0i,arcstats_evict_not_enough=12189444i,arcstats_evict_skip=195190764i,arcstats_hash_chain_max=2i,arcstats_hash_chains=10i,arcstats_hash_collisions=43134i,arcstats_hash_elements=2268i,arcstats_hash_elements_max=6136i,arcstats_hdr_size=565632i,arcstats_hits=16515778i,arcstats_l2_abort_lowmem=0i,arcstats_l2_asize=0i,arcstats_l2_cdata_free_on_write=0i,arcstats_l2_cksum_bad=0i,arcstats_l2_compress_failures=0i,arcstats_l2_compress_successes=0i,arcstats_l2_compress_zeros=0i,arcstats_l2_evict_l1cached=0i,arcstats_l2_evict_lock_retry=0i,arcstats_l2_evict_reading=0i,arcstats_l2_feeds=0i,arcstats_l2_free_on_write=0i,arcstats_l2_hdr_size=0i,arcstats_l2_hits=0i,arcstats_l2_io_error=0i,arcstats_l2_misses=0i,arcstats_l2_read_bytes=0i,arcstats_l2_rw_clash=0i,arcstats_l2_size=0i,arcstats_l2_write_buffer_bytes_scanned=0i,arcstats_l2_write_buffer_iter=0i,arcstats_l2_write_buffer_list_iter=0i,arcstats_l2_write_buffer_list_null_iter=0i,arcstats_l2_write_bytes=0i,arcstats_l2_write_full=0i,arcstats_l2_write_in_l2=0i,arcstats_l2_write_io_in_progress=0i,arcstats_l2_write_not_cacheable=380i,arcstats_l2_write_passed_headroom=0i,arcstats_l2_write_pios=0i,arcstats_l2_write_spa_mismatch=0i,arcstats_l2_write_trylock_fail=0i,arcstats_l2_writes_done=0i,arcstats_l2_writes_error=0i,arcstats_l2_writes_lock_retry=0i,arcstats_l2_writes_sent=0i,arcstats_memory_throttle_count=0i,arcstats_metadata_size=17014784i,arcstats_mfu_evictable_data=0i,arcstats_mfu_evictable_metadata=16384i,arcstats_mfu_ghost_evictable_data=5723648i,arcstats_mfu_ghost_evictable_metadata=10709504i,arcstats_mfu_ghost_hits=1315619i,arcstats_mfu_ghost_size=16433152i,arcstats_mfu_hits=7646611i,arcstats_mfu_size=305152i,arcstats_misses=3676993i,arcstats_mru_evictable_data=0i,arcstats_mru_evictable_metadata=0i,arcstats_mru_ghost_evictable_data=0i,arcstats_mru_ghost_evictable_metadata=80896i,arcstats_mru_ghost_hits=324250i,arcstats_mru_ghost_size=80896i,arcstats_mru_hits=8844526i,arcstats_mru_size=16693248i,arcstats_mutex_miss=354023i,arcstats_other_size=34397040i,arcstats_p=4172800i,arcstats_prefetch_data_hits=0i,arcstats_prefetch_data_misses=0i,arcstats_prefetch_metadata_hits=24641i,arcstats_prefetch_metadata_misses=3974i,arcstats_size=51977456i,arcstats_sync_wait_for_async=0i,vdev_cache_stats_delegations=779i,vdev_cache_stats_hits=323123i,vdev_cache_stats_misses=59929i,zfetchstats_hits=0i,zfetchstats_max_streams=0i,zfetchstats_misses=0i 1464473103634124908
```

### Description

A short description for some of the metrics.

#### ARC Stats

`arcstats_hits` Total amount of cache hits in the arc.

`arcstats_misses` Total amount of cache misses in the arc.

`arcstats_demand_data_hits` Amount of cache hits for demand data, this is what matters (is good) for your application/share.

`arcstats_demand_data_misses` Amount of cache misses for demand data, this is what matters (is bad) for your application/share.

`arcstats_demand_metadata_hits` Amount of cache hits for demand metadata, this matters (is good) for getting filesystem data (ls,find,…)

`arcstats_demand_metadata_misses` Amount of cache misses for demand metadata, this matters (is bad) for getting filesystem data (ls,find,…)

`arcstats_prefetch_data_hits` The zfs prefetcher tried to prefetch something, but it was already cached (boring)

`arcstats_prefetch_data_misses` The zfs prefetcher prefetched something which was not in the cache (good job, could become a demand hit in the future)

`arcstats_prefetch_metadata_hits` Same as above, but for metadata

`arcstats_prefetch_metadata_misses` Same as above, but for metadata

`arcstats_mru_hits` Cache hit in the “most recently used cache”, we move this to the mfu cache.

`arcstats_mru_ghost_hits` Cache hit in the “most recently used ghost list” we had this item in the cache, but evicted it, maybe we should increase the mru cache size.

`arcstats_mfu_hits` Cache hit in the “most frequently used cache” we move this to the beginning of the mfu cache.

`arcstats_mfu_ghost_hits` Cache hit in the “most frequently used ghost list” we had this item in the cache, but evicted it, maybe we should increase the mfu cache size.

`arcstats_allocated` New data is written to the cache.

`arcstats_deleted` Old data is evicted (deleted) from the cache.

`arcstats_evict_l2_cached` We evicted something from the arc, but its still cached in the l2 if we need it.

`arcstats_evict_l2_eligible` We evicted something from the arc, and it’s not in the l2 this is sad. (maybe we hadn’t had enough time to store it there)

`arcstats_evict_l2_ineligible` We evicted something which cannot be stored in the l2.
 Reasons could be:
 - We have multiple pools, we evicted something from a pool without an l2 device.
 - The zfs property secondary cache.

`arcstats_c` Arc target size, this is the size the system thinks the arc should have.

`arcstats_size` Total size of the arc.

`arcstats_l2_hits` Hits to the L2 cache. (It was not in the arc, but in the l2 cache)

`arcstats_l2_misses` Miss to the L2 cache. (It was not in the arc, and not in the l2 cache)

`arcstats_l2_size` Size of the l2 cache.

`arcstats_l2_hdr_size` Size of the metadata in the arc (ram) used to manage (lookup if something is in the l2) the l2 cache.

#### Zfetch Stats

`zfetchstats_hits` Counts the number of cache hits, to items which are in the cache because of the prefetcher.

`zfetchstats_misses` Counts the number of prefetch cache misses.

`zfetchstats_colinear_hits` Counts the number of cache hits, to items which are in the cache because of the prefetcher (prefetched linear reads)

`zfetchstats_stride_hits` Counts the number of cache hits, to items which are in the cache because of the prefetcher (prefetched stride reads)

#### Vdev Cache Stats (FreeBSD only)
note: the vdev cache is deprecated in some ZFS implementations

`vdev_cache_stats_hits` Hits to the vdev (device level) cache.

`vdev_cache_stats_misses` Misses to the vdev (device level) cache.

#### ABD Stats (Linux Only)
ABD is a linear/scatter dual typed buffer for ARC

`abdstats_linear_cnt` number of linear ABDs which are currently allocated

`abdstats_linear_data_size` amount of data stored in all linear ABDs

`abdstats_scatter_cnt` number of scatter ABDs which are currently allocated

`abdstats_scatter_data_size` amount of data stored in all scatter ABDs

#### DMU Stats (Linux Only)

`dmu_tx_dirty_throttle` counts when writes are throttled due to the amount of dirty data growing too large

`dmu_tx_memory_reclaim` counts when memory is low and throttling activity

`dmu_tx_memory_reserve` counts when memory footprint of the txg exceeds the ARC size

#### Fault Management Ereport errors (Linux Only)

`fm_erpt-dropped` counts when an error report cannot be created (eg available memory is too low)

#### ZIL (Linux Only)
note: ZIL measurements are system-wide, neither per-pool nor per-dataset

`zil_commit_count` counts when ZFS transactions are committed to a ZIL
