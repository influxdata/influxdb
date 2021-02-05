# Kernel VMStat Input Plugin

The kernel_vmstat plugin gathers virtual memory statistics 
by reading /proc/vmstat. For a full list of available fields see the 
/proc/vmstat section of the [proc man page](http://man7.org/linux/man-pages/man5/proc.5.html).
For a better idea of what each field represents, see the 
[vmstat man page](http://linux.die.net/man/8/vmstat).


```
/proc/vmstat
kernel/system statistics. Common entries include (from http://www.linuxinsight.com/proc_vmstat.html):

Number of pages that are dirty, under writeback or unstable:

nr_dirty 1550
nr_writeback 0
nr_unstable 0

Number of pages allocated to page tables, mapped by files or allocated by the kernel slab allocator:

nr_page_table_pages 699
nr_mapped 139596
nr_slab 42723

Number of pageins and pageouts (since the last boot):

pgpgin 33754195
pgpgout 38985992

Number of swapins and swapouts (since the last boot):

pswpin 2473
pswpout 2995

Number of page allocations per zone (since the last boot):

pgalloc_high 0
pgalloc_normal 110123213
pgalloc_dma32 0
pgalloc_dma 415219

Number of page frees, activations and deactivations (since the last boot):

pgfree 110549163
pgactivate 4509729
pgdeactivate 2136215

Number of minor and major page faults (since the last boot):

pgfault 80663722
pgmajfault 49813

Number of page refills (per zone, since the last boot):

pgrefill_high 0
pgrefill_normal 5817500
pgrefill_dma32 0
pgrefill_dma 149176

Number of page steals (per zone, since the last boot):

pgsteal_high 0
pgsteal_normal 10421346
pgsteal_dma32 0
pgsteal_dma 142196

Number of pages scanned by the kswapd daemon (per zone, since the last boot):

pgscan_kswapd_high 0
pgscan_kswapd_normal 10491424
pgscan_kswapd_dma32 0
pgscan_kswapd_dma 156130

Number of pages reclaimed directly (per zone, since the last boot):

pgscan_direct_high 0
pgscan_direct_normal 11904
pgscan_direct_dma32 0
pgscan_direct_dma 225

Number of pages reclaimed via inode freeing (since the last boot):

pginodesteal 11

Number of slab objects scanned (since the last boot):

slabs_scanned 8926976

Number of pages reclaimed by kswapd (since the last boot):

kswapd_steal 10551674

Number of pages reclaimed by kswapd via inode freeing (since the last boot):

kswapd_inodesteal 338730

Number of kswapd's calls to page reclaim (since the last boot):

pageoutrun 181908

Number of direct reclaim calls (since the last boot):

allocstall 160

Miscellaneous statistics:

pgrotated 3781
nr_bounce 0
```

### Configuration:

```toml
# Get kernel statistics from /proc/vmstat
[[inputs.kernel_vmstat]]
  # no configuration
```

### Measurements & Fields:

- kernel_vmstat
    - nr_free_pages (integer, `nr_free_pages`)
    - nr_inactive_anon (integer, `nr_inactive_anon`)
    - nr_active_anon (integer, `nr_active_anon`)
    - nr_inactive_file (integer, `nr_inactive_file`)
    - nr_active_file (integer, `nr_active_file`)
    - nr_unevictable (integer, `nr_unevictable`)
    - nr_mlock (integer, `nr_mlock`)
    - nr_anon_pages (integer, `nr_anon_pages`)
    - nr_mapped (integer, `nr_mapped`)
    - nr_file_pages (integer, `nr_file_pages`)
    - nr_dirty (integer, `nr_dirty`)
    - nr_writeback (integer, `nr_writeback`)
    - nr_slab_reclaimable (integer, `nr_slab_reclaimable`)
    - nr_slab_unreclaimable (integer, `nr_slab_unreclaimable`)
    - nr_page_table_pages (integer, `nr_page_table_pages`)
    - nr_kernel_stack (integer, `nr_kernel_stack`)
    - nr_unstable (integer, `nr_unstable`)
    - nr_bounce (integer, `nr_bounce`)
    - nr_vmscan_write (integer, `nr_vmscan_write`)
    - nr_writeback_temp (integer, `nr_writeback_temp`)
    - nr_isolated_anon (integer, `nr_isolated_anon`)
    - nr_isolated_file (integer, `nr_isolated_file`)
    - nr_shmem (integer, `nr_shmem`)
    - numa_hit (integer, `numa_hit`)
    - numa_miss (integer, `numa_miss`)
    - numa_foreign (integer, `numa_foreign`)
    - numa_interleave (integer, `numa_interleave`)
    - numa_local (integer, `numa_local`)
    - numa_other (integer, `numa_other`)
    - nr_anon_transparent_hugepages (integer, `nr_anon_transparent_hugepages`)
    - pgpgin (integer, `pgpgin`)
    - pgpgout (integer, `pgpgout`)
    - pswpin (integer, `pswpin`)
    - pswpout (integer, `pswpout`)
    - pgalloc_dma (integer, `pgalloc_dma`)
    - pgalloc_dma32 (integer, `pgalloc_dma32`)
    - pgalloc_normal (integer, `pgalloc_normal`)
    - pgalloc_movable (integer, `pgalloc_movable`)
    - pgfree (integer, `pgfree`)
    - pgactivate (integer, `pgactivate`)
    - pgdeactivate (integer, `pgdeactivate`)
    - pgfault (integer, `pgfault`)
    - pgmajfault (integer, `pgmajfault`)
    - pgrefill_dma (integer, `pgrefill_dma`)
    - pgrefill_dma32 (integer, `pgrefill_dma32`)
    - pgrefill_normal (integer, `pgrefill_normal`)
    - pgrefill_movable (integer, `pgrefill_movable`)
    - pgsteal_dma (integer, `pgsteal_dma`)
    - pgsteal_dma32 (integer, `pgsteal_dma32`)
    - pgsteal_normal (integer, `pgsteal_normal`)
    - pgsteal_movable (integer, `pgsteal_movable`)
    - pgscan_kswapd_dma (integer, `pgscan_kswapd_dma`)
    - pgscan_kswapd_dma32 (integer, `pgscan_kswapd_dma32`)
    - pgscan_kswapd_normal (integer, `pgscan_kswapd_normal`)
    - pgscan_kswapd_movable (integer, `pgscan_kswapd_movable`)
    - pgscan_direct_dma (integer, `pgscan_direct_dma`)
    - pgscan_direct_dma32 (integer, `pgscan_direct_dma32`)
    - pgscan_direct_normal (integer, `pgscan_direct_normal`)
    - pgscan_direct_movable (integer, `pgscan_direct_movable`)
    - zone_reclaim_failed (integer, `zone_reclaim_failed`)
    - pginodesteal (integer, `pginodesteal`)
    - slabs_scanned (integer, `slabs_scanned`)
    - kswapd_steal (integer, `kswapd_steal`)
    - kswapd_inodesteal (integer, `kswapd_inodesteal`)
    - kswapd_low_wmark_hit_quickly (integer, `kswapd_low_wmark_hit_quickly`)
    - kswapd_high_wmark_hit_quickly (integer, `kswapd_high_wmark_hit_quickly`)
    - kswapd_skip_congestion_wait (integer, `kswapd_skip_congestion_wait`)
    - pageoutrun (integer, `pageoutrun`)
    - allocstall (integer, `allocstall`)
    - pgrotated (integer, `pgrotated`)
    - compact_blocks_moved (integer, `compact_blocks_moved`)
    - compact_pages_moved (integer, `compact_pages_moved`)
    - compact_pagemigrate_failed (integer, `compact_pagemigrate_failed`)
    - compact_stall (integer, `compact_stall`)
    - compact_fail (integer, `compact_fail`)
    - compact_success (integer, `compact_success`)
    - htlb_buddy_alloc_success (integer, `htlb_buddy_alloc_success`)
    - htlb_buddy_alloc_fail (integer, `htlb_buddy_alloc_fail`)
    - unevictable_pgs_culled (integer, `unevictable_pgs_culled`)
    - unevictable_pgs_scanned (integer, `unevictable_pgs_scanned`)
    - unevictable_pgs_rescued (integer, `unevictable_pgs_rescued`)
    - unevictable_pgs_mlocked (integer, `unevictable_pgs_mlocked`)
    - unevictable_pgs_munlocked (integer, `unevictable_pgs_munlocked`)
    - unevictable_pgs_cleared (integer, `unevictable_pgs_cleared`)
    - unevictable_pgs_stranded (integer, `unevictable_pgs_stranded`)
    - unevictable_pgs_mlockfreed (integer, `unevictable_pgs_mlockfreed`)
    - thp_fault_alloc (integer, `thp_fault_alloc`)
    - thp_fault_fallback (integer, `thp_fault_fallback`)
    - thp_collapse_alloc (integer, `thp_collapse_alloc`)
    - thp_collapse_alloc_failed (integer, `thp_collapse_alloc_failed`)
    - thp_split (integer, `thp_split`)

### Tags:

None

### Example Output:

```
$ telegraf --config ~/ws/telegraf.conf --input-filter kernel_vmstat --test
* Plugin: kernel_vmstat, Collection 1
> kernel_vmstat allocstall=81496i,compact_blocks_moved=238196i,compact_fail=135220i,compact_pagemigrate_failed=0i,compact_pages_moved=6370588i,compact_stall=142092i,compact_success=6872i,htlb_buddy_alloc_fail=0i,htlb_buddy_alloc_success=0i,kswapd_high_wmark_hit_quickly=25439i,kswapd_inodesteal=29770874i,kswapd_low_wmark_hit_quickly=8756i,kswapd_skip_congestion_wait=0i,kswapd_steal=291534428i,nr_active_anon=2515657i,nr_active_file=2244914i,nr_anon_pages=1358675i,nr_anon_transparent_hugepages=2034i,nr_bounce=0i,nr_dirty=5690i,nr_file_pages=5153546i,nr_free_pages=78730i,nr_inactive_anon=426259i,nr_inactive_file=2366791i,nr_isolated_anon=0i,nr_isolated_file=0i,nr_kernel_stack=579i,nr_mapped=558821i,nr_mlock=0i,nr_page_table_pages=11115i,nr_shmem=541689i,nr_slab_reclaimable=459806i,nr_slab_unreclaimable=47859i,nr_unevictable=0i,nr_unstable=0i,nr_vmscan_write=6206i,nr_writeback=0i,nr_writeback_temp=0i,numa_foreign=0i,numa_hit=5113399878i,numa_interleave=35793i,numa_local=5113399878i,numa_miss=0i,numa_other=0i,pageoutrun=505006i,pgactivate=375664931i,pgalloc_dma=0i,pgalloc_dma32=122480220i,pgalloc_movable=0i,pgalloc_normal=5233176719i,pgdeactivate=122735906i,pgfault=8699921410i,pgfree=5359765021i,pginodesteal=9188431i,pgmajfault=122210i,pgpgin=219717626i,pgpgout=3495885510i,pgrefill_dma=0i,pgrefill_dma32=1180010i,pgrefill_movable=0i,pgrefill_normal=119866676i,pgrotated=60620i,pgscan_direct_dma=0i,pgscan_direct_dma32=12256i,pgscan_direct_movable=0i,pgscan_direct_normal=31501600i,pgscan_kswapd_dma=0i,pgscan_kswapd_dma32=4480608i,pgscan_kswapd_movable=0i,pgscan_kswapd_normal=287857984i,pgsteal_dma=0i,pgsteal_dma32=4466436i,pgsteal_movable=0i,pgsteal_normal=318463755i,pswpin=2092i,pswpout=6206i,slabs_scanned=93775616i,thp_collapse_alloc=24857i,thp_collapse_alloc_failed=102214i,thp_fault_alloc=346219i,thp_fault_fallback=895453i,thp_split=9817i,unevictable_pgs_cleared=0i,unevictable_pgs_culled=1531i,unevictable_pgs_mlocked=6988i,unevictable_pgs_mlockfreed=0i,unevictable_pgs_munlocked=6988i,unevictable_pgs_rescued=5426i,unevictable_pgs_scanned=0i,unevictable_pgs_stranded=0i,zone_reclaim_failed=0i 1459455200071462843 
```
