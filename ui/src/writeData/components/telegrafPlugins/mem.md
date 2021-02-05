# Memory Input Plugin

The mem plugin collects system memory metrics.

For a more complete explanation of the difference between *used* and
*actual_used* RAM, see [Linux ate my ram](http://www.linuxatemyram.com/).

### Configuration:
```toml
# Read metrics about memory usage
[[inputs.mem]]
  # no configuration
```

### Metrics:

Available fields are dependent on platform.

- mem
  - fields:
    - active (integer, Darwin, FreeBSD, Linux, OpenBSD)
    - available (integer)
    - available_percent (float)
    - buffered (integer, FreeBSD, Linux)
    - cached (integer, FreeBSD, Linux, OpenBSD)
    - commit_limit (integer, Linux)
    - committed_as (integer, Linux)
    - dirty (integer, Linux)
    - free (integer, Darwin, FreeBSD, Linux, OpenBSD)
    - high_free (integer, Linux)
    - high_total (integer, Linux)
    - huge_pages_free (integer, Linux)
    - huge_page_size (integer, Linux)
    - huge_pages_total (integer, Linux)
    - inactive (integer, Darwin, FreeBSD, Linux, OpenBSD)
    - laundry (integer, FreeBSD)
    - low_free (integer, Linux)
    - low_total (integer, Linux)
    - mapped (integer, Linux)
    - page_tables (integer, Linux)
    - shared (integer, Linux)
    - slab (integer, Linux)
    - sreclaimable (integer, Linux)
    - sunreclaim (integer, Linux)
    - swap_cached (integer, Linux)
    - swap_free (integer, Linux)
    - swap_total (integer, Linux)
    - total (integer)
    - used (integer)
    - used_percent (float)
    - vmalloc_chunk (integer, Linux)
    - vmalloc_total (integer, Linux)
    - vmalloc_used (integer, Linux)
    - wired (integer, Darwin, FreeBSD, OpenBSD)
    - write_back (integer, Linux)
    - write_back_tmp (integer, Linux)

### Example Output:
```
mem active=9299595264i,available=16818249728i,available_percent=80.41654254645131,buffered=2383761408i,cached=13316689920i,commit_limit=14751920128i,committed_as=11781156864i,dirty=122880i,free=1877688320i,high_free=0i,high_total=0i,huge_page_size=2097152i,huge_pages_free=0i,huge_pages_total=0i,inactive=7549939712i,low_free=0i,low_total=0i,mapped=416763904i,page_tables=19787776i,shared=670679040i,slab=2081071104i,sreclaimable=1923395584i,sunreclaim=157675520i,swap_cached=1302528i,swap_free=4286128128i,swap_total=4294963200i,total=20913917952i,used=3335778304i,used_percent=15.95004011996231,vmalloc_chunk=0i,vmalloc_total=35184372087808i,vmalloc_used=0i,wired=0i,write_back=0i,write_back_tmp=0i 1574712869000000000
```
