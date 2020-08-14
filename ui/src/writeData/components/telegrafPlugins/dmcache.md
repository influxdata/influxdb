# DMCache Input Plugin

This plugin provide a native collection for dmsetup based statistics for dm-cache.

This plugin requires sudo, that is why you should setup and be sure that the telegraf is able to execute sudo without a password.

`sudo /sbin/dmsetup status --target cache` is the full command that telegraf will run for debugging purposes.

### Configuration

```toml
[[inputs.dmcache]]
  ## Whether to report per-device stats or not
  per_device = true
```

### Measurements & Fields:

- dmcache
    - length
    - target
    - metadata_blocksize
    - metadata_used
    - metadata_total
    - cache_blocksize
    - cache_used
    - cache_total
    - read_hits
    - read_misses
    - write_hits
    - write_misses
    - demotions
    - promotions
    - dirty

### Tags:

- All measurements have the following tags:
    - device

### Example Output:

```
$ ./telegraf --test --config /etc/telegraf/telegraf.conf --input-filter dmcache
* Plugin: inputs.dmcache, Collection 1
> dmcache,device=example cache_blocksize=0i,read_hits=995134034411520i,read_misses=916807089127424i,write_hits=195107267543040i,metadata_used=12861440i,write_misses=563725346013184i,promotions=3265223720960i,dirty=0i,metadata_blocksize=0i,cache_used=1099511627776ii,cache_total=0i,length=0i,metadata_total=1073741824i,demotions=3265223720960i 1491482035000000000
```
