# Raindrops Input Plugin

The [raindrops](http://raindrops.bogomips.org/) plugin reads from
specified raindops [middleware](http://raindrops.bogomips.org/Raindrops/Middleware.html) URI and adds stats to InfluxDB.

### Configuration:

```toml
# Read raindrops stats
[[inputs.raindrops]]
  urls = ["http://localhost:8080/_raindrops"]
```

### Measurements & Fields:

- raindrops
    - calling (integer, count)
    - writing (integer, count)
- raindrops_listen
    - active (integer, bytes)
    - queued (integer, bytes)

### Tags:

- Raindops calling/writing of all the workers:
    - server
    - port

- raindrops_listen (ip:port):
    - ip
    - port

- raindrops_listen (Unix Socket):
    - socket

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter raindrops --test
* Plugin: raindrops, Collection 1
> raindrops,port=8080,server=localhost calling=0i,writing=0i 1455479896806238204
> raindrops_listen,ip=0.0.0.0,port=8080 active=0i,queued=0i 1455479896806561938
> raindrops_listen,ip=0.0.0.0,port=8081 active=1i,queued=0i 1455479896806605749
> raindrops_listen,ip=127.0.0.1,port=8082 active=0i,queued=0i 1455479896806646315
> raindrops_listen,ip=0.0.0.0,port=8083 active=0i,queued=0i 1455479896806683252
> raindrops_listen,ip=0.0.0.0,port=8084 active=0i,queued=0i 1455479896806712025
> raindrops_listen,ip=0.0.0.0,port=3000 active=0i,queued=0i 1455479896806779197
> raindrops_listen,socket=/tmp/listen.me active=0i,queued=0i 1455479896806813907
```
