# Telegraf Plugin: nginx_vts

This plugin gathers Nginx status using external virtual host traffic status module -  https://github.com/vozlt/nginx-module-vts. This is an Nginx module that provides access to virtual host status information. It contains the current status such as servers, upstreams, caches. This is similar to the live activity monitoring of Nginx plus.
For module configuration details please see its [documentation](https://github.com/vozlt/nginx-module-vts#synopsis).

### Configuration:

```
# Read nginx status information using nginx-module-vts module
[[inputs.nginx_vts]]
  ## An array of Nginx status URIs to gather stats.
  urls = ["http://localhost/status"]
```

### Measurements & Fields:

- nginx_vts_connections
  - active
  - reading
  - writing
  - waiting
  - accepted
  - handled
  - requests
- nginx_vts_server, nginx_vts_filter
  - requests
  - request_time
  - in_bytes
  - out_bytes
  - response_1xx_count
  - response_2xx_count
  - response_3xx_count
  - response_4xx_count
  - response_5xx_count
  - cache_miss
  - cache_bypass
  - cache_expired
  - cache_stale
  - cache_updating
  - cache_revalidated
  - cache_hit
  - cache_scarce
- nginx_vts_upstream
  - requests
  - request_time
  - response_time
  - in_bytes
  - out_bytes
  - response_1xx_count
  - response_2xx_count
  - response_3xx_count
  - response_4xx_count
  - response_5xx_count
  - weight
  - max_fails
  - fail_timeout
  - backup
  - down
- nginx_vts_cache
  - max_bytes
  - used_bytes
  - in_bytes
  - out_bytes
  - miss
  - bypass
  - expired
  - stale
  - updating
  - revalidated
  - hit
  - scarce


### Tags:

- nginx_vts_connections
  - source
  - port
- nginx_vts_server
  - source
  - port
  - zone
- nginx_vts_filter
  - source
  - port
  - filter_name
  - filter_key
- nginx_vts_upstream
  - source
  - port
  - upstream
  - upstream_address
- nginx_vts_cache
  - source
  - port
  - zone


### Example Output:

Using this configuration:
```
[[inputs.nginx_vts]]
  ## An array of Nginx status URIs to gather stats.
  urls = ["http://localhost/status"]
```

When run with:
```
./telegraf -config telegraf.conf -input-filter nginx_vts -test
```

It produces:
```
nginx_vts_connections,source=localhost,port=80,host=localhost waiting=30i,accepted=295333i,handled=295333i,requests=6833487i,active=33i,reading=0i,writing=3i 1518341521000000000
nginx_vts_server,zone=example.com,port=80,host=localhost,source=localhost cache_hit=158915i,in_bytes=1935528964i,out_bytes=6531366419i,response_2xx_count=809994i,response_4xx_count=16664i,cache_bypass=0i,cache_stale=0i,cache_revalidated=0i,requests=2187977i,response_1xx_count=0i,response_3xx_count=1360390i,cache_miss=2249i,cache_updating=0i,cache_scarce=0i,request_time=13i,response_5xx_count=929i,cache_expired=0i 1518341521000000000
nginx_vts_server,host=localhost,source=localhost,port=80,zone=* requests=6775284i,in_bytes=5003242389i,out_bytes=36858233827i,cache_expired=318881i,cache_updating=0i,request_time=51i,response_1xx_count=0i,response_2xx_count=4385916i,response_4xx_count=83680i,response_5xx_count=1186i,cache_bypass=0i,cache_revalidated=0i,cache_hit=1972222i,cache_scarce=0i,response_3xx_count=2304502i,cache_miss=408251i,cache_stale=0i 1518341521000000000
nginx_vts_filter,filter_key=FI,filter_name=country,port=80,host=localhost,source=localhost request_time=0i,in_bytes=139701i,response_3xx_count=0i,out_bytes=2644495i,response_1xx_count=0i,cache_expired=0i,cache_scarce=0i,requests=179i,cache_miss=0i,cache_bypass=0i,cache_stale=0i,cache_updating=0i,cache_revalidated=0i,cache_hit=0i,response_2xx_count=177i,response_4xx_count=2i,response_5xx_count=0i 1518341521000000000
nginx_vts_upstream,port=80,host=localhost,upstream=backend_cluster,upstream_address=127.0.0.1:6000,source=localhost fail_timeout=10i,backup=false,request_time=31i,response_5xx_count=1081i,response_2xx_count=1877498i,max_fails=1i,in_bytes=2763336289i,out_bytes=19470265071i,weight=1i,down=false,response_time=31i,response_1xx_count=0i,response_4xx_count=76125i,requests=3379232i,response_3xx_count=1424528i 1518341521000000000
nginx_vts_cache,source=localhost,port=80,host=localhost,zone=example stale=0i,used_bytes=64334336i,miss=394573i,bypass=0i,expired=318788i,updating=0i,revalidated=0i,hit=689883i,scarce=0i,max_bytes=9223372036854775296i,in_bytes=1111161581i,out_bytes=19175548290i 1518341521000000000
```
