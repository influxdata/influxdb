# Telegraf Plugin: nginx_plus

Nginx Plus is a commercial version of the open source web server Nginx. The use this plugin you will need a license. For more information about the differences between Nginx (F/OSS) and Nginx Plus, [click here](https://www.nginx.com/blog/whats-difference-nginx-foss-nginx-plus/).

Structures for Nginx Plus have been built based on history of
[status module documentation](http://nginx.org/en/docs/http/ngx_http_status_module.html)

### Configuration:

```
# Read Nginx Plus' advanced status information
[[inputs.nginx_plus]]
  ## An array of Nginx status URIs to gather stats.
  urls = ["http://localhost/status"]
```

### Measurements & Fields:

- nginx_plus_processes
  - respawned
- nginx_plus_connections
  - accepted
  - dropped
  - active
  - idle
- nginx_plus_ssl
  - handshakes
  - handshakes_failed
  - session_reuses
- nginx_plus_requests
  - total
  - current
- nginx_plus_upstream, nginx_plus_stream_upstream
  - keepalive
  - zombies
- nginx_plus_upstream_peer, nginx_plus_stream_upstream_peer
  - requests
  - unavail
  - healthchecks_checks
  - header_time
  - response_time
  - state
  - active
  - downstart
  - healthchecks_last_passed
  - weight
  - responses_1xx
  - responses_2xx
  - responses_3xx
  - responses_4xx
  - responses_5xx
  - received
  - selected
  - healthchecks_fails
  - healthchecks_unhealthy
  - backup
  - responses_total
  - sent
  - fails
  - downtime


### Tags:

- nginx_plus_processes, nginx_plus_connections, nginx_plus_ssl, nginx_plus_requests
  - server
  - port

- nginx_plus_upstream, nginx_plus_stream_upstream
  - upstream
  - server
  - port

- nginx_plus_upstream_peer, nginx_plus_stream_upstream_peer
  - id
  - upstream
  - server
  - port
  - upstream_address

### Example Output:

Using this configuration:
```
[[inputs.nginx_plus]]
  ## An array of Nginx Plus status URIs to gather stats.
  urls = ["http://localhost/status"]
```

When run with:
```
./telegraf -config telegraf.conf -input-filter nginx_plus -test
```

It produces:
```
* Plugin: inputs.nginx_plus, Collection 1
> nginx_plus_processes,server=localhost,port=12021,host=word.local respawned=0i 1505782513000000000
> nginx_plus_connections,server=localhost,port=12021,host=word.local accepted=5535735212i,dropped=10140186i,active=9541i,idle=67540i 1505782513000000000
> nginx_plus_ssl,server=localhost,port=12021,host=word.local handshakes=0i,handshakes_failed=0i,session_reuses=0i 1505782513000000000
> nginx_plus_requests,server=localhost,port=12021,host=word.local total=186780541173i,current=9037i 1505782513000000000
> nginx_plus_upstream,port=12021,host=word.local,upstream=dataserver80,server=localhost keepalive=0i,zombies=0i 1505782513000000000
> nginx_plus_upstream_peer,upstream=dataserver80,upstream_address=10.10.102.181:80,id=0,server=localhost,port=12021,host=word.local sent=53806910399i,received=7516943964i,fails=207i,downtime=2325979i,selected=1505782512000i,backup=false,active=6i,responses_4xx=6935i,header_time=80i,response_time=80i,healthchecks_last_passed=true,responses_1xx=0i,responses_2xx=36299890i,responses_5xx=360450i,responses_total=36667275i,unavail=154i,downstart=0i,state="up",requests=36673741i,responses_3xx=0i,healthchecks_unhealthy=5i,weight=1i,healthchecks_checks=177209i,healthchecks_fails=29i 1505782513000000000
> nginx_plus_stream_upstream,server=localhost,port=12021,host=word.local,upstream=dataserver443 zombies=0i 1505782513000000000
> nginx_plus_stream_upstream_peer,server=localhost,upstream_address=10.10.102.181:443,id=0,port=12021,host=word.local,upstream=dataserver443 active=1i,healthchecks_unhealthy=1i,weight=1i,unavail=0i,connect_time=24i,first_byte_time=78i,healthchecks_last_passed=true,state="up",sent=4457713140i,received=698065272i,fails=0i,healthchecks_checks=178421i,downstart=0i,selected=1505782512000i,response_time=5156i,backup=false,connections=56251i,healthchecks_fails=20i,downtime=391017i 1505782513000000000
```

### Reference material

Subsequent versions of status response structure available here:

- [version 1](http://web.archive.org/web/20130805111222/http://nginx.org/en/docs/http/ngx_http_status_module.html)

- [version 2](http://web.archive.org/web/20131218101504/http://nginx.org/en/docs/http/ngx_http_status_module.html)

- version 3 - not available

- [version 4](http://web.archive.org/web/20141218170938/http://nginx.org/en/docs/http/ngx_http_status_module.html)

- [version 5](http://web.archive.org/web/20150414043916/http://nginx.org/en/docs/http/ngx_http_status_module.html)

- [version 6](http://web.archive.org/web/20150918163811/http://nginx.org/en/docs/http/ngx_http_status_module.html)

- [version 7](http://web.archive.org/web/20161107221028/http://nginx.org/en/docs/http/ngx_http_status_module.html)
