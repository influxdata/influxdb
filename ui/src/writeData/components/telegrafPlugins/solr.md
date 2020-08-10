# Solr input plugin

The [solr](http://lucene.apache.org/solr/) plugin collects stats via the
[MBean Request Handler](https://cwiki.apache.org/confluence/display/solr/MBean+Request+Handler)

More about [performance statistics](https://cwiki.apache.org/confluence/display/solr/Performance+Statistics+Reference)

Tested from 3.5 to 7.*

### Configuration:

```
[[inputs.solr]]
  ## specify a list of one or more Solr servers
  servers = ["http://localhost:8983"]
  ##
  ## specify a list of one or more Solr cores (default - all)
  # cores = ["main"]
  ##
  ## Optional HTTP Basic Auth Credentials
  # username = "username"
  # password = "pa$$word"
```

### Example output of gathered metrics:

```
➜  ~ telegraf -config telegraf.conf -input-filter solr -test
* Plugin: solr, Collection 1
> solr_core,core=main,handler=searcher,host=testhost deleted_docs=17616645i,max_docs=261848363i,num_docs=244231718i 1478214949000000000
> solr_core,core=main,handler=core,host=testhost deleted_docs=0i,max_docs=0i,num_docs=0i 1478214949000000000
> solr_queryhandler,core=main,handler=/replication,host=testhost 15min_rate_reqs_per_second=0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000444659081257,5min_rate_reqs_per_second=0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014821969375,75th_pc_request_time=16.484211,95th_pc_request_time=16.484211,999th_pc_request_time=16.484211,99th_pc_request_time=16.484211,avg_requests_per_second=0.0000008443809966322143,avg_time_per_request=12.984811,errors=0i,handler_start=1474662050865i,median_request_time=11.352427,requests=3i,timeouts=0i,total_time=38.954433 1478214949000000000
> solr_queryhandler,core=main,handler=/update/extract,host=testhost 15min_rate_reqs_per_second=0,5min_rate_reqs_per_second=0,75th_pc_request_time=0,95th_pc_request_time=0,999th_pc_request_time=0,99th_pc_request_time=0,avg_requests_per_second=0,avg_time_per_request=0,errors=0i,handler_start=0i,median_request_time=0,requests=0i,timeouts=0i,total_time=0 1478214949000000000
> solr_queryhandler,core=main,handler=org.apache.solr.handler.component.SearchHandler,host=testhost 15min_rate_reqs_per_second=0,5min_rate_reqs_per_second=0,75th_pc_request_time=0,95th_pc_request_time=0,999th_pc_request_time=0,99th_pc_request_time=0,avg_requests_per_second=0,avg_time_per_request=0,errors=0i,handler_start=1474662050861i,median_request_time=0,requests=0i,timeouts=0i,total_time=0 1478214949000000000
> solr_queryhandler,core=main,handler=/tvrh,host=testhost 15min_rate_reqs_per_second=0,5min_rate_reqs_per_second=0,75th_pc_request_time=0,95th_pc_request_time=0,999th_pc_request_time=0,99th_pc_request_time=0,avg_requests_per_second=0,avg_time_per_request=0,errors=0i,handler_start=0i,median_request_time=0,requests=0i,timeouts=0i,total_time=0 1478214949000000000
[…]
```
