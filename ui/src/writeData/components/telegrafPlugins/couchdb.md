# CouchDB Input Plugin

The CouchDB plugin gathers metrics of CouchDB using [_stats] endpoint.

### Configuration

```toml
[[inputs.couchdb]]
  ## Works with CouchDB stats endpoints out of the box
  ## Multiple Hosts from which to read CouchDB stats:
  hosts = ["http://localhost:8086/_stats"]

  ## Use HTTP Basic Authentication.
  # basic_username = "telegraf"
  # basic_password = "p@ssw0rd"
```

### Measurements & Fields:

Statistics specific to the internals of CouchDB:

- couchdb_auth_cache_misses
- couchdb_database_writes
- couchdb_open_databases
- couchdb_auth_cache_hits
- couchdb_request_time
- couchdb_database_reads
- couchdb_open_os_files

Statistics of HTTP requests by method:

- httpd_request_methods_put
- httpd_request_methods_get
- httpd_request_methods_copy
- httpd_request_methods_delete
- httpd_request_methods_post
- httpd_request_methods_head

Statistics of HTTP requests by response code:

- httpd_status_codes_200
- httpd_status_codes_201
- httpd_status_codes_202
- httpd_status_codes_301
- httpd_status_codes_304
- httpd_status_codes_400
- httpd_status_codes_401
- httpd_status_codes_403
- httpd_status_codes_404
- httpd_status_codes_405
- httpd_status_codes_409
- httpd_status_codes_412
- httpd_status_codes_500

httpd statistics:

- httpd_clients_requesting_changes
- httpd_temporary_view_reads
- httpd_requests
- httpd_bulk_requests
- httpd_view_reads

### Tags:

- server (url of the couchdb _stats endpoint)

### Example output:

**Post Couchdb 2.0**
```
couchdb,server=http://couchdb22:5984/_node/_local/_stats couchdb_auth_cache_hits_value=0,httpd_request_methods_delete_value=0,couchdb_auth_cache_misses_value=0,httpd_request_methods_get_value=42,httpd_status_codes_304_value=0,httpd_status_codes_400_value=0,httpd_request_methods_head_value=0,httpd_status_codes_201_value=0,couchdb_database_reads_value=0,httpd_request_methods_copy_value=0,couchdb_request_time_max=0,httpd_status_codes_200_value=42,httpd_status_codes_301_value=0,couchdb_open_os_files_value=2,httpd_request_methods_put_value=0,httpd_request_methods_post_value=0,httpd_status_codes_202_value=0,httpd_status_codes_403_value=0,httpd_status_codes_409_value=0,couchdb_database_writes_value=0,couchdb_request_time_min=0,httpd_status_codes_412_value=0,httpd_status_codes_500_value=0,httpd_status_codes_401_value=0,httpd_status_codes_404_value=0,httpd_status_codes_405_value=0,couchdb_open_databases_value=0 1536707179000000000
```

**Pre Couchdb 2.0**
```
couchdb,server=http://couchdb16:5984/_stats couchdb_request_time_sum=96,httpd_status_codes_200_sum=37,httpd_status_codes_200_min=0,httpd_requests_mean=0.005,httpd_requests_min=0,couchdb_request_time_stddev=3.833,couchdb_request_time_min=1,httpd_request_methods_get_stddev=0.073,httpd_request_methods_get_min=0,httpd_status_codes_200_mean=0.005,httpd_status_codes_200_max=1,httpd_requests_sum=37,couchdb_request_time_current=96,httpd_request_methods_get_sum=37,httpd_request_methods_get_mean=0.005,httpd_request_methods_get_max=1,httpd_status_codes_200_stddev=0.073,couchdb_request_time_mean=2.595,couchdb_request_time_max=25,httpd_request_methods_get_current=37,httpd_status_codes_200_current=37,httpd_requests_current=37,httpd_requests_stddev=0.073,httpd_requests_max=1 1536707179000000000
```

[_stats]: http://docs.couchdb.org/en/1.6.1/api/server/common.html?highlight=stats#get--_stats
