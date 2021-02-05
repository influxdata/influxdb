# Tomcat Input Plugin

The Tomcat plugin collects statistics available from the tomcat manager status page from the `http://<host>/manager/status/all?XML=true URL.` (`XML=true` will return only xml data).

See the [Tomcat documentation](https://tomcat.apache.org/tomcat-9.0-doc/manager-howto.html#Server_Status) for details of these statistics.

### Configuration:

```toml
# Gather metrics from the Tomcat server status page.
[[inputs.tomcat]]
  ## URL of the Tomcat server status
  # url = "http://127.0.0.1:8080/manager/status/all?XML=true"

  ## HTTP Basic Auth Credentials
  # username = "tomcat"
  # password = "s3cret"

  ## Request timeout
  # timeout = "5s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

### Measurements & Fields:

- tomcat_jvm_memory
  - free
  - total
  - max
- tomcat_jvm_memorypool
  - max_threads
  - current_thread_count
  - current_threads_busy
  - max_time
  - processing_time
  - request_count
  - error_count
  - bytes_received
  - bytes_sent
- tomcat_connector
  - max_threads
  - current_thread_count
  - current_thread_busy
  - max_time
  - processing_time
  - request_count
  - error_count
  - bytes_received
  - bytes_sent

### Tags:

- tomcat_jvm_memorypool has the following tags:
  - name
  - type
- tomcat_connector
  - name

### Example Output:

```
tomcat_jvm_memory,host=N8-MBP free=20014352i,max=127729664i,total=41459712i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Eden\ Space,type=Heap\ memory committed=11534336i,init=2228224i,max=35258368i,used=1941200i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Survivor\ Space,type=Heap\ memory committed=1376256i,init=262144i,max=4390912i,used=1376248i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Tenured\ Gen,type=Heap\ memory committed=28549120i,init=5636096i,max=88080384i,used=18127912i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Code\ Cache,type=Non-heap\ memory committed=6946816i,init=2555904i,max=251658240i,used=6406528i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Compressed\ Class\ Space,type=Non-heap\ memory committed=1966080i,init=0i,max=1073741824i,used=1816120i 1474663361000000000
tomcat_jvm_memorypool,host=N8-MBP,name=Metaspace,type=Non-heap\ memory committed=18219008i,init=0i,max=-1i,used=17559376i 1474663361000000000
tomcat_connector,host=N8-MBP,name=ajp-bio-8009 bytes_received=0i,bytes_sent=0i,current_thread_count=0i,current_threads_busy=0i,error_count=0i,max_threads=200i,max_time=0i,processing_time=0i,request_count=0i 1474663361000000000
tomcat_connector,host=N8-MBP,name=http-bio-8080 bytes_received=0i,bytes_sent=86435i,current_thread_count=10i,current_threads_busy=1i,error_count=2i,max_threads=200i,max_time=167i,processing_time=245i,request_count=15i 1474663361000000000
```
