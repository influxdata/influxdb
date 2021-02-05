# Unbound Input Plugin

This plugin gathers stats from [Unbound](https://www.unbound.net/) -
a validating, recursive, and caching DNS resolver.

### Configuration:

```toml
# A plugin to collect stats from the Unbound DNS resolver
[[inputs.unbound]]
  ## Address of server to connect to, read from unbound conf default, optionally ':port'
  ## Will lookup IP if given a hostname
  server = "127.0.0.1:8953"

  ## If running as a restricted user you can prepend sudo for additional access:
  # use_sudo = false

  ## The default location of the unbound-control binary can be overridden with:
  # binary = "/usr/sbin/unbound-control"

  ## The default location of the unbound config file can be overridden with:
  # config_file = "/etc/unbound/unbound.conf"

  ## The default timeout of 1s can be overridden with:
  # timeout = "1s"

  ## When set to true, thread metrics are tagged with the thread id.
  ##
  ## The default is false for backwards compatibility, and will be changed to
  ## true in a future version.  It is recommended to set to true on new
  ## deployments.
  thread_as_tag = false
```

#### Permissions:

It's important to note that this plugin references unbound-control, which may require additional permissions to execute successfully.
Depending on the user/group permissions of the telegraf user executing this plugin, you may need to alter the group membership, set facls, or use sudo.

**Group membership (Recommended)**:
```bash
$ groups telegraf
telegraf : telegraf

$ usermod -a -G unbound telegraf

$ groups telegraf
telegraf : telegraf unbound
```

**Sudo privileges**:
If you use this method, you will need the following in your telegraf config:
```toml
[[inputs.unbound]]
  use_sudo = true
```

You will also need to update your sudoers file:
```bash
$ visudo
# Add the following line:
Cmnd_Alias UNBOUNDCTL = /usr/sbin/unbound-control
telegraf  ALL=(ALL) NOPASSWD: UNBOUNDCTL
Defaults!UNBOUNDCTL !logfile, !syslog, !pam_session
```

Please use the solution you see as most appropriate.

### Metrics:

This is the full list of stats provided by unbound-control and potentially collected
depending of your unbound configuration.  Histogram related statistics will never be collected,
extended statistics can also be imported ("extended-statistics: yes" in unbound configuration).
In the output, the dots in the unbound-control stat name are replaced by underscores(see
https://www.unbound.net/documentation/unbound-control.html for details).

Shown metrics are with `thread_as_tag` enabled.

- unbound
  - fields:
    total_num_queries
    total_num_cachehits
    total_num_cachemiss
    total_num_prefetch
    total_num_recursivereplies
    total_requestlist_avg
    total_requestlist_max
    total_requestlist_overwritten
    total_requestlist_exceeded
    total_requestlist_current_all
    total_requestlist_current_user
    total_recursion_time_avg
    total_recursion_time_median
    time_now
    time_up
    time_elapsed
    mem_total_sbrk
    mem_cache_rrset
    mem_cache_message
    mem_mod_iterator
    mem_mod_validator
    num_query_type_A
    num_query_type_PTR
    num_query_type_TXT
    num_query_type_AAAA
    num_query_type_SRV
    num_query_type_ANY
    num_query_class_IN
    num_query_opcode_QUERY
    num_query_tcp
    num_query_ipv6
    num_query_flags_QR
    num_query_flags_AA
    num_query_flags_TC
    num_query_flags_RD
    num_query_flags_RA
    num_query_flags_Z
    num_query_flags_AD
    num_query_flags_CD
    num_query_edns_present
    num_query_edns_DO
    num_answer_rcode_NOERROR
    num_answer_rcode_SERVFAIL
    num_answer_rcode_NXDOMAIN
    num_answer_rcode_nodata
    num_answer_secure
    num_answer_bogus
    num_rrset_bogus
    unwanted_queries
    unwanted_replies

- unbound_thread
  - tags:
    - thread
  - fields:
    - num_queries
    - num_cachehits
    - num_cachemiss
    - num_prefetch
    - num_recursivereplies
    - requestlist_avg
    - requestlist_max
    - requestlist_overwritten
    - requestlist_exceeded
    - requestlist_current_all
    - requestlist_current_user
    - recursion_time_avg
    - recursion_time_median

### Example Output:
```
unbound,host=localhost total_requestlist_avg=0,total_requestlist_exceeded=0,total_requestlist_overwritten=0,total_requestlist_current_user=0,total_recursion_time_avg=0.029186,total_tcpusage=0,total_num_queries=51,total_num_queries_ip_ratelimited=0,total_num_recursivereplies=6,total_requestlist_max=0,time_now=1522804978.784814,time_elapsed=310.435217,total_num_cachemiss=6,total_num_zero_ttl=0,time_up=310.435217,total_num_cachehits=45,total_num_prefetch=0,total_requestlist_current_all=0,total_recursion_time_median=0.016384 1522804979000000000
unbound_threads,host=localhost,thread=0 num_queries_ip_ratelimited=0,requestlist_current_user=0,recursion_time_avg=0.029186,num_prefetch=0,requestlist_overwritten=0,requestlist_exceeded=0,requestlist_current_all=0,tcpusage=0,num_cachehits=37,num_cachemiss=6,num_recursivereplies=6,requestlist_avg=0,num_queries=43,num_zero_ttl=0,requestlist_max=0,recursion_time_median=0.032768 1522804979000000000
unbound_threads,host=localhost,thread=1 num_zero_ttl=0,recursion_time_avg=0,num_queries_ip_ratelimited=0,num_cachehits=8,num_prefetch=0,requestlist_exceeded=0,recursion_time_median=0,tcpusage=0,num_cachemiss=0,num_recursivereplies=0,requestlist_max=0,requestlist_overwritten=0,requestlist_current_user=0,num_queries=8,requestlist_avg=0,requestlist_current_all=0 1522804979000000000
```
