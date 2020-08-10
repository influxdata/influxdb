# Openldap Input Plugin

This plugin gathers metrics from OpenLDAP's cn=Monitor backend.

### Configuration:

To use this plugin you must enable the [slapd monitoring](https://www.openldap.org/devel/admin/monitoringslapd.html) backend.

```toml
[[inputs.openldap]]
  host = "localhost"
  port = 389

  # ldaps, starttls, or no encryption. default is an empty string, disabling all encryption.
  # note that port will likely need to be changed to 636 for ldaps
  # valid options: "" | "starttls" | "ldaps"
  tls = ""

  # skip peer certificate verification. Default is false.
  insecure_skip_verify = false

  # Path to PEM-encoded Root certificate to use to verify server certificate
  tls_ca = "/etc/ssl/certs.pem"

  # dn/password to bind with. If bind_dn is empty, an anonymous bind is performed.
  bind_dn = ""
  bind_password = ""
  
  # reverse metric names so they sort more naturally
  # Defaults to false if unset, but is set to true when generating a new config
  reverse_metric_names = true
```

### Measurements & Fields:

All **monitorCounter**, **monitoredInfo**, **monitorOpInitiated**, and **monitorOpCompleted** attributes are gathered based on this LDAP query:

```(|(objectClass=monitorCounterObject)(objectClass=monitorOperation)(objectClass=monitoredObject))```

Metric names are based on their entry DN with the cn=Monitor base removed. If `reverse_metric_names` is not set, metrics are based on their DN. If `reverse_metric_names` is set to `true`, the names are reversed. This is recommended as it allows the names to sort more naturally.

Metrics for the **monitorOp*** attributes have **_initiated** and **_completed** added to the base name as appropriate.

An OpenLDAP 2.4 server will provide these metrics:

- openldap
	- connections_current
	- connections_max_file_descriptors
	- connections_total
	- operations_abandon_completed
	- operations_abandon_initiated
	- operations_add_completed
	- operations_add_initiated
	- operations_bind_completed
	- operations_bind_initiated
	- operations_compare_completed
	- operations_compare_initiated
	- operations_delete_completed
	- operations_delete_initiated
	- operations_extended_completed
	- operations_extended_initiated
	- operations_modify_completed
	- operations_modify_initiated
	- operations_modrdn_completed
	- operations_modrdn_initiated
	- operations_search_completed
	- operations_search_initiated
	- operations_unbind_completed
	- operations_unbind_initiated
	- statistics_bytes
	- statistics_entries
	- statistics_pdu
	- statistics_referrals
	- threads_active
	- threads_backload
	- threads_max
	- threads_max_pending
	- threads_open
	- threads_pending
	- threads_starting
	- time_uptime
	- waiters_read
	- waiters_write

### Tags:

- server= # value from config
- port= # value from config

### Example Output:

```
$ telegraf -config telegraf.conf -input-filter openldap -test --debug
* Plugin: inputs.openldap, Collection 1
> openldap,server=localhost,port=389,host=niska.ait.psu.edu operations_bind_initiated=10i,operations_unbind_initiated=6i,operations_modrdn_completed=0i,operations_delete_initiated=0i,operations_add_completed=2i,operations_delete_completed=0i,operations_abandon_completed=0i,statistics_entries=1516i,threads_open=2i,threads_active=1i,waiters_read=1i,operations_modify_completed=0i,operations_extended_initiated=4i,threads_pending=0i,operations_search_initiated=36i,operations_compare_initiated=0i,connections_max_file_descriptors=4096i,operations_modify_initiated=0i,operations_modrdn_initiated=0i,threads_max=16i,time_uptime=6017i,connections_total=1037i,connections_current=1i,operations_add_initiated=2i,statistics_bytes=162071i,operations_unbind_completed=6i,operations_abandon_initiated=0i,statistics_pdu=1566i,threads_max_pending=0i,threads_backload=1i,waiters_write=0i,operations_bind_completed=10i,operations_search_completed=35i,operations_compare_completed=0i,operations_extended_completed=4i,statistics_referrals=0i,threads_starting=0i 1516912070000000000
```
