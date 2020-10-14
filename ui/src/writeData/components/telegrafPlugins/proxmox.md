# Proxmox Input Plugin

The proxmox plugin gathers metrics about containers and VMs using the Proxmox API.

Telegraf minimum version: Telegraf 1.16.0

### Configuration:

```toml
[[inputs.proxmox]]
  ## API connection configuration. The API token was introduced in Proxmox v6.2. Required permissions for user and token: PVEAuditor role on /.
  base_url = "https://localhost:8006/api2/json"
  api_token = "USER@REALM!TOKENID=UUID"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  insecure_skip_verify = false

  # HTTP response timeout (default: 5s)
  response_timeout = "5s"
```

#### Permissions

The plugin will need to have access to the Proxmox API. An API token
must be provided with the corresponding user being assigned at least the PVEAuditor
role on /.

### Measurements & Fields:

- proxmox
  - status
  - uptime
  - cpuload
  - mem_used
  - mem_total
  - mem_free
  - mem_used_percentage
  - swap_used
  - swap_total
  - swap_free
  - swap_used_percentage
  - disk_used
  - disk_total
  - disk_free
  - disk_used_percentage

### Tags:

  - node_fqdn - FQDN of the node telegraf is running on
  - vm_name - Name of the VM/container
  - vm_fqdn - FQDN of the VM/container
  - vm_type - Type of the VM/container (lxc, qemu)

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter proxmox --test
> proxmox,host=pxnode,node_fqdn=pxnode.example.com,vm_fqdn=vm1.example.com,vm_name=vm1,vm_type=lxc cpuload=0.147998116735236,disk_free=4461129728i,disk_total=5217320960i,disk_used=756191232i,disk_used_percentage=14,mem_free=1046827008i,mem_total=1073741824i,mem_used=26914816i,mem_used_percentage=2,status="running",swap_free=536698880i,swap_total=536870912i,swap_used=172032i,swap_used_percentage=0,uptime=1643793i 1595457277000000000
> ...
```
