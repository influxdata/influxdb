# Conntrack Plugin

Collects stats from Netfilter's conntrack-tools.

The conntrack-tools provide a mechanism for tracking various aspects of
network connections as they are processed by netfilter. At runtime, 
conntrack exposes many of those connection statistics within /proc/sys/net.
Depending on your kernel version, these files can be found in either
/proc/sys/net/ipv4/netfilter or /proc/sys/net/netfilter and will be
prefixed with either ip_ or nf_.  This plugin reads the files specified 
in its configuration and publishes each one as a field, with the prefix
normalized to ip_.  

In order to simplify configuration in a heterogeneous environment, a superset
of directory and filenames can be specified.  Any locations that don't exist
will be ignored.

For more information on conntrack-tools, see the 
[Netfilter Documentation](http://conntrack-tools.netfilter.org/).


### Configuration:

```toml
 # Collects conntrack stats from the configured directories and files.
 [[inputs.conntrack]]
   ## The following defaults would work with multiple versions of conntrack.
   ## Note the nf_ and ip_ filename prefixes are mutually exclusive across
   ## kernel versions, as are the directory locations.

   ## Superset of filenames to look for within the conntrack dirs.
   ## Missing files will be ignored.
   files = ["ip_conntrack_count","ip_conntrack_max",
            "nf_conntrack_count","nf_conntrack_max"]

   ## Directories to search within for the conntrack files above.
   ## Missing directories will be ignored.
   dirs = ["/proc/sys/net/ipv4/netfilter","/proc/sys/net/netfilter"]
```

### Measurements & Fields:

- conntrack
    - ip_conntrack_count (int, count): the number of entries in the conntrack table 
    - ip_conntrack_max (int, size): the max capacity of the conntrack table

### Tags:

This input does not use tags.

### Example Output:

```
$ ./telegraf --config telegraf.conf --input-filter conntrack --test
conntrack,host=myhost ip_conntrack_count=2,ip_conntrack_max=262144 1461620427667995735
```
