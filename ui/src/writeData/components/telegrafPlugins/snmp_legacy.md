# SNMP Input Plugin

The SNMP input plugin gathers metrics from SNMP agents

### Configuration:


#### Very simple example

In this example, the plugin will gather value of OIDS:

 - `.1.3.6.1.2.1.2.2.1.4.1`

```toml
# Very Simple Example
[[inputs.snmp]]

  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Simple list of OIDs to get, in addition to "collect"
    get_oids = [".1.3.6.1.2.1.2.2.1.4.1"]
```


#### Simple example

In this example, Telegraf gathers value of OIDS:

 - named **ifnumber**
 - named **interface_speed**

With **inputs.snmp.get** section the plugin gets the oid number:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed*

As you can see *ifSpeed* is not a valid OID. In order to get
the valid OID, the plugin uses `snmptranslate_file` to match the OID:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed* => `.1.3.6.1.2.1.2.2.1.5`

Also as the plugin will append `instance` to the corresponding OID:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed* => `.1.3.6.1.2.1.2.2.1.5.1`

In this example, the plugin will gather value of OIDS:

- `.1.3.6.1.2.1.2.1.0`
- `.1.3.6.1.2.1.2.2.1.5.1`


```toml
# Simple example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which get/bulk do you want to collect for this host
    collect = ["ifnumber", "interface_speed"]

  [[inputs.snmp.get]]
    name = "ifnumber"
    oid = ".1.3.6.1.2.1.2.1.0"

  [[inputs.snmp.get]]
    name = "interface_speed"
    oid = "ifSpeed"
    instance = "1"

```


#### Simple bulk example

In this example, Telegraf gathers value of OIDS:

 - named **ifnumber**
 - named **interface_speed**
 - named **if_out_octets**

With **inputs.snmp.get** section the plugin gets oid number:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed*

With **inputs.snmp.bulk** section the plugin gets the oid number:

 - **if_out_octets** => *ifOutOctets*

As you can see *ifSpeed* and *ifOutOctets* are not a valid OID.
In order to get the valid OID, the plugin uses `snmptranslate_file`
to match the OID:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed* => `.1.3.6.1.2.1.2.2.1.5`
 - **if_out_octets** => *ifOutOctets*  => `.1.3.6.1.2.1.2.2.1.16`

Also, the plugin will append `instance` to the corresponding OID:

 - **ifnumber** => `.1.3.6.1.2.1.2.1.0`
 - **interface_speed** => *ifSpeed* => `.1.3.6.1.2.1.2.2.1.5.1`

And **if_out_octets** is a bulk request, the plugin will gathers all
OIDS in the table.

- `.1.3.6.1.2.1.2.2.1.16.1`
- `.1.3.6.1.2.1.2.2.1.16.2`
- `.1.3.6.1.2.1.2.2.1.16.3`
- `.1.3.6.1.2.1.2.2.1.16.4`
- `.1.3.6.1.2.1.2.2.1.16.5`
- `...`

In this example, the plugin will gather value of OIDS:

- `.1.3.6.1.2.1.2.1.0`
- `.1.3.6.1.2.1.2.2.1.5.1`
- `.1.3.6.1.2.1.2.2.1.16.1`
- `.1.3.6.1.2.1.2.2.1.16.2`
- `.1.3.6.1.2.1.2.2.1.16.3`
- `.1.3.6.1.2.1.2.2.1.16.4`
- `.1.3.6.1.2.1.2.2.1.16.5`
- `...`


```toml
# Simple bulk example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which get/bulk do you want to collect for this host
    collect = ["interface_speed", "if_number", "if_out_octets"]

  [[inputs.snmp.get]]
    name = "interface_speed"
    oid = "ifSpeed"
    instance = "1"

  [[inputs.snmp.get]]
    name = "if_number"
    oid = "ifNumber"

  [[inputs.snmp.bulk]]
    name = "if_out_octets"
    oid = "ifOutOctets"
```


#### Table example

In this example, we remove collect attribute to the host section,
but you can still use it in combination of the following part.

Note: This example is like a bulk request a but using an
other configuration

Telegraf gathers value of OIDS of the table:

 - named **iftable1**

With **inputs.snmp.table** section the plugin gets oid number:

 - **iftable1** => `.1.3.6.1.2.1.31.1.1.1`

Also **iftable1** is a table, the plugin will gathers all
OIDS in the table and in the subtables

- `.1.3.6.1.2.1.31.1.1.1.1`
- `.1.3.6.1.2.1.31.1.1.1.1.1`
- `.1.3.6.1.2.1.31.1.1.1.1.2`
- `.1.3.6.1.2.1.31.1.1.1.1.3`
- `.1.3.6.1.2.1.31.1.1.1.1.4`
- `.1.3.6.1.2.1.31.1.1.1.1....`
- `.1.3.6.1.2.1.31.1.1.1.2`
- `.1.3.6.1.2.1.31.1.1.1.2....`
- `.1.3.6.1.2.1.31.1.1.1.3`
- `.1.3.6.1.2.1.31.1.1.1.3....`
- `.1.3.6.1.2.1.31.1.1.1.4`
- `.1.3.6.1.2.1.31.1.1.1.4....`
- `.1.3.6.1.2.1.31.1.1.1.5`
- `.1.3.6.1.2.1.31.1.1.1.5....`
- `.1.3.6.1.2.1.31.1.1.1.6....`
- `...`

```toml
# Table example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which get/bulk do you want to collect for this host
    # Which table do you want to collect
    [[inputs.snmp.host.table]]
      name = "iftable1"

  # table without mapping neither subtables
  # This is like bulk request
  [[inputs.snmp.table]]
    name = "iftable1"
    oid = ".1.3.6.1.2.1.31.1.1.1"
```


#### Table with subtable example

In this example, we remove collect attribute to the host section,
but you can still use it in combination of the following part.

Note: This example is like a bulk request a but using an
other configuration

Telegraf gathers value of OIDS of the table:

 - named **iftable2**

With **inputs.snmp.table** section *AND* **sub_tables** attribute,
the plugin will get OIDS from subtables:

 - **iftable2** => `.1.3.6.1.2.1.2.2.1.13`

Also **iftable2** is a table, the plugin will gathers all
OIDS in subtables:

- `.1.3.6.1.2.1.2.2.1.13.1`
- `.1.3.6.1.2.1.2.2.1.13.2`
- `.1.3.6.1.2.1.2.2.1.13.3`
- `.1.3.6.1.2.1.2.2.1.13.4`
- `.1.3.6.1.2.1.2.2.1.13....`


```toml
# Table with subtable example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which table do you want to collect
    [[inputs.snmp.host.table]]
      name = "iftable2"

  # table without mapping but with subtables
  [[inputs.snmp.table]]
    name = "iftable2"
    sub_tables = [".1.3.6.1.2.1.2.2.1.13"]
    # note
    # oid attribute is useless
```


#### Table with mapping example

In this example, we remove collect attribute to the host section,
but you can still use it in combination of the following part.

Telegraf gathers value of OIDS of the table:

 - named **iftable3**

With **inputs.snmp.table** section the plugin gets oid number:

 - **iftable3** => `.1.3.6.1.2.1.31.1.1.1`

Also **iftable2** is a table, the plugin will gathers all
OIDS in the table and in the subtables

- `.1.3.6.1.2.1.31.1.1.1.1`
- `.1.3.6.1.2.1.31.1.1.1.1.1`
- `.1.3.6.1.2.1.31.1.1.1.1.2`
- `.1.3.6.1.2.1.31.1.1.1.1.3`
- `.1.3.6.1.2.1.31.1.1.1.1.4`
- `.1.3.6.1.2.1.31.1.1.1.1....`
- `.1.3.6.1.2.1.31.1.1.1.2`
- `.1.3.6.1.2.1.31.1.1.1.2....`
- `.1.3.6.1.2.1.31.1.1.1.3`
- `.1.3.6.1.2.1.31.1.1.1.3....`
- `.1.3.6.1.2.1.31.1.1.1.4`
- `.1.3.6.1.2.1.31.1.1.1.4....`
- `.1.3.6.1.2.1.31.1.1.1.5`
- `.1.3.6.1.2.1.31.1.1.1.5....`
- `.1.3.6.1.2.1.31.1.1.1.6....`
- `...`

But the **include_instances** attribute will filter which OIDS
will be gathered; As you see, there is an other attribute, `mapping_table`.
`include_instances` and `mapping_table` permit to build a hash table
to filter only OIDS you want.
Let's say, we have the following data on SNMP server:
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.1` has as value: `enp5s0`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.2` has as value: `enp5s1`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.3` has as value: `enp5s2`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.4` has as value: `eth0`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.5` has as value: `eth1`

The plugin will build the following hash table:

| instance name | instance id |
|---------------|-------------|
| `enp5s0`      | `1`         |
| `enp5s1`      | `2`         |
| `enp5s2`      | `3`         |
| `eth0`        | `4`         |
| `eth1`        | `5`         |

With the **include_instances** attribute, the plugin will gather
the following OIDS:

- `.1.3.6.1.2.1.31.1.1.1.1.1`
- `.1.3.6.1.2.1.31.1.1.1.1.5`
- `.1.3.6.1.2.1.31.1.1.1.2.1`
- `.1.3.6.1.2.1.31.1.1.1.2.5`
- `.1.3.6.1.2.1.31.1.1.1.3.1`
- `.1.3.6.1.2.1.31.1.1.1.3.5`
- `.1.3.6.1.2.1.31.1.1.1.4.1`
- `.1.3.6.1.2.1.31.1.1.1.4.5`
- `.1.3.6.1.2.1.31.1.1.1.5.1`
- `.1.3.6.1.2.1.31.1.1.1.5.5`
- `.1.3.6.1.2.1.31.1.1.1.6.1`
- `.1.3.6.1.2.1.31.1.1.1.6.5`
- `...`

Note: the plugin will add instance name as tag *instance*

```toml
# Simple table with mapping example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which table do you want to collect
    [[inputs.snmp.host.table]]
      name = "iftable3"
      include_instances = ["enp5s0", "eth1"]

  # table with mapping but without subtables
  [[inputs.snmp.table]]
    name = "iftable3"
    oid = ".1.3.6.1.2.1.31.1.1.1"
    # if empty. get all instances
    mapping_table = ".1.3.6.1.2.1.31.1.1.1.1"
    # if empty, get all subtables
```


#### Table with both mapping and subtable example

In this example, we remove collect attribute to the host section,
but you can still use it in combination of the following part.

Telegraf gathers value of OIDS of the table:

 - named **iftable4**

With **inputs.snmp.table** section *AND* **sub_tables** attribute,
the plugin will get OIDS from subtables:

 - **iftable4** => `.1.3.6.1.2.1.31.1.1.1`

Also **iftable2** is a table, the plugin will gathers all
OIDS in the table and in the subtables

- `.1.3.6.1.2.1.31.1.1.1.6.1
- `.1.3.6.1.2.1.31.1.1.1.6.2`
- `.1.3.6.1.2.1.31.1.1.1.6.3`
- `.1.3.6.1.2.1.31.1.1.1.6.4`
- `.1.3.6.1.2.1.31.1.1.1.6....`
- `.1.3.6.1.2.1.31.1.1.1.10.1`
- `.1.3.6.1.2.1.31.1.1.1.10.2`
- `.1.3.6.1.2.1.31.1.1.1.10.3`
- `.1.3.6.1.2.1.31.1.1.1.10.4`
- `.1.3.6.1.2.1.31.1.1.1.10....`

But the **include_instances** attribute will filter which OIDS
will be gathered; As you see, there is an other attribute, `mapping_table`.
`include_instances` and `mapping_table` permit to build a hash table
to filter only OIDS you want.
Let's say, we have the following data on SNMP server:
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.1` has as value: `enp5s0`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.2` has as value: `enp5s1`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.3` has as value: `enp5s2`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.4` has as value: `eth0`
 - OID: `.1.3.6.1.2.1.31.1.1.1.1.5` has as value: `eth1`

The plugin will build the following hash table:

| instance name | instance id |
|---------------|-------------|
| `enp5s0`      | `1`         |
| `enp5s1`      | `2`         |
| `enp5s2`      | `3`         |
| `eth0`        | `4`         |
| `eth1`        | `5`         |

With the **include_instances** attribute, the plugin will gather
the following OIDS:

- `.1.3.6.1.2.1.31.1.1.1.6.1`
- `.1.3.6.1.2.1.31.1.1.1.6.5`
- `.1.3.6.1.2.1.31.1.1.1.10.1`
- `.1.3.6.1.2.1.31.1.1.1.10.5`

Note: the plugin will add instance name as tag *instance*



```toml
# Table with both mapping and subtable example
[[inputs.snmp]]
  ## Use 'oids.txt' file to translate oids to names
  ## To generate 'oids.txt' you need to run:
  ##   snmptranslate -m all -Tz -On | sed -e 's/"//g' > /tmp/oids.txt
  ## Or if you have an other MIB folder with custom MIBs
  ##   snmptranslate -M /mycustommibfolder -Tz -On -m all | sed -e 's/"//g' > oids.txt
  snmptranslate_file = "/tmp/oids.txt"
  [[inputs.snmp.host]]
    address = "127.0.0.1:161"
    # SNMP community
    community = "public" # default public
    # SNMP version (1, 2 or 3)
    # Version 3 not supported yet
    version = 2 # default 2
    # Which table do you want to collect
    [[inputs.snmp.host.table]]
      name = "iftable4"
      include_instances = ["enp5s0", "eth1"]

  # table with both mapping and subtables
  [[inputs.snmp.table]]
    name = "iftable4"
    # if empty get all instances
    mapping_table = ".1.3.6.1.2.1.31.1.1.1.1"
    # if empty get all subtables
    # sub_tables could be not "real subtables"  
    sub_tables=[".1.3.6.1.2.1.2.2.1.13", "bytes_recv", "bytes_send"]
    # note
    # oid attribute is useless

  # SNMP SUBTABLES
  [[inputs.snmp.subtable]]
    name = "bytes_recv"
    oid = ".1.3.6.1.2.1.31.1.1.1.6"
    unit = "octets"

  [[inputs.snmp.subtable]]
    name = "bytes_send"
    oid = ".1.3.6.1.2.1.31.1.1.1.10"
    unit = "octets"
```

#### Configuration notes

- In **inputs.snmp.table** section, the `oid` attribute is useless if
  the `sub_tables` attributes is defined

- In **inputs.snmp.subtable** section, you can put a name from `snmptranslate_file`
  as `oid` attribute instead of a valid OID

### Measurements & Fields:

With the last example (Table with both mapping and subtable example):

- ifHCOutOctets
    - ifHCOutOctets
- ifInDiscards
    - ifInDiscards
- ifHCInOctets
    - ifHCInOctets

### Tags:

With the last example (Table with both mapping and subtable example):

- ifHCOutOctets
    - host
    - instance
    - unit
- ifInDiscards
    - host
    - instance
- ifHCInOctets
    - host
    - instance
    - unit

### Example Output:

With the last example (Table with both mapping and subtable example):

```
ifHCOutOctets,host=127.0.0.1,instance=enp5s0,unit=octets ifHCOutOctets=10565628i 1456878706044462901
ifInDiscards,host=127.0.0.1,instance=enp5s0 ifInDiscards=0i 1456878706044510264
ifHCInOctets,host=127.0.0.1,instance=enp5s0,unit=octets ifHCInOctets=76351777i 1456878706044531312
```
