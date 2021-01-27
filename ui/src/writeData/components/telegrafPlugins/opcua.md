# OPC UA Client Input Plugin

The `opcua` plugin retrieves data from OPC UA client devices.

Telegraf minimum version: Telegraf 1.16
Plugin minimum tested version: 1.16

### Configuration:

```toml
[[inputs.opcua]]
  ## Metric name
  # name = "opcua"
  #
  ## OPC UA Endpoint URL
  # endpoint = "opc.tcp://localhost:4840"
  #
  ## Maximum time allowed to establish a connect to the endpoint.
  # connect_timeout = "10s"
  #
  ## Maximum time allowed for a request over the estabilished connection.
  # request_timeout = "5s"
  #
  ## Security policy, one of "None", "Basic128Rsa15", "Basic256",
  ## "Basic256Sha256", or "auto"
  # security_policy = "auto"
  #
  ## Security mode, one of "None", "Sign", "SignAndEncrypt", or "auto"
  # security_mode = "auto"
  #
  ## Path to cert.pem. Required when security mode or policy isn't "None".
  ## If cert path is not supplied, self-signed cert and key will be generated.
  # certificate = "/etc/telegraf/cert.pem"
  #
  ## Path to private key.pem. Required when security mode or policy isn't "None".
  ## If key path is not supplied, self-signed cert and key will be generated.
  # private_key = "/etc/telegraf/key.pem"
  #
  ## Authentication Method, one of "Certificate", "UserName", or "Anonymous".  To
  ## authenticate using a specific ID, select 'Certificate' or 'UserName'
  # auth_method = "Anonymous"
  #
  ## Username. Required for auth_method = "UserName"
  # username = ""
  #
  ## Password. Required for auth_method = "UserName"
  # password = ""
  #
  ## Node ID configuration
  ## name              - field name to use in the output
  ## namespace         - OPC UA namespace of the node (integer value 0 thru 3)
  ## identifier_type   - OPC UA ID type (s=string, i=numeric, g=guid, b=opaque)
  ## identifier        - OPC UA ID (tag as shown in opcua browser)
  ## tags              - extra tags to be added to the output metric (optional)
  ## Example:
  ## {name="ProductUri", namespace="0", identifier_type="i", identifier="2262", tags=[["tag1","value1"],["tag2","value2]]}
  # nodes = [
  #  {name="", namespace="", identifier_type="", identifier=""},
  #  {name="", namespace="", identifier_type="", identifier=""},
  #]
  #
  ## Node Group
  ## Sets defaults for OPC UA namespace and ID type so they aren't required in
  ## every node.  A group can also have a metric name that overrides the main
  ## plugin metric name.
  ##
  ## Multiple node groups are allowed
  #[[inputs.opcua.group]]
  ## Group Metric name. Overrides the top level name.  If unset, the
  ## top level name is used.
  # name =
  #
  ## Group default namespace. If a node in the group doesn't set its
  ## namespace, this is used.
  # namespace =
  #
  ## Group default identifier type. If a node in the group doesn't set its
  ## namespace, this is used.
  # identifier_type =
  #
  ## Node ID Configuration.  Array of nodes with the same settings as above.
  # nodes = [
  #  {name="", namespace="", identifier_type="", identifier=""},
  #  {name="", namespace="", identifier_type="", identifier=""},
  #]
```

### Node Configuration
An OPC UA node ID may resemble: "n=3;s=Temperature". In this example:
- n=3 is indicating the `namespace` is 3
- s=Temperature is indicting that the `identifier_type` is a string and `identifier` value is 'Temperature'
- This example temperature node has a value of 79.0
To gather data from this node enter the following line into the 'nodes' property above:
```
{field_name="temp", namespace="3", identifier_type="s", identifier="Temperature"},
```

This node configuration produces a metric like this:
```
opcua,id=n\=3;s\=Temperature temp=79.0,quality="OK (0x0)" 1597820490000000000

```

### Group Configuration
Groups can set default values for the namespace, identifier type, and
tags settings.  The default values apply to all the nodes in the
group.  If a default is set, a node may omit the setting altogether.
This simplifies node configuration, especially when many nodes share
the same namespace or identifier type.

The output metric will include tags set in the group and the node.  If
a tag with the same name is set in both places, the tag value from the
node is used.

This example group configuration has two groups with two nodes each:
```
  [[inputs.opcua.group]]
  name="group1_metric_name"
  namespace="3"
  identifier_type="i"
  tags=[["group1_tag", "val1"]]
  nodes = [
    {name="name", identifier="1001", tags=[["node1_tag", "val2"]]},
    {name="name", identifier="1002", tags=[["node1_tag", "val3"]]},
  ]
  [[inputs.opcua.group]]
  name="group2_metric_name"
  namespace="3"
  identifier_type="i"
  tags=[["group2_tag", "val3"]]
  nodes = [
    {name="saw", identifier="1003", tags=[["node2_tag", "val4"]]},
    {name="sin", identifier="1004"},
  ]
```

It produces metrics like these:
```
group1_metric_name,group1_tag=val1,id=ns\=3;i\=1001,node1_tag=val2 name=0,Quality="OK (0x0)" 1606893246000000000
group1_metric_name,group1_tag=val1,id=ns\=3;i\=1002,node1_tag=val3 name=-1.389117,Quality="OK (0x0)" 1606893246000000000
group2_metric_name,group2_tag=val3,id=ns\=3;i\=1003,node2_tag=val4 Quality="OK (0x0)",saw=-1.6 1606893246000000000
group2_metric_name,group2_tag=val3,id=ns\=3;i\=1004 sin=1.902113,Quality="OK (0x0)" 1606893246000000000
```
