# OPC UA Client Input Plugin

The `opcua` plugin retrieves data from OPC UA client devices.

Telegraf minimum version: Telegraf 1.16
Plugin minimum tested version: 1.16

### Configuration:

```toml
[[inputs.opcua]]
  ## Device name
  # name = "localhost"
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
  ## name       			- the variable name
  ## namespace  			- integer value 0 thru 3
  ## identifier_type		- s=string, i=numeric, g=guid, b=opaque
  ## identifier			- tag as shown in opcua browser
  ## data_type  			- boolean, byte, short, int, uint, uint16, int16,
  ##                        uint32, int32, float, double, string, datetime, number
  ## Example:
  ## {name="ProductUri", namespace="0", identifier_type="i", identifier="2262", data_type="string", description="http://open62541.org"}
  nodes = [
    {name="", namespace="", identifier_type="", identifier="", data_type="", description=""},
    {name="", namespace="", identifier_type="", identifier="", data_type="", description=""},
  ]
```

### Example Node Configuration
An OPC UA node ID may resemble: "n=3,s=Temperature". In this example:
- n=3 is indicating the `namespace` is 3
- s=Temperature is indicting that the `identifier_type` is a string and `identifier` value is 'Temperature'
- This example temperature node has a value of 79.0, which makes the `data_type` a 'float'.
To gather data from this node enter the following line into the 'nodes' property above:
```
{name="LabelName", namespace="3", identifier_type="s", identifier="Temperature", data_type="float", description="Description of node"},
```


### Example Output

```
opcua,host=3c70aee0901e,name=Random,type=double Random=0.018158170305814902 1597820490000000000

```
