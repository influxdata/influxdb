# Modbus Input Plugin

The Modbus plugin collects Discrete Inputs, Coils, Input Registers and Holding
Registers via Modbus TCP or Modbus RTU/ASCII.

### Configuration

```toml
[[inputs.modbus]]
  ## Connection Configuration
  ##
  ## The plugin supports connections to PLCs via MODBUS/TCP or
  ## via serial line communication in binary (RTU) or readable (ASCII) encoding
  ##
  ## Device name
  name = "Device"

  ## Slave ID - addresses a MODBUS device on the bus
  ## Range: 0 - 255 [0 = broadcast; 248 - 255 = reserved]
  slave_id = 1

  ## Timeout for each request
  timeout = "1s"

  ## Maximum number of retries and the time to wait between retries
  ## when a slave-device is busy.
  # busy_retries = 0
  # busy_retries_wait = "100ms"

  # TCP - connect via Modbus/TCP
  controller = "tcp://localhost:502"

  ## Serial (RS485; RS232)
  # controller = "file:///dev/ttyUSB0"
  # baud_rate = 9600
  # data_bits = 8
  # parity = "N"
  # stop_bits = 1
  # transmission_mode = "RTU"


  ## Measurements
  ##

  ## Digital Variables, Discrete Inputs and Coils
  ## measurement - the (optional) measurement name, defaults to "modbus"
  ## name        - the variable name
  ## address     - variable address

  discrete_inputs = [
    { name = "start",          address = [0]},
    { name = "stop",           address = [1]},
    { name = "reset",          address = [2]},
    { name = "emergency_stop", address = [3]},
  ]
  coils = [
    { name = "motor1_run",     address = [0]},
    { name = "motor1_jog",     address = [1]},
    { name = "motor1_stop",    address = [2]},
  ]

  ## Analog Variables, Input Registers and Holding Registers
  ## measurement - the (optional) measurement name, defaults to "modbus"
  ## name        - the variable name
  ## byte_order  - the ordering of bytes
  ##  |---AB, ABCD   - Big Endian
  ##  |---BA, DCBA   - Little Endian
  ##  |---BADC       - Mid-Big Endian
  ##  |---CDAB       - Mid-Little Endian
  ## data_type  - INT16, UINT16, INT32, UINT32, INT64, UINT64, FLOAT32-IEEE (the IEEE 754 binary representation)
  ##              FLOAT32 (deprecated), FIXED, UFIXED (fixed-point representation on input)
  ## scale      - the final numeric variable representation
  ## address    - variable address

  holding_registers = [
    { name = "power_factor", byte_order = "AB",   data_type = "FIXED", scale=0.01,  address = [8]},
    { name = "voltage",      byte_order = "AB",   data_type = "FIXED", scale=0.1,   address = [0]},
    { name = "energy",       byte_order = "ABCD", data_type = "FIXED", scale=0.001, address = [5,6]},
    { name = "current",      byte_order = "ABCD", data_type = "FIXED", scale=0.001, address = [1,2]},
    { name = "frequency",    byte_order = "AB",   data_type = "UFIXED", scale=0.1,  address = [7]},
    { name = "power",        byte_order = "ABCD", data_type = "UFIXED", scale=0.1,  address = [3,4]},
  ]
  input_registers = [
    { name = "tank_level",   byte_order = "AB",   data_type = "INT16",   scale=1.0,     address = [0]},
    { name = "tank_ph",      byte_order = "AB",   data_type = "INT16",   scale=1.0,     address = [1]},
    { name = "pump1_speed",  byte_order = "ABCD", data_type = "INT32",   scale=1.0,     address = [3,4]},
  ]
```

### Metrics

Metric are custom and configured using the `discrete_inputs`, `coils`,
`holding_register` and `input_registers` options.

### Usage of `data_type`

The field `data_type` defines the representation of the data value on input from the modbus registers.
The input values are then converted from the given `data_type` to a type that is apropriate when
sending the value to the output plugin. These output types are usually one of string, 
integer or floating-point-number. The size of the output type is assumed to be large enough
for all supported input types. The mapping from the input type to the output type is fixed
and cannot be configured.

#### Integers: `INT16`, `UINT16`, `INT32`, `UINT32`, `INT64`, `UINT64`

These types are used for integer input values. Select the one that matches your modbus data source.

#### Floating Point: `FLOAT32-IEEE`

Use this type if your modbus registers contain a value that is encoded in this format. This type
always includes the sign and therefore there exists no variant.

#### Fixed Point: `FIXED`, `UFIXED` (`FLOAT32`)

These types are handled as an integer type on input, but are converted to floating point representation
for further processing (e.g. scaling). Use one of these types when the input value is a decimal fixed point
representation of a non-integer value. 

Select the type `UFIXED` when the input type is declared to hold unsigned integer values, which cannot
be negative. The documentation of your modbus device should indicate this by a term like
'uint16 containing fixed-point representation with N decimal places'.

Select the type `FIXED` when the input type is declared to hold signed integer values. Your documentation
of the modbus device should indicate this with a term like 'int32 containing fixed-point representation
with N decimal places'.

(FLOAT32 is deprecated and should not be used any more. UFIXED provides the same conversion
from unsigned values).

### Example Output

```sh
$ ./telegraf -config telegraf.conf -input-filter modbus -test
modbus.InputRegisters,host=orangepizero Current=0,Energy=0,Frecuency=60,Power=0,PowerFactor=0,Voltage=123.9000015258789 1554079521000000000
```
