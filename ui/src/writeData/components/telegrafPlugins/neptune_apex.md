# Neptune Apex Input Plugin

The Neptune Apex controller family allows an aquarium hobbyist to monitor and control
their tanks based on various probes. The data is taken directly from the `/cgi-bin/status.xml` at the interval specified
in the telegraf.conf configuration file.

The [Neptune Apex](https://www.neptunesystems.com/) input plugin collects real-time data from the Apex's status.xml page.


### Configuration

```toml
[[inputs.neptune_apex]]
  ## The Neptune Apex plugin reads the publicly available status.xml data from a local Apex.
  ## Measurements will be logged under "apex".

  ## The base URL of the local Apex(es). If you specify more than one server, they will
  ## be differentiated by the "source" tag.
  servers = [
    "http://apex.local",
  ]

  ## The response_timeout specifies how long to wait for a reply from the Apex.
  #response_timeout = "5s"

```

### Metrics

The Neptune Apex controller family allows an aquarium hobbyist to monitor and control
their tanks based on various probes. The data is taken directly from the /cgi-bin/status.xml at the interval specified
in the telegraf.conf configuration file.

No manipulation is done on any of the fields to ensure future changes to the status.xml do not introduce conversion bugs
to this plugin. When reasonable and predictable, some tags are derived to make graphing easier and without front-end
programming. These tags are clearly marked in the list below and should be considered a convenience rather than authoritative.

- neptune_apex (All metrics have this measurement name)
  - tags:
    - host (mandatory, string) is the host on which telegraf runs.
    - source (mandatory, string) contains the hostname of the apex device. This can be used to differentiate between
    different units. By using the source instead of the serial number, replacements units won't disturb graphs.
    - type (mandatory, string) maps to the different types of data. Values can be "controller" (The Apex controller
    itself), "probe" for the different input probes, or "output" for any physical or virtual outputs. The Watt and Amp
    probes attached to the physical 120V outlets are aggregated under the output type.
    - hardware (mandatory, string) controller hardware version
    - software (mandatory, string) software version
    - probe_type (optional, string) contains the probe type as reported by the Apex.
    - name (optional, string) contains the name of the probe or output.
    - output_id (optional, string) represents the internal unique output ID. This is different from the device_id.
    - device_id (optional, string) maps to either the aquabus address or the internal reference.
    - output_type (optional, string) categorizes the output into different categories. This tag is DERIVED from the
    device_id. Possible values are: "variable" for the 0-10V signal ports, "outlet" for physical 120V sockets, "alert"
    for alarms (email, sound), "virtual" for user-defined outputs, and "unknown" for everything else.
  - fields:
    - value (float, various unit) represents the probe reading.
    - state (string) represents the output state as defined by the Apex. Examples include "AOF" for Auto (OFF), "TBL"
    for operating according to a table, and "PF*" for different programs.
    - amp (float, Ampere) is the amount of current flowing through the 120V outlet.
    - watt (float, Watt) represents the amount of energy flowing through the 120V outlet.
    - xstatus (string) indicates the xstatus of an outlet. Found on wireless Vortech devices.
    - power_failed (int64, Unix epoch in ns) when the controller last lost power. Omitted if the apex reports it as "none"
    - power_restored (int64, Unix epoch in ns) when the controller last powered on. Omitted if the apex reports it as "none"
    - serial (string, serial number)
   - time:
     - The time used for the metric is parsed from the status.xml page. This helps when cross-referencing events with
     the local system of Apex Fusion. Since the Apex uses NTP, this should not matter in most scenarios.


### Sample Queries


Get the max, mean, and min for the temperature in the last hour:
```
SELECT mean("value") FROM "neptune_apex" WHERE ("probe_type" = 'Temp') AND time >= now() - 6h GROUP BY time(20s)
```

### Troubleshooting

#### sendRequest failure
This indicates a problem communicating with the local Apex controller. If on Mac/Linux, try curl:
```
$ curl apex.local/cgi-bin/status.xml
```
to isolate the problem.

#### parseXML errors
Ensure the XML being returned is valid. If you get valid XML back, open a bug request.

#### Missing fields/data
The neptune_apex plugin is strict on its input to prevent any conversion errors. If you have fields in the status.xml
output that are not converted to a metric, open a feature request and paste your whole status.xml

### Example Output

```
neptune_apex,hardware=1.0,host=ubuntu,software=5.04_7A18,source=apex,type=controller power_failed=1544814000000000000i,power_restored=1544833875000000000i,serial="AC5:12345" 1545978278000000000
neptune_apex,device_id=base_Var1,hardware=1.0,host=ubuntu,name=VarSpd1_I1,output_id=0,output_type=variable,software=5.04_7A18,source=apex,type=output state="PF1" 1545978278000000000
neptune_apex,device_id=base_Var2,hardware=1.0,host=ubuntu,name=VarSpd2_I2,output_id=1,output_type=variable,software=5.04_7A18,source=apex,type=output state="PF2" 1545978278000000000
neptune_apex,device_id=base_Var3,hardware=1.0,host=ubuntu,name=VarSpd3_I3,output_id=2,output_type=variable,software=5.04_7A18,source=apex,type=output state="PF3" 1545978278000000000
neptune_apex,device_id=base_Var4,hardware=1.0,host=ubuntu,name=VarSpd4_I4,output_id=3,output_type=variable,software=5.04_7A18,source=apex,type=output state="PF4" 1545978278000000000
neptune_apex,device_id=base_Alarm,hardware=1.0,host=ubuntu,name=SndAlm_I6,output_id=4,output_type=alert,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=base_Warn,hardware=1.0,host=ubuntu,name=SndWrn_I7,output_id=5,output_type=alert,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=base_email,hardware=1.0,host=ubuntu,name=EmailAlm_I5,output_id=6,output_type=alert,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=base_email2,hardware=1.0,host=ubuntu,name=Email2Alm_I9,output_id=7,output_type=alert,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=2_1,hardware=1.0,host=ubuntu,name=RETURN_2_1,output_id=8,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0.3,state="AON",watt=34 1545978278000000000
neptune_apex,device_id=2_2,hardware=1.0,host=ubuntu,name=Heater1_2_2,output_id=9,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AOF",watt=0 1545978278000000000
neptune_apex,device_id=2_3,hardware=1.0,host=ubuntu,name=FREE_2_3,output_id=10,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="OFF",watt=1 1545978278000000000
neptune_apex,device_id=2_4,hardware=1.0,host=ubuntu,name=LIGHT_2_4,output_id=11,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="OFF",watt=1 1545978278000000000
neptune_apex,device_id=2_5,hardware=1.0,host=ubuntu,name=LHead_2_5,output_id=12,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=4 1545978278000000000
neptune_apex,device_id=2_6,hardware=1.0,host=ubuntu,name=SKIMMER_2_6,output_id=13,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0.1,state="AON",watt=12 1545978278000000000
neptune_apex,device_id=2_7,hardware=1.0,host=ubuntu,name=FREE_2_7,output_id=14,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="OFF",watt=1 1545978278000000000
neptune_apex,device_id=2_8,hardware=1.0,host=ubuntu,name=CABLIGHT_2_8,output_id=15,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=1 1545978278000000000
neptune_apex,device_id=2_9,hardware=1.0,host=ubuntu,name=LinkA_2_9,output_id=16,output_type=unknown,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=2_10,hardware=1.0,host=ubuntu,name=LinkB_2_10,output_id=17,output_type=unknown,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=3_1,hardware=1.0,host=ubuntu,name=RVortech_3_1,output_id=18,output_type=unknown,software=5.04_7A18,source=apex,type=output state="TBL",xstatus="OK" 1545978278000000000
neptune_apex,device_id=3_2,hardware=1.0,host=ubuntu,name=LVortech_3_2,output_id=19,output_type=unknown,software=5.04_7A18,source=apex,type=output state="TBL",xstatus="OK" 1545978278000000000
neptune_apex,device_id=4_1,hardware=1.0,host=ubuntu,name=OSMOLATO_4_1,output_id=20,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AOF",watt=0 1545978278000000000
neptune_apex,device_id=4_2,hardware=1.0,host=ubuntu,name=HEATER2_4_2,output_id=21,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AOF",watt=0 1545978278000000000
neptune_apex,device_id=4_3,hardware=1.0,host=ubuntu,name=NUC_4_3,output_id=22,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0.1,state="AON",watt=8 1545978278000000000
neptune_apex,device_id=4_4,hardware=1.0,host=ubuntu,name=CABFAN_4_4,output_id=23,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=1 1545978278000000000
neptune_apex,device_id=4_5,hardware=1.0,host=ubuntu,name=RHEAD_4_5,output_id=24,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=3 1545978278000000000
neptune_apex,device_id=4_6,hardware=1.0,host=ubuntu,name=FIRE_4_6,output_id=25,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=3 1545978278000000000
neptune_apex,device_id=4_7,hardware=1.0,host=ubuntu,name=LightGW_4_7,output_id=26,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=1 1545978278000000000
neptune_apex,device_id=4_8,hardware=1.0,host=ubuntu,name=GBSWITCH_4_8,output_id=27,output_type=outlet,software=5.04_7A18,source=apex,type=output amp=0,state="AON",watt=0 1545978278000000000
neptune_apex,device_id=4_9,hardware=1.0,host=ubuntu,name=LinkA_4_9,output_id=28,output_type=unknown,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=4_10,hardware=1.0,host=ubuntu,name=LinkB_4_10,output_id=29,output_type=unknown,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=5_1,hardware=1.0,host=ubuntu,name=LinkA_5_1,output_id=30,output_type=unknown,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=Cntl_A1,hardware=1.0,host=ubuntu,name=ATO_EMPTY,output_id=31,output_type=virtual,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=Cntl_A2,hardware=1.0,host=ubuntu,name=LEAK,output_id=32,output_type=virtual,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,device_id=Cntl_A3,hardware=1.0,host=ubuntu,name=SKMR_NOPWR,output_id=33,output_type=virtual,software=5.04_7A18,source=apex,type=output state="AOF" 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=Tmp,probe_type=Temp,software=5.04_7A18,source=apex,type=probe value=78.1 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=pH,probe_type=pH,software=5.04_7A18,source=apex,type=probe value=7.93 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=ORP,probe_type=ORP,software=5.04_7A18,source=apex,type=probe value=191 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=Salt,probe_type=Cond,software=5.04_7A18,source=apex,type=probe value=29.4 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=Volt_2,software=5.04_7A18,source=apex,type=probe value=117 1545978278000000000
neptune_apex,hardware=1.0,host=ubuntu,name=Volt_4,software=5.04_7A18,source=apex,type=probe value=118 1545978278000000000

```

### Contributing

This plugin is used for mission-critical aquatic life support. A bug could very well result in the death of animals.
Neptune does not publish a schema file and as such, we have made this plugin very strict on input with no provisions for
automatically adding fields. We are also careful to not add default values when none are presented to prevent automation
errors.

When writing unit tests, use actual Apex output to run tests. It's acceptable to abridge the number of repeated fields
but never inner fields or parameters.
