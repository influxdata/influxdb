inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,0,usage_guest,cpu,cpu-total,host.local
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,0,usage_guest,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,91.7364670583823,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,89.51118889861233,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,91.0977744436109,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,91.02836436336374,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,68.304576144036,usage_idle,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,87.88598574821853,usage_idle,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,0,usage_iowait,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,0,usage_iowait,cpu,cpu-total,host.local
,,4,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,0,usage_irq,cpu,cpu-total,host.local
,,4,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,0,usage_irq,cpu,cpu-total,host.local
"
outData = "
#datatype,string,long,string,string
#group,false,false,false,false
#default,_result,,,
,result,table,_field,_value
,,0,usage_guest,usage_guest
,,0,usage_guest_nice,usage_guest_nice
,,0,usage_idle,usage_idle
,,0,usage_iowait,usage_iowait
,,0,usage_irq,usage_irq
"

t_meta_query_fields = (table=<-) =>
  table
    |> range(start:2018-05-22T19:53:26Z)
    |> filter(fn: (r) => r._measurement == "cpu")
    |> group(columns: ["_field"])
    |> distinct(column: "_field")
    |> group()
testingTest(name: "meta_query_fields",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_meta_query_fields)