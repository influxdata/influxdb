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
#datatype,string,long,string,string,string,string,string
#group,false,false,true,true,true,true,false
#default,0,,,,,,
,result,table,_field,_measurement,cpu,host,_value
,,0,usage_guest,cpu,cpu-total,host.local,_field
,,0,usage_guest,cpu,cpu-total,host.local,_measurement
,,0,usage_guest,cpu,cpu-total,host.local,_start
,,0,usage_guest,cpu,cpu-total,host.local,_stop
,,0,usage_guest,cpu,cpu-total,host.local,cpu
,,0,usage_guest,cpu,cpu-total,host.local,host
,,1,usage_guest_nice,cpu,cpu-total,host.local,_field
,,1,usage_guest_nice,cpu,cpu-total,host.local,_measurement
,,1,usage_guest_nice,cpu,cpu-total,host.local,_start
,,1,usage_guest_nice,cpu,cpu-total,host.local,_stop
,,1,usage_guest_nice,cpu,cpu-total,host.local,cpu
,,1,usage_guest_nice,cpu,cpu-total,host.local,host
,,2,usage_idle,cpu,cpu-total,host.local,_field
,,2,usage_idle,cpu,cpu-total,host.local,_measurement
,,2,usage_idle,cpu,cpu-total,host.local,_start
,,2,usage_idle,cpu,cpu-total,host.local,_stop
,,2,usage_idle,cpu,cpu-total,host.local,cpu
,,2,usage_idle,cpu,cpu-total,host.local,host
,,3,usage_iowait,cpu,cpu-total,host.local,_field
,,3,usage_iowait,cpu,cpu-total,host.local,_measurement
,,3,usage_iowait,cpu,cpu-total,host.local,_start
,,3,usage_iowait,cpu,cpu-total,host.local,_stop
,,3,usage_iowait,cpu,cpu-total,host.local,cpu
,,3,usage_iowait,cpu,cpu-total,host.local,host
,,4,usage_irq,cpu,cpu-total,host.local,_field
,,4,usage_irq,cpu,cpu-total,host.local,_measurement
,,4,usage_irq,cpu,cpu-total,host.local,_start
,,4,usage_irq,cpu,cpu-total,host.local,_stop
,,4,usage_irq,cpu,cpu-total,host.local,cpu
,,4,usage_irq,cpu,cpu-total,host.local,host

#datatype,string,long,string,string
#group,false,false,false,false
#default,1,,,
,result,table,host,_value
,,0,host.local,host.local
"

t_meta_query_keys = (table=<-) => {
  zero = table
    |> range(start:2018-05-22T19:53:26Z)
    |> filter(fn: (r) => r._measurement == "cpu")
    |> keys()
    |> yield(name:"0")

  one = table
    |> range(start:2018-05-22T19:53:26Z)
    |> filter(fn: (r) => r._measurement == "cpu")
    |> group(columns: ["host"])
    |> distinct(column: "host")
    |> group()
    |> yield(name:"1")
  return union(tables: [zero, one])
}
testingTest(name: "meta_query_keys",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_meta_query_keys)