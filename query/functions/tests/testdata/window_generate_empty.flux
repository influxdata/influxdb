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
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-22T19:53:26Z,2018-05-22T19:53:30Z,2018-05-22T19:53:26Z,0,usage_guest,cpu,cpu-total,host.local
,,1,2018-05-22T19:53:26Z,2018-05-22T19:53:30Z,2018-05-22T19:53:26Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,2,2018-05-22T19:53:26Z,2018-05-22T19:53:30Z,2018-05-22T19:53:26Z,91.7364670583823,usage_idle,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:36Z,0,usage_guest,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:46Z,0,usage_guest,cpu,cpu-total,host.local
,,3,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:56Z,0,usage_guest,cpu,cpu-total,host.local
,,4,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:36Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,4,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:46Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,4,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:56Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,5,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:36Z,89.51118889861233,usage_idle,cpu,cpu-total,host.local
,,5,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:46Z,91.0977744436109,usage_idle,cpu,cpu-total,host.local
,,5,2018-05-22T19:53:30Z,2018-05-22T19:54:00Z,2018-05-22T19:53:56Z,91.02836436336374,usage_idle,cpu,cpu-total,host.local
,,6,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:06Z,0,usage_guest,cpu,cpu-total,host.local
,,6,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:16Z,0,usage_guest,cpu,cpu-total,host.local
,,7,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:06Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,7,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:16Z,0,usage_guest_nice,cpu,cpu-total,host.local
,,8,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:06Z,68.304576144036,usage_idle,cpu,cpu-total,host.local
,,8,2018-05-22T19:54:00Z,2018-05-22T19:54:30Z,2018-05-22T19:54:16Z,87.88598574821853,usage_idle,cpu,cpu-total,host.local

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,9,2018-05-22T19:54:30Z,2018-05-22T19:55:00Z,,,usage_guest,cpu,cpu-total,host.local
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,10,2018-05-22T19:54:30Z,2018-05-22T19:55:00Z,,,usage_guest_nice,cpu,cpu-total,host.local
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,11,2018-05-22T19:54:30Z,2018-05-22T19:55:00Z,,,usage_idle,cpu,cpu-total,host.local
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
"

t_window_generate_empty = (table=<-) =>
  table
	|> range(start:2018-05-22T19:53:26Z, stop: 2018-05-22T19:55:00Z)
	|> window(every: 30s, createEmpty: true)

testingTest(name: "window_generate_empty",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_window_generate_empty)