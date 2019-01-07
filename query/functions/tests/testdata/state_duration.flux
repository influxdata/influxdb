inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#group,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,72.9,used_percent,swap,hostA.local
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,82.5,used_percent,swap,hostA.local
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,82.5,used_percent,swap,hostA.local
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,32.5,used_percent,swap,hostA.local
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,32.5,used_percent,swap,hostA.local
,,161,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,92.6,used_percent,swap,hostA.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,82.9,used_percent,swap,hostB.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,32.5,used_percent,swap,hostB.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,42.5,used_percent,swap,hostB.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,52.5,used_percent,swap,hostB.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,82.5,used_percent,swap,hostB.local
,,162,2018-05-22T19:53:24.4214704Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,92.6,used_percent,swap,hostB.local
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,long
#group,false,false,false,false,false,false,true,true,true,false
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host,stateDuration
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,72.9,used_percent,swap,hostA.local,-1
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,82.5,used_percent,swap,hostA.local,0
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,82.5,used_percent,swap,hostA.local,10
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,32.5,used_percent,swap,hostA.local,-1
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,32.5,used_percent,swap,hostA.local,-1
,,0,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,92.6,used_percent,swap,hostA.local,0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,82.9,used_percent,swap,hostB.local,0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,32.5,used_percent,swap,hostB.local,-1
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,42.5,used_percent,swap,hostB.local,-1
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,52.5,used_percent,swap,hostB.local,-1
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,82.5,used_percent,swap,hostB.local,0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,92.6,used_percent,swap,hostB.local,10
"

t_state_duration = (table=<-) =>
  table
  |> range(start: 2018-05-22T19:53:26Z)
  |> stateDuration(fn:(r) => r._value > 80)

testingTest(name: "state_duration",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_state_duration)