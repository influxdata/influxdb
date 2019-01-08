inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host,name
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,15204688,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,15204894,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,15205102,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,15205226,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,15205499,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,15205755,io_time,diskio,host.local,disk0
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,648,io_time,diskio,host.local,disk2
"
outData = "
#datatype,string,long,dateTime:RFC3339,long
#group,false,false,false,false
#default,0,,,
,result,table,_time,io_time
,,0,2018-05-22T19:53:26Z,15204688
,,0,2018-05-22T19:53:36Z,15204894
,,0,2018-05-22T19:53:46Z,15205102
,,0,2018-05-22T19:53:56Z,15205226
,,0,2018-05-22T19:54:06Z,15205499
,,0,2018-05-22T19:54:16Z,15205755
,,0,2018-05-22T19:53:26Z,648
,,0,2018-05-22T19:53:36Z,648
,,0,2018-05-22T19:53:46Z,648
,,0,2018-05-22T19:53:56Z,648
,,0,2018-05-22T19:54:06Z,648
,,0,2018-05-22T19:54:16Z,648
"

t_group_ungroup = (table=<-) =>
  table
  |> range(start: 2018-05-22T19:53:26Z)
  |> group(columns: ["name"])
  |> group()
  |> map(fn: (r) => ({_time: r._time, io_time:r._value}))
  |> yield(name:"0")

testingTest(name: "group_ungroup",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_group_ungroup)