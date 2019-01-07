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
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,double
#group,false,false,false,false,false,false
#default,_result,,,,,
,result,table,_start,_stop,name,_value
,,0,2018-05-22T19:53:26Z,2018-05-22T19:53:27Z,disk0,15204688
,,0,2018-05-22T19:53:26Z,2018-05-22T19:53:27Z,disk2,648
,,0,2018-05-22T19:53:36Z,2018-05-22T19:53:37Z,disk0,15204894
,,0,2018-05-22T19:53:36Z,2018-05-22T19:53:37Z,disk2,648
,,0,2018-05-22T19:53:46Z,2018-05-22T19:53:47Z,disk0,15205102
,,0,2018-05-22T19:53:46Z,2018-05-22T19:53:47Z,disk2,648
,,0,2018-05-22T19:53:56Z,2018-05-22T19:53:57Z,disk0,15205226
,,0,2018-05-22T19:53:56Z,2018-05-22T19:53:57Z,disk2,648
,,0,2018-05-22T19:54:06Z,2018-05-22T19:54:07Z,disk0,15205499
,,0,2018-05-22T19:54:06Z,2018-05-22T19:54:07Z,disk2,648
,,0,2018-05-22T19:54:16Z,2018-05-22T19:54:17Z,disk0,15205755
,,0,2018-05-22T19:54:16Z,2018-05-22T19:54:17Z,disk2,648
"

t_window_group_mean_ungroup = (table=<-) =>
  table
  |> range(start:2018-05-22T19:53:00Z, stop: 2018-05-22T19:55:00Z)
  |> group(columns: ["name"])
  |> window(every:1s)
  |> mean()
  |> group()

testingTest(name: "window_group_mean_ungroup",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_window_group_mean_ungroup)