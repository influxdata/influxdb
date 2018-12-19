inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#group,false,false,false,false,false,false,false,false,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host,name
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,15204688,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,15204894,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,15205102,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,15205226,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,15205499,io_time,diskio,host.local,disk0
,,1,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,15205755,io_time,diskio,host.local,disk0
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:26Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:36Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:46Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:53:56Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:06Z,648,io_time,diskio,host.local,disk2
,,10,2018-05-22T19:53:26Z,2018-05-22T19:54:16Z,2018-05-22T19:54:16Z,648,io_time,diskio,host.local,disk2
"
outData = "
#datatype,string,long,string
#group,false,false,false
#default,0,,
,result,table,_value
,,0,host
,,0,name
"

t_keys = (table=<-) =>
  table
  |> range(start: 2018-05-20T19:53:26Z)
  |> filter(fn: (r) => r._measurement == "diskio")
  |> keys(except:["_time", "_start", "_stop", "_field", "_measurement", "_value"])
  |> group()
  |> distinct(column:"_value")
  |> yield(name:"0")
testingTest(name: "keys",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_keys)