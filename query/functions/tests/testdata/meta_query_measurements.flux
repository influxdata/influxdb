inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true,true,true
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,path
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,318324641792,free,disk,disk1s1,apfs,host.local,/
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,318324609024,free,disk,disk1s1,apfs,host.local,/
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,318324129792,free,disk,disk1s1,apfs,host.local,/
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,318324129792,free,disk,disk1s1,apfs,host.local,/
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,318326116352,free,disk,disk1s1,apfs,host.local,/
,,90,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,318325084160,free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036853184345,inodes_free,disk1,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,9223372036853184345,inodes_free,disk1,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,9223372036853184344,inodes_free,disk1,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,9223372036853184344,inodes_free,disk1,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,9223372036853184344,inodes_free,disk1,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,9223372036853184345,inodes_free,disk2,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036854775807,inodes_total,disk2,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,9223372036854775807,inodes_total,disk2,disk1s1,apfs,host.local,/
"
outData = "
#datatype,string,long,string,string
#group,false,false,false,false
#default,_result,,,
,result,table,_measurement,_value
,,0,disk,disk
,,0,disk1,disk1
,,0,disk2,disk2
"

t_meta_query_measurements = (table=<-) =>
  table
    |> range(start:2018-05-22T19:53:26Z)
    |> group(columns: ["_measurement"])
    |> distinct(column: "_measurement")
    |> group()
testingTest(name: "meta_query_measurements",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_meta_query_measurements)