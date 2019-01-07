inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true,true,true
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,path
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036853184345,inodes_free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,9223372036853184345,inodes_free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,9223372036853184344,inodes_free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,9223372036853184344,inodes_free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,9223372036853184344,inodes_free,disk,disk1s1,apfs,host.local,/
,,91,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,9223372036853184345,inodes_free,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:36Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:46Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:56Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:06Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
,,92,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:54:16Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
"
outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true,true,true
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,path
,,0,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036853184345,inodes_free,disk,disk1s1,apfs,host.local,/
,,1,2018-05-22T19:53:24.421470485Z,2018-05-22T19:54:24.421470485Z,2018-05-22T19:53:26Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,host.local,/
"

filterRegex = /inodes*/

t_parse_regex = (table=<-) =>
  table

    |> range(start:2018-05-20T19:53:26Z)
    |> filter(fn: (r) => r._field =~ filterRegex)
    |> max()
testingTest(name: "parse_regex",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: t_parse_regex)