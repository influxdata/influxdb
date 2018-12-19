inData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true,true,true
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,path
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:00Z,30,used_percent,disk,disk1s1,apfs,host.local,/
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:10Z,30,used_percent,disk,disk1s1,apfs,host.local,/
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:20Z,30,used_percent,disk,disk1s1,apfs,host.local,/
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:30Z,40,used_percent,disk,disk1s1,apfs,host.local,/
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:40Z,40,used_percent,disk,disk1s1,apfs,host.local,/
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:50Z,40,used_percent,disk,disk1s1,apfs,host.local,/
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:00Z,35,used_percent,disk,disk1s1,apfs,host.local,/tmp
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:10Z,35,used_percent,disk,disk1s1,apfs,host.local,/tmp
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:20Z,35,used_percent,disk,disk1s1,apfs,host.local,/tmp
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:30Z,45,used_percent,disk,disk1s1,apfs,host.local,/tmp
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:40Z,45,used_percent,disk,disk1s1,apfs,host.local,/tmp
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:50Z,45,used_percent,disk,disk1s1,apfs,host.local,/tmp
"

outData = "
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string,string,string,double
#group,false,false,true,true,false,true,true,true,true,true,true,false
#default,_result,,,,,,,,,,,
,result,table,_start,_stop,_time,_field,_measurement,device,fstype,host,path,_value
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:30Z,used_percent,disk,disk1s1,apfs,host.local,/,30
,,0,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:01:00Z,used_percent,disk,disk1s1,apfs,host.local,/,40
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:00:30Z,used_percent,disk,disk1s1,apfs,host.local,/tmp,35
,,1,2018-05-22T00:00:00Z,2018-05-22T00:01:00Z,2018-05-22T00:01:00Z,used_percent,disk,disk1s1,apfs,host.local,/tmp,45
"

aggregate_window = (table=<-) =>
  table
  |> range(start: 2018-05-22T00:00:00Z, stop:2018-05-22T00:01:00Z)
  |> aggregateWindow(every:30s,fn:mean)

testingTest(name: "aggregate_window",
            input: testLoadStorage(csv: inData),
            want: testLoadMem(csv: outData),
            test: aggregate_window)
